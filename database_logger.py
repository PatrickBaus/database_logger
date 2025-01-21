#!/usr/bin/env python
# ##### BEGIN GPL LICENSE BLOCK #####
#
# Copyright (C) 2021  Patrick Baus
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# ##### END GPL LICENSE BLOCK #####
"""
This is a demo logger for the Kraken sensor system. It connects to the MQTT broker and pushes all data from data
sources, that are configured, into a database.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import re  # Used to parse exceptions
import signal
import warnings
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import TypedDict
from uuid import UUID

import aiomqtt
import asyncpg
from decouple import UndefinedValueError, config

from _version import __version__
from typedefs import DatabaseParams, MQTTParams

POSTGRES_STMS = {
    "insert_data": "INSERT INTO sensor_data (time ,sensor_id ,value) VALUES ($1, (SELECT id FROM sensors WHERE"
    " uuid=$2 and sensor_sid=$3 and enabled), $4)",
}


def load_secret(name: str, *args, **kwargs) -> str:
    """
    Loads a sensitive parameter either from the environment variable or Docker secret file. See
    https://docs.docker.com/engine/swarm/secrets/ for details. The env variable read for the file
    path is automatically appended by '_FILE'. Using the name 'MY_SECRET', the secret is either read from 'MY_SECRET' or
    the secret path is read from 'MY_SECRET_FILE'. In the latter case the secret is then read from  the file
    'MY_SECRET_FILE'. The secret file is preferred over the env variable.

    Parameters
    ----------
    name: str
        The name of the env variable containing the secret or the file path.
    Returns
    -------
    str:
        The secret read either from the environment or secrets file.
    """
    # Remove the default return value for testing against the secrets file, we will put it back later.
    if "default" in kwargs:
        has_default = True
        default_value = kwargs.pop("default")
    else:
        has_default = False
        default_value = None
    try:
        with open(
            config(f"{name}_FILE", *args, **kwargs), newline=None, mode="r", encoding="utf-8"
        ) as secret_file:  # pylint: disable=unspecified-encoding
            return secret_file.read().rstrip("\n")
    except UndefinedValueError:
        pass

    if has_default:
        kwargs["default"] = default_value
    try:
        return config(name, *args, **kwargs)
    except UndefinedValueError:
        raise UndefinedValueError(f"{name} not found. Declare it as env var or define a default value.") from None


class DataEventDict(TypedDict):
    """The kraken data package"""

    timestamp: Decimal
    sid: int
    unit: str
    uuid: str
    value: int | Decimal


class DatabaseLogger:
    """
    The database logger connects to the MQTT broker and pushes all data to the database.
    """

    def __init__(self):
        """
        Creates a sensorDaemon object.
        """
        self.__logger = logging.getLogger(__name__)

    @asynccontextmanager
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    async def database_connector(
        self, hostname: str, port: int, username: str, password: str, database: str, **kwargs
    ) -> asyncpg.Connection:
        """
        A wrapper around asyncpg.connect() to create a context manager.

        Parameters
        ----------
        hostname: str
            The database hostname
        port: int
            The database port
        username: str
            The database username
        password: str
            The database password
        database: str
            The database name
        **kwargs: dict, optional
            These parameters will be passed to asyncpg.connect() as well

        Yields
        -------
        asyncpg.Connection
            An asyncpg PostgreSQL connection

        """
        conn = await asyncpg.connect(
            user=username, password=password, database=database, host=hostname, port=port, **kwargs
        )
        try:
            yield conn
        finally:
            await conn.close()

    @staticmethod
    def _calculate_timeout(last_reconnect_attempt: float, reconnect_interval: float) -> float:
        return max(0.0, reconnect_interval - (asyncio.get_running_loop().time() - last_reconnect_attempt))

    # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    async def mqtt_producer(
        self,
        mqtt_config: MQTTParams,
        output_queue: asyncio.Queue[DataEventDict],
        reconnect_interval: float = 3,
    ) -> None:
        """
        The producer connects to the MQTT broker and streams Kraken data to be sent to the database.

        Parameters
        ----------
        mqtt_config: MQTTParams
            The client id/identifier used by the data logger to enable MQTT persistence when subscribing to topics.
            Setting this to None will use a random client id and disables persistent messages.
            The username/password is used for authentication with the MQTT server. Set to None if no username is
            required.
        output_queue: Queue of DataEventDict
            The data read from the MQTT stream
        reconnect_interval: float, default=3
            Time in seconds between connection attempts.
        """
        previous_reconnect_attempt = asyncio.get_running_loop().time() - reconnect_interval
        for hostname, port in itertools.cycle(
            mqtt_config.hosts
        ):  # iterate over the list of hostnames until the end of time
            # Wait for at least reconnect_interval before connecting again
            timeout = self._calculate_timeout(previous_reconnect_attempt, reconnect_interval)
            if timeout > 0:
                self.__logger.info("Delaying reconnect by %.0f s.", timeout)
            await asyncio.sleep(timeout)
            previous_reconnect_attempt = asyncio.get_running_loop().time()

            try:
                # Connect to the MQTT broker
                self.__logger.info("Connecting producer to MQTT broker at '%s:%i'", hostname, port)
                async with aiomqtt.Client(
                    hostname=hostname,
                    port=port,
                    **mqtt_config.model_dump(exclude={"hosts"}),
                    clean_session=not bool(mqtt_config.identifier),
                ) as client:
                    self.__logger.info("Connected to MQTT broker at '%s:%i'", hostname, port)
                    await client.subscribe("sensors/#", qos=2)
                    async for message in client.messages:
                        # if message.topic.matches("sensors/+/+/+"):
                        self.__logger.debug("MQTT message received: (%s).", message.payload.decode("utf-8"))
                        try:
                            event = json.loads(message.payload, parse_float=Decimal)
                            # TODO: validate event
                            await output_queue.put(event)
                        except (json.decoder.JSONDecodeError, TypeError):
                            self.__logger.warning(
                                "Received invalid message '%s' on channel '%s'", message.payload, message.topic
                            )
            except aiomqtt.MqttCodeError as exc:
                # The paho mqtt error codes can be found here:
                # https://github.com/eclipse/paho.mqtt.python/blob/master/src/paho/mqtt/reasoncodes.py
                # and here (bottom):
                # https://github.com/sbtinstruments/aiomqtt/blob/main/aiomqtt/exceptions.py
                reason_code = exc.rc
                if reason_code == 7:
                    self.__logger.error(
                        "Connection to MQTT server (%s:%i) lost. Reconnecting.",
                        hostname,
                        port,
                    )
                else:
                    self.__logger.error(
                        "PAHO MQTT code error: %s. Reconnecting.",
                        reason_code,
                    )
            except ConnectionRefusedError:
                self.__logger.warning(
                    "Connection refused by MQTT server (%s:%i). Retrying.",
                    hostname,
                    port,
                )
            except aiomqtt.MqttError as exc:
                error = re.search(r"\[Errno (\d+)]", str(exc))
                if error is not None:
                    error_code = int(error.group(1))
                    if error_code == 111:
                        self.__logger.warning(
                            "Connection refused by MQTT server (%s:%i). Retrying.",
                            hostname,
                            port,
                        )
                    else:
                        self.__logger.exception("Connection error. Retrying.")
                else:
                    self.__logger.exception("Connection error. Retrying.")

    async def mqtt_consumer(
        self,
        input_queue: asyncio.Queue[DataEventDict],
        database_config: DatabaseParams,
        worker_name: str,
        reconnect_interval: float = 3,
    ) -> None:
        """
        A database worker that inserts the data read from the MQTT broker into the SQL database.

        Parameters
        ----------
        input_queue: Queue of DataEventDict
            The event to be inserted into the database
        database_config: DatabaseParams
            A dictionary containing the database configuration parameters passed on to asyncpg.connect()
        worker_name: str
            A human-readable name for the worker used for logging.
        reconnect_interval: float
            Time in seconds between connection attempts.
        """
        item: DataEventDict | None = None
        previous_reconnect_attempt = asyncio.get_running_loop().time() - reconnect_interval
        while "not connected":
            # Wait for at least reconnect_interval before connecting again
            timeout = self._calculate_timeout(previous_reconnect_attempt, reconnect_interval)
            if round(timeout) > 0:  # Do not print '0 s' as this is confusing, Waiting for less than a second is OK.
                self.__logger.info("Delaying reconnect of '%s' by %.0f s.", worker_name, timeout)
            await asyncio.sleep(timeout)
            previous_reconnect_attempt = asyncio.get_running_loop().time()
            try:
                self.__logger.info(
                    "Connecting consumer (%s) to database at '%s:%i",
                    worker_name,
                    *database_config.host,
                )
                async with self.database_connector(
                    hostname=database_config.host[0],
                    port=database_config.host[1],
                    **database_config.model_dump(exclude={"host"}),
                ) as conn:
                    self.__logger.info(
                        "Connected consumer (%s) to database at '%s:%i",
                        worker_name,
                        *database_config.host,
                    )
                    while "queue not done":
                        if item is None:
                            # only get new data if we have pushed everything to the DB
                            item = await input_queue.get()
                        # TODO: catch asyncpg.exceptions.InvalidPasswordError
                        try:
                            timestamp = datetime.fromtimestamp(float(item["timestamp"]), timezone.utc)
                        except (OverflowError, OSError):
                            # If there is a conversion error, we will use the current timestamp instead
                            timestamp = datetime.now(timezone.utc)
                        try:
                            uuid, sid, value = UUID(item["uuid"]), item.get("sid", 0), item["value"]
                        except (KeyError, ValueError):
                            self.__logger.warning("Invalid data received (%s). Dropping it.", item)
                            # ignore invalid entries
                            item = None  # Get a new event to publish
                            input_queue.task_done()
                            continue
                        try:
                            await conn.execute(POSTGRES_STMS["insert_data"], timestamp, uuid, sid, value)
                        except (
                            asyncpg.exceptions.NotNullViolationError,  # unknown sensors
                            asyncpg.exceptions.UniqueViolationError,  # duplicate entries
                        ):
                            item = None  # Get a new event to publish
                            input_queue.task_done()
                        except asyncpg.exceptions.DataError:
                            self.__logger.warning("Invalid data received (%s). Dropping it.", item)
                            item = None  # Get a new event to publish
                            input_queue.task_done()
                        else:
                            self.__logger.debug("Storing event: (%s).", item)
                            item = None  # Get a new event to publish
                            input_queue.task_done()
            except (
                asyncpg.exceptions.InterfaceError,
                asyncpg.exceptions.CannotConnectNowError,  # DB is reconnecting
                asyncpg.exceptions.InternalClientError,  # an unclassified error
                asyncpg.exceptions.PostgresError,  # Catch-all for Postgres errors
            ) as exc:
                self.__logger.error(
                    "Database connection (%s:%i) error: %s. Retrying.",
                    *database_config.host,
                    exc,
                )
            except (ConnectionRefusedError, ConnectionResetError):
                self.__logger.error(
                    "Connection refused by database host (%s:%i). Retrying.",
                    *database_config.host,
                )
            except OSError as exc:
                self.__logger.error(
                    "Socket error while connecting to database (%s:%i): %s. Retrying.",
                    *database_config.host,
                    exc,
                )

    async def run(self, number_of_publishers: int = 5):
        """
        Start the daemon and keep it running through the while (True)
        loop. Execute shutdown() to kill it.
        """
        self.__logger.warning("#################################################")
        self.__logger.warning("Starting Kraken logger v%s...", __version__)
        self.__logger.warning("#################################################")

        # Catch signals and shutdown
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            asyncio.get_running_loop().add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # Read either environment variable, settings.ini or .env file
        try:
            mqtt_config = MQTTParams(
                hosts=config("MQTT_HOST"),
                identifier=config("MQTT_CLIENT_ID", default=None),
                username=load_secret("MQTT_CLIENT_USER", default=None),
                password=load_secret("MQTT_CLIENT_PASSWORD", default=None),
            )

            database_config = DatabaseParams(
                host=config("DATABASE_HOST"),
                username=load_secret("DATABASE_USER"),
                password=load_secret("DATABASE_PASSWORD"),
                database=config("DATABASE_NAME", default="sensors"),
            )
        except UndefinedValueError as exc:
            self.__logger.error("Environment variable undefined: %s", exc)
            return

        if mqtt_config.identifier is None:
            self.__logger.warning(
                "No MQTT client id set. Durable queues cannot be enabled. Events will be lost when "
                "the logger disconnects from the MQTT server."
            )
        else:
            self.__logger.info("MQTT persistence enabled. Using unique client id: '%s'.", mqtt_config.identifier)

        async with asyncio.TaskGroup() as task_group:
            message_queue: asyncio.Queue[DataEventDict] = asyncio.Queue()

            for i in range(number_of_publishers):
                task_group.create_task(
                    self.mqtt_consumer(message_queue, database_config, worker_name=f"MQTT consumer {i}")
                )

            # Start the MQTT producer
            task_group.create_task(self.mqtt_producer(output_queue=message_queue, mqtt_config=mqtt_config))

    async def shutdown(self):
        """
        Stops the daemon and gracefully disconnect from all clients.
        """
        self.__logger.warning("#################################################")
        self.__logger.warning("Stopping Daemon...")
        self.__logger.warning("#################################################")

        # Get all running tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        # and stop them
        [task.cancel() for task in tasks]  # pylint: disable=expression-not-assigned
        # finally wait for them to terminate
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        except Exception:  # pylint: disable=broad-except
            # We want to catch all exceptions on shutdown, except the asyncio.CancelledError
            # The exception will then be printed using the logger
            self.__logger.exception("Error while reaping tasks during shutdown")


async def main():
    """
    The main (infinite) loop, that runs until Kraken has shut down.
    """
    daemon = DatabaseLogger()
    try:
        await daemon.run()
    except asyncio.CancelledError:
        # Swallow that error, because this is the root task, there is nothing
        # cancel above it.
        pass


def parse_log_level(log_level: int | str) -> int:
    """
    Parse an int or string, then return its standard log level definition.
    Parameters
    ----------
    log_level: int or str
        The log level. Either a string or a number.
    Returns
    -------
    int
        The log level as defined by the standard library. Returns logging.INFO as default
    """
    try:
        level = int(log_level)
    except ValueError:
        # parse the string
        level = logging.getLevelName(str(log_level).upper())
    if isinstance(level, int):
        return level
    return logging.INFO  # default log level


# Report all mistakes managing asynchronous resources.
warnings.simplefilter("always", ResourceWarning)
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
    level=config("APPLICATION_LOG_LEVEL", default=logging.INFO, cast=parse_log_level),
    datefmt="%Y-%m-%d %H:%M:%S",
)

asyncio.run(main(), debug=False)
