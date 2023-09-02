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
import json
import logging
import re  # Used to parse exceptions
import signal
import warnings
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal
from typing import TypedDict
from uuid import UUID

import aiomqtt
import asyncpg
from decouple import UndefinedValueError, config

from _version import __version__

POSTGRES_STMS = {
    "insert_data": "INSERT INTO sensor_data (time ,sensor_id ,value) VALUES ($1, (SELECT id FROM sensors WHERE"
    " uuid=$2 and sensor_sid=$3 and enabled), $4)",
}


def load_secret(name: str) -> str:
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
    try:
        from_env = config(name)
    except UndefinedValueError:
        from_env = None

    try:
        with open(config(f"{name}_FILE"), newline=None) as secret_file:  # pylint: disable=unspecified-encoding
            from_secret = secret_file.read().rstrip("\n")
    except FileNotFoundError:
        from_secret = None

    if from_secret is not None:
        return from_secret
    if from_env is not None:
        return from_env

    raise UndefinedValueError(f"{name} not found. Declare it as envvar or define a default value.")


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
    # pylint: disable=too-many-arguments
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
            The database passwort
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

    # pylint: disable=too-many-arguments,too-many-locals
    async def mqtt_producer(
        self,
        mqtt_host: str,
        mqtt_port: int,
        mqtt_client_id: str | None,
        output_queue: asyncio.Queue[DataEventDict],
        reconnect_interval: float = 3,
    ) -> None:
        """
        The data produces connects to the MQTT broker and streams the kraken data to send it to the database.

        Parameters
        ----------
        mqtt_host: str
            The MQTT broker hostname
        mqtt_port: int
            The  MQTT broker port
        mqtt_client_id: str
            The client id used by the data logger to enable MQTT persistence when subscribing to topics
        output_queue: Queue of DataEventDict
            The data read from the MQTT stream
        reconnect_interval: float
            Time in seconds between connection attempts.
        """
        previous_reconnect_attempt = asyncio.get_running_loop().time() - reconnect_interval
        while "not connected":
            # Wait for at least reconnect_interval before connecting again
            timeout = self._calculate_timeout(previous_reconnect_attempt, reconnect_interval)
            if timeout > 0:
                self.__logger.info("Delaying reconnect by %.0f s.", timeout)
            await asyncio.sleep(timeout)
            previous_reconnect_attempt = asyncio.get_running_loop().time()

            try:
                # Connect to the MQTT broker
                self.__logger.info("Connecting producer to MQTT broker at '%s:%i'", mqtt_host, mqtt_port)
                async with aiomqtt.Client(
                    hostname=mqtt_host,
                    port=mqtt_port,
                    clean_session=not bool(mqtt_client_id),
                    client_id=mqtt_client_id,
                ) as client:
                    async with client.messages() as messages:
                        await client.subscribe("sensors/#", qos=2)
                        # Log all messages that match the filter
                        async for message in messages:
                            # if message.topic.matches("sensors/+/+/+"):
                            payload = message.payload.decode()
                            event = json.loads(payload, parse_float=Decimal)
                            # TODO: validate event
                            await output_queue.put(event)
            except aiomqtt.error.MqttCodeError as exc:
                # The paho mqtt error codes can be found here:
                # https://github.com/eclipse/paho.mqtt.python/blob/master/src/paho/mqtt/reasoncodes.py
                # and here (bottom):
                # https://github.com/sbtinstruments/asyncio-mqtt/blob/master/asyncio_mqtt/error.py
                reason_code = exc.rc
                self.__logger.error(
                    "PAHO MQTT code error: %s. Reconnecting.",
                    reason_code,
                )
            except ConnectionRefusedError:
                self.__logger.info(
                    "Connection refused by MQTT server (%s:%i). Retrying.",
                    mqtt_host,
                    mqtt_port,
                )
            except aiomqtt.error.MqttError as exc:
                error = re.search(r"^\[Errno (\d+)\]", str(exc))
                if error is not None:
                    error_code = int(error.group(1))
                    if error_code == 111:
                        self.__logger.info(
                            "Connection refused by MQTT server (%s:%i). Retrying.",
                            mqtt_host,
                            mqtt_port,
                        )
                    else:
                        self.__logger.exception("Connection error. Retrying.")
                else:
                    self.__logger.exception("Connection error. Retrying.")

    async def mqtt_consumer(
        self,
        input_queue: asyncio.Queue[DataEventDict],
        database_config: dict,
        worker_name: str,
        reconnect_interval: float = 3,
    ) -> None:
        """
        A database worker that inserts the data read from the MQTT broker into the SQL database.

        Parameters
        ----------
        input_queue: Queue of DataEventDict
            The event to be inserted into the database
        database_config: dict
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
                self.__logger.info("Delaying reconnect of %s by %.0f s.", worker_name, timeout)
            await asyncio.sleep(timeout)
            previous_reconnect_attempt = asyncio.get_running_loop().time()
            try:
                self.__logger.info(
                    "Connecting consumer (%s) to database at '%s:%i",
                    worker_name,
                    database_config["hostname"],
                    database_config["port"],
                )
                async with self.database_connector(**database_config) as conn:
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
                            self.__logger.info("Invalid data received (%s). Dropping it.", item)
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
                            self.__logger.info("Invalid data received (%s). Dropping it.", item)
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
                    database_config["hostname"],
                    database_config["port"],
                    exc,
                )
            except (ConnectionRefusedError, ConnectionResetError):
                self.__logger.error(
                    "Connection refused by host (%s:%i). Retrying.",
                    database_config["hostname"],
                    database_config["port"],
                )
            except OSError as exc:
                self.__logger.error(
                    "Socket error while connecting to database (%s:%i): %s. Retrying.",
                    database_config["hostname"],
                    database_config["port"],
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
            mqtt_host = config("MQTT_HOST")
            mqtt_port = config("MQTT_PORT", cast=int, default=1883)
            mqtt_client_id = config("MQTT_CLIENT_ID", default=None)

            database_config = {
                "hostname": config("DATABASE_HOST"),
                "port": config("DATABASE_PORT", cast=int, default=5432),
                "username": load_secret("DATABASE_USER"),
                "password": load_secret("DATABASE_PASSWORD"),
                "database": config("DATABASE_NAME", default="sensors"),
            }
        except UndefinedValueError as exc:
            self.__logger.error("Environment variable undefined: %s", exc)
            return

        if mqtt_client_id is not None:
            self.__logger.info("MQTT persistence enabled. Using unique client id: '%s'.", mqtt_client_id)

        async with AsyncExitStack() as stack:
            tasks: set[asyncio.Task] = set()
            stack.push_async_callback(self.cancel_tasks, tasks)
            message_queue: asyncio.Queue[DataEventDict] = asyncio.Queue()

            consumers = {
                asyncio.create_task(
                    self.mqtt_consumer(message_queue, database_config, worker_name=f"MQTT consumer {i}")
                )
                for i in range(number_of_publishers)
            }
            tasks.update(consumers)

            # Start the MQTT producer
            task = asyncio.create_task(self.mqtt_producer(mqtt_host, mqtt_port, mqtt_client_id, message_queue))
            tasks.add(task)

            await asyncio.gather(*tasks)

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

    @staticmethod
    async def cancel_tasks(tasks: set[asyncio.Task]) -> None:
        """
        Cancel all running tasks and wait for them to finish. It will raise the task exception if the task returns with
        an exception.

        Parameters
        ----------
        tasks: set of asyncio.Task
            The tasks to cancel

        """
        for task in tasks:
            if task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


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

asyncio.run(main(), debug=True)
