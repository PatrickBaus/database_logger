#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
Kraken is a sensor data aggregation tool for distributed sensors arrays. It uses
AsyncIO instead of threads to scale and outputs data to a MQTT broker.
"""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime
import os
import logging
import re   # Used to parse exceptions
import signal
import sys
from uuid import UUID
import warnings

from aiostream import stream, pipe
import asyncpg
import asyncio_mqtt
from decouple import config
import simplejson as json

from _version import __version__


def we_are_frozen():
    """Returns whether we are frozen via py2exe.
    This will affect how we find out where we are located."""

    return hasattr(sys, "frozen")


def module_path():
    """ This will get us the program's directory,
    even if we are frozen using py2exe"""

    if we_are_frozen():
        return os.path.dirname(sys.executable)

    return os.path.dirname(__file__)


POSTGRES_STMS = {
    'insert_data': "INSERT INTO sensor_data (time ,sensor_id ,value) VALUES ($1, (SELECT id FROM sensors WHERE uuid=$2 and sensor_sid=$3 and enabled), $4)",
}


class DatabaseLogger():
    """
    Main daemon, that runs in the background and monitors all sensors. It will
    configure them according to options set in the database and then place the
    returned data in the database as well.
    """
    def __init__(self):
        """
        Creates a sensorDaemon object.
        """
        self.__logger = logging.getLogger(__name__)

    @asynccontextmanager
    async def database_connector(self, hostname, port, username, password, database):
        conn = await asyncpg.connect(user=username, password=password, database=database, host=hostname)
        try:
            yield conn
        finally:
            await conn.close()

    async def mqtt_producer(self, mqtt_host, mqtt_port, output_queue, reconnect_interval=3):
        while "not connected":
            try:
                async with AsyncExitStack() as stack:
                    # Keep track of the asyncio tasks that we create, so that
                    # we can cancel them on exit
                    tasks = set()
                    stack.push_async_callback(self.cancel_tasks, tasks)

                    # Connect to the MQTT broker
                    self.__logger.info("Connecting producer to MQTT broker at '%s:%i", mqtt_host, mqtt_port)
                    client = asyncio_mqtt.Client(hostname=mqtt_host, port=mqtt_port, clean_session=False, client_id='beholder_datalogger')
                    await stack.enter_async_context(client)

                    # You can create any number of topic filters
                    topic_filters = (
                        "sensors/+/+/+",
                    )
                    for topic_filter in topic_filters:
                        # Log all messages that matches the filter
                        manager = client.filtered_messages(topic_filter)
                        messages = await stack.enter_async_context(manager)
                        task = asyncio.create_task(self.log_messages(messages, output_queue))
                        tasks.add(task)

                    # Messages that doesn't match a filter will get logged here
                    messages = await stack.enter_async_context(client.unfiltered_messages())
                    task = asyncio.create_task(self.log_messages(messages, output_queue))
                    tasks.add(task)

                    await client.subscribe("sensors/#", qos=2)

                    # Wait for everything to complete (or fail due to, e.g., network
                    # errors)
                    await asyncio.gather(*tasks)
            except asyncio_mqtt.error.MqttCodeError as exc:
                # The paho mqtt errorcodes can be found here:
                # https://github.com/eclipse/paho.mqtt.python/blob/master/src/paho/mqtt/reasoncodes.py
                # and here (bottom):
                # https://github.com/sbtinstruments/asyncio-mqtt/blob/master/asyncio_mqtt/error.py
                # reason_code = exc.rc
                await asyncio.sleep(reconnect_interval)
            except ConnectionRefusedError:
                self.__logger.info("Connection refused by host (%s:%i). Retrying.", mqtt_host, mqtt_port)
                await asyncio.sleep(reconnect_interval)
            except asyncio_mqtt.error.MqttError as exc:
                x = re.search(r"^\[Errno (\d+)\]", str(exc))
                if x is not None:
                    errorcode = int(x.group(1))
                    if errorcode == 111:
                        self.__logger.info("Connection refused by host (%s:%i). Retrying.", mqtt_host, mqtt_port)
                    else:
                        self.__logger.exception("Connection error. Retrying.")
                else:
                    self.__logger.exception("Connection error. Retrying.")
                await asyncio.sleep(reconnect_interval)

    async def log_messages(self, messages, output_queue):
        async for message in messages:
            payload = message.payload.decode()
            event = json.loads(payload, use_decimal=True)
            output_queue.put_nowait(event)

    async def mqtt_consumer(self, input_queue, database_config):
        async with AsyncExitStack() as stack:
            data_stream = stream.call(input_queue.get) | pipe.cycle()
            # Need to catch asyncpg.exceptions.InvalidPasswordError
            conn = await stack.enter_async_context(self.database_connector(
                **database_config
            ))
            streamer = await stack.enter_async_context(data_stream.stream())
            async for item in streamer:
                uuid, sid, timestamp, value = UUID(item['uuid']), item['sid'], datetime.fromtimestamp(item['timestamp']), item['value']
                try:
                    await conn.execute(POSTGRES_STMS['insert_data'], timestamp, uuid, sid, value)
                except asyncpg.exceptions.NotNullViolationError:
                    # Ignore unknown sensors
                    pass
                else:
                    print(item)

    async def mqtt_test_consumer(self, input_queue, *args, **kwargs):
        data_stream = stream.call(input_queue.get) | pipe.cycle()
        async with data_stream.stream() as streamer:
            async for item in streamer:
                print(item)

    async def run(self, number_of_publishers=5):
        """
        Start the daemon and keep it running through the while (True)
        loop. Execute shutdown() to kill it.
        """
        self.__logger.warning("#################################################")
        self.__logger.warning("Starting Daemon...")
        self.__logger.warning("#################################################")

        # Catch signals and shutdown
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for sig in signals:
            asyncio.get_running_loop().add_signal_handler(
                sig, lambda: asyncio.create_task(self.shutdown()))

        # Read either environment variable, settings.ini or .env file
        try:
            mqtt_host = config('MQTT_HOST')
            mqtt_port = config('MQTT_PORT', cast=int, default=1883)

            database_config = {
                'hostname': config('DATABASE_HOST'),
                'port': config('DATABASE_PORT', cast=int, default=5432),
                'username': config('DATABASE_USER'),
                'password': config('DATABASE_PASSWORD'),
                'database': config('DATABASE_NAME', default="sensors"),
            }
        except UndefinedValueError as exc:
            self.__logger.error("Environment variable undefined: %s", exc)
            return

        async with AsyncExitStack() as stack:
            tasks = set()
            stack.push_async_callback(self.cancel_tasks, tasks)
            message_queue = asyncio.Queue()

            consumers = {asyncio.create_task(self.mqtt_consumer(message_queue, database_config)) for i in range(number_of_publishers)}
            tasks.update(consumers)

            # Start the MQTT producer
            task = asyncio.create_task(self.mqtt_producer(mqtt_host, mqtt_port, message_queue))
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
        [task.cancel() for task in tasks]   # pylint: disable=expression-not-assigned
        # finally wait for them to terminate
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        except Exception:   # pylint: disable=broad-except
            # We want to catch all exceptions on shutdown, except the asyncio.CancelledError
            # The exception will then be printed using the logger
            self.__logger.exception("Error while reaping tasks during shutdown")

    async def cancel_tasks(self, tasks):
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


# Report all mistakes managing asynchronous resources.
warnings.simplefilter('always', ResourceWarning)
logging.basicConfig(
    format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
    level=logging.INFO,    # Enable logs from the ip connection. Set to debug for even more info
    datefmt='%Y-%m-%d %H:%M:%S'
)

asyncio.run(main(), debug=True)
