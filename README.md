[![pylint](https://github.com/PatrickBaus/database_logger/actions/workflows/pylint.yml/badge.svg)](https://github.com/PatrickBaus/database_logger/actions/workflows/pylint.yml)
[![code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)
# LabKraken Database Logger
This is a simple Python asyncio database logger for the [LabKraken](https://github.com/PatrickBaus/sensorDaemon) data
acquisition daemon. It can connect to an MQTT broker and stream the data into a [PostgreSQL](https://www.postgresql.org/)
or [Timescale](https://www.timescale.com/) database.

# Setup
The `Kraken logger` is best installed via the Docker repository provided with this repository.

## ... via [`docker-compose`](https://github.com/docker/compose)

Example `docker-compose.yml` for the `Kraken logger`:

```yaml
services:
  db_timescale_sensors:
    image: timescale/timescaledb
    container_name: db_timescale_sensors
    restart: always
    environment:
      POSTGRES_PASSWORD: example
    secrets:
      - timescale_password
    ports:
      - 5432:5432/tcp

  database_logger:
    image: ghcr.io/patrickbaus/database_logger
    container_name: database_logger
    restart: always
    depends_on:
      - db_timescale_sensors
    environment:
      DATABASE_HOST=db_timescale_sensors
      DATABASE_USER=kraken
      DATABASE_PASSWORD=kraken
      MQTT_HOST=my-mqtt-broker
```

# How to extend this image
The `Kraken logger` image can be configured in many ways and the example given above is a minimalistic example for
educational purposes only. The easiest way is to use environment variables.

## Environment Variables
The environment variables can be used to configure the connection options for both the MQTT server and the postgreSQL
server. For passwords, it is recommended to use [docker secrets](https://docs.docker.com/engine/swarm/secrets/) and
the corresponding variable that ends with `_FILE`.

### DATABASE_HOST
This variable defines the hostname of the postgreSQL database. It is a mandatory parameter. Use it in conjunction with
the `DATABASE_PORT` variable to set the database connection parameters.

### DATABASE_PORT
The port used by the database. The default is port `5432`.

### DATABASE_USER
The username used for authentication to the database. This user should have minimal access privileges. The privileges
required are `SELECT` and `INSERT` on table `sensor_data` and `SELECT` on table `sensors`. Using 
[docker secrets](https://docs.docker.com/engine/swarm/secrets/) is the preferred way though and any password set via
secrets will take precedence.

### DATABASE_USER_FILE
The mount point of the Docker secret from which the `DATABASE_USER` will be read. Using Docker secrets is a secure way
to inject a secret into a container as it is not part of the Docker image and cannot be extracted when the container is
shut down. An example implementing [docker secrets](https://docs.docker.com/engine/swarm/secrets/) is shown below.

```yaml
services:
  database_logger:
    image: ghcr.io/patrickbaus/database_logger
    container_name: database_logger
    restart: always
    depends_on:
      - db_timescale_sensors
    environment:
      DATABASE_HOST=db_timescale_sensors
      DATABASE_USER_FILE=/run/secrets/database_logger_user
      DATABASE_PASSWORD_FILE=/run/secrets/database_logger_password
      MQTT_HOST=my-mqtt-broker
    secrets:
      - database_logger_user
      - database_logger_password

secrets:
  database_logger_user:
    file: ./docker-database_logger_user.secret
  database_logger_password:
    file: ./docker-database_logger_password.secret
```

The file `docker-database_logger_user.secret` is located in the same folder as the `docker-compose.yml` file and
contains the user password as a simple string. This file will be mounted into the docker container at runtime to provide
access to the password.

### DATABASE_PASSWORD
The password used for authenticating the `DATABASE_USER`. Using [docker secrets](https://docs.docker.com/engine/swarm/secrets/)
is the preferred way though and any password set via secrets will take precedence.

### DATABASE_PASSWORD_FILE
The mount point of the Docker secret from which the `DATABASE_PASSWORD` will be read. Using Docker secrets is a secure
way to inject a secret into a container as it is not part of the Docker image and cannot be extracted when the container
is shut down. An example can be found [above](#DATABASE_USER_FILE).

### DATABASE_NAME
The name of the database that contains the two tables `sensors` and `sensor_data`. By default, this is `sensors`

### MQTT_HOST
The hostname of the [MQTT](https://en.wikipedia.org/wiki/MQTT) broker used to publish the sensor data.

### MQTT_PORT
The port used to connect to the [MQTT](https://en.wikipedia.org/wiki/MQTT) broker. By default, this is `1883`.

### MQTT_CLIENT_ID
If the [MQTT](https://en.wikipedia.org/wiki/MQTT) broker supports persistent sessions an `MQTT_CLIENT_ID` can be set to
make sure there is no data loss during reconnects. LabKraken publishes all messages with a
[quality of service](https://en.wikipedia.org/wiki/MQTT#Quality_of_service) (`QOS`) tag set to `2`. This means that
all subscribers will receive a message exactly once. Using a custom client id ensures that the messages will get
delivered as soon as a disconnected client comes back online. The broker will store the messages for the client until
that happens. By default, a random client id is used and persistence is not enabled. 

### APPLICATION_LOG_LEVEL
Changes the logging verbosity of the `Kraken logger`. The options are taken from the
[Python logging](https://docs.python.org/3/library/logging.html#levels) module and can be set `DEBUG`, `INFO`, `WARNING`,
`ERROR`, `CRITICAL`. The default log level is `INFO`. For more details see [Logging](#LOGGING).

# Logging
The database logger, by default, only logs connection attempts and errors. Setting the log level to `DEBUG` will print all
data events received and stored in the database. To keep the log file from quickly growing to enormous size it is
recommended to limit its size in the `docker-compose.yml` file.

```yaml
services:
  database_logger:
    image: ghcr.io/patrickbaus/database_logger
    container_name: database_logger
    restart: always
    depends_on:
      - db_timescale_sensors
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

This example uses the default `jason-file` logger and limits the size of the logs to three files of at most 10 MB in
size. The three files will be rotated when full.

## Versioning
I use [SemVer](http://semver.org/) for versioning. For the versions available, see the
[tags on this repository](/../../tags).

## Documentation
I use the [Numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html) style for documentation.

## Authors
* **Patrick Baus** - *Initial work* - [PatrickBaus](https://github.com/PatrickBaus)

## License
This project is licensed under the GPL v3 license - see the [LICENSE](LICENSE) file for details.
