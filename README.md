[![pylint](https://github.com/PatrickBaus/database_logger/actions/workflows/pylint.yml/badge.svg)](https://github.com/PatrickBaus/database_logger/actions/workflows/pylint.yml)
[![code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)
# LabKraken Database Logger
This is a simple database logger for the [LabKraken](https://github.com/PatrickBaus/sensorDaemon) data acquisition
daemon. It can connect to the MQTT broker to push the data into a [PostgreSQL](https://www.postgresql.org/) or
(Timescale)(https://www.timescale.com/) database.

# Setup
The `Kraken logger` is best installed via the Docker repository provided with this repository.

## ... via [`docker-compose`](https://github.com/docker/compose)

Example `docker-compose.yml` for `Kraken logger`:

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
    image: ghcr.io/patrickbaus/database_logger:master
    container_name: database_logger
    restart: always
    depends_on:
      - db_timescale_sensors
    environment:
      DATABASE_HOST=localhost
      DATABASE_USER=sensors
      DATABASE_PASSWORD=sensors
      MQTT_HOST=my-mqtt-broker
```

# How to extend this image
The `Kraken logger` image can be configured in many ways and the example given above is only a minimalistic example
for educational purposes only. The easiest way is to use environment variables.

## Versioning
I use [SemVer](http://semver.org/) for versioning. For the versions available, see the
[tags on this repository](https://github.com/PatrickBaus/pyAsyncPrologix/tags).

## Documentation
I use the [Numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html) style for documentation.

## Authors
* **Patrick Baus** - *Initial work* - [PatrickBaus](https://github.com/PatrickBaus)

## License
This project is licensed under the GPL v3 license - see the [LICENSE](LICENSE) file for details
