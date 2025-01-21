"""
Pydantic type definitions for the input validator
"""

import re

from pydantic import BaseModel, field_validator

# A regular expression to match a hostname with an optional port.
# It adheres to RFC 1035 (https://www.rfc-editor.org/rfc/rfc1035) and matches ports
# between 0-65535.
HOSTNAME_REGEX = (
    r"^((?=.{1,255}$)[0-9A-Za-z](?:(?:[0-9A-Za-z]|-){0,61}[0-9A-Za-z])?(?:\.[0-9A-Za-z](?:(?:["
    r"0-9A-Za-z]|-){0,61}[0-9A-Za-z])?)*\.?)(?:\:([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{"
    r"2}|655[0-2][0-9]|6553[0-5]))?$"
)


class MQTTParams(BaseModel):
    """
    Parameters used to connect to the MQTT broker.

    Parameters
    ----------
    hosts: List of Tuple of str and int
        A list of host:port tuples. The list contains the servers of a cluster. If no port is provided it defaults to
        1883. If port number 0 is provided the default value of 1883 is used.
    identifier: str or None
        An MQTT client id used to uniquely identify a client to persist messages.
    username: str or None
        The username used for authentication. Set to None if no username is required
    password: str or None
        The password used for authentication. Set to None if no username is required
    """

    hosts: list[tuple[str, int]]
    identifier: str | None
    username: str | None
    password: str | None

    @field_validator("hosts", mode="before")
    @classmethod
    def ensure_list_of_hosts(cls, value: str) -> list[tuple[str, int]]:
        """
        Parse
        Parameters
        ----------
        value: str
            Either a single hostname:port string or a comma separated list of hostname:port strings.

        Returns
        -------
        list of tuple of str and int
            A list of (hostname, port) tuples.
        """
        hosts = value.split(",")
        result = []
        for host in hosts:
            host = host.strip()
            match = re.search(HOSTNAME_REGEX, host)
            if match is None:
                raise ValueError(f"'{value}' is not a valid hostname or list of hostnames.")
            result.append(
                (
                    match.group(1),
                    int(match.group(2)) if match.group(2) and not match.group(2) == "0" else 1883,
                )
            )
        return result
