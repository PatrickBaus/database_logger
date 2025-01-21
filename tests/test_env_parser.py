"""
Tests for the input environment variable parser.
"""

import pytest
from pydantic import ValidationError

from typedefs import MQTTParams


@pytest.mark.parametrize(
    ["hosts", "result"],
    [
        ["example.com", [("example.com", 1883)]],
        ["example.com:1234", [("example.com", 1234)]],
        ["example1.com,example2.com", [("example1.com", 1883), ("example2.com", 1883)]],
        ["example1.com, example2.com", [("example1.com", 1883), ("example2.com", 1883)]],
        ["example1.com:1234,example2.com", [("example1.com", 1234), ("example2.com", 1883)]],
        ["example1.com:1234,example2.com:0", [("example1.com", 1234), ("example2.com", 1883)]],
    ],
)
def test_mqtt_hostnames_pass(hosts, result):
    """
    Test parsing hostname(s) from env variables.
    """
    env_params = MQTTParams(hosts=hosts, identifier="foo", username="bar", password="12345")

    assert env_params.hosts == result


@pytest.mark.parametrize(
    ["hosts", "expectation"],
    [
        ["example.com:123456", pytest.raises(ValidationError)],
        ["e^xample.com", pytest.raises(ValidationError)],
        ["example.com:-1", pytest.raises(ValidationError)],
    ],
)
def test_mqtt_hostnames_fail(hosts, expectation):
    """
    Test parsing hostname(s) from env variables. Test invalid hostnames.
    """
    with expectation:
        MQTTParams(hosts=hosts, identifier="foo", username="bar", password="12345")
