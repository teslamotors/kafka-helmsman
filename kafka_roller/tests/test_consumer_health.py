from unittest.mock import patch
from kafka_roller import consumer_health as health
import pytest


@patch("kafka_roller.consumer_health.requests.get")
def test_consumer_is_healthy(mock_get):
    response = {
        "status": {
            "status": "OK",
            # ...rest of the details..
        }
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = response
    assert health.consumer_is_healthy("http://localhost", "test_consumer")


@patch("kafka_roller.consumer_health.requests.get")
def test_consumer_not_healthy(mock_get):
    response = {
        "status": {
            "status": "WARN",
            # ...rest of the details..
        }
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = response
    assert not health.consumer_is_healthy("http://localhost", "test_consumer")


@patch("kafka_roller.consumer_health.requests.get")
def test_consumer_missing(mock_get):
    mock_get.return_value.status_code = 404
    with pytest.raises(SystemExit):
        health.consumer_is_healthy("http://localhost", "test_consumer")
