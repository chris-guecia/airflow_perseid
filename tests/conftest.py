import pytest
from unittest.mock import patch
from scripts.openweather_etl.daily_etl import create_requests


@pytest.fixture
def mocked_requests_get():
    with patch('scripts.openweather_etl.daily_etl.requests.get') as mock_get:
        yield mock_get


@pytest.fixture
def mock_daily_summary_response(mocked_requests_get):
    mock_response = {
            "lat": 39.9526,
            "lon": -75.1652,
            "tz": "-04:00",
            "date": "2024-06-07",
            "units": "imperial",
            "cloud_cover": {
              "afternoon": 14
            },
            "humidity": {
              "afternoon": 43
            },
            "precipitation": {
              "total": 0
            },
            "temperature": {
              "min": 66.49,
              "max": 84.11,
              "afternoon": 82.4,
              "night": 74.26,
              "evening": 81.64,
              "morning": 66.49
            },
            "pressure": {
              "afternoon": 1001
            },
            "wind": {
              "max": {
                "speed": 23.02,
                "direction": 240
              }
            }
        }

    mocked_requests_get.return_value.status_code = 200
    mocked_requests_get.return_value.json.return_value = mock_response
    location = {"location_name": "Philadelphia", "lat": 39.9526, "lon": -75.1652, "tz": "America/New_York"}
    test_extract = create_requests(locations=[location], api_key="foo", date='2024-06-08')
    yield test_extract
