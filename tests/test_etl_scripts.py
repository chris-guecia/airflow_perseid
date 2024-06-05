import pytest
from scripts.openweather_etl.daily_etl import get_target_date, OpenWeatherAPIDailyAggregateParameters
from pendulum.parsing.exceptions import ParserError


@pytest.mark.parametrize(
    "logical_date, expected_target_date",
    [
        ("2024-05-28", "2024-05-27"),  # Normal scenario
        ("2024-03-01", "2024-02-29"),  # Leap year scenario
        ("2024-06-01", "2024-05-31"),  # End of the month scenario
        ("2024-01-01", "2023-12-31"),  # Start of the year scenario
    ]
)
def test_get_target_date(logical_date, expected_target_date):
    assert get_target_date(logical_date) == expected_target_date


@pytest.mark.parametrize(
    "logical_date",
    [
        "2024-5-28",  # Invalid input format
        "2024-02-30",  # Invalid date
    ]
)
def test_get_target_date_invalid_input(logical_date):
    with pytest.raises(ParserError):
        get_target_date(logical_date)


def test_valid_api_parameters():
    params = OpenWeatherAPIDailyAggregateParameters(
        lat=40.7128,
        lon=-74.0060,
        date='2023-05-28',
        appid='API_KEY'
    )
    assert params.lat == 40.7128
    assert params.lon == -74.0060
    assert params.date == '2023-05-28'
    assert params.appid == 'API_KEY'
    assert params.units == 'imperial'
    assert params.lang == 'en'


def test_optional_parameters():
    params = OpenWeatherAPIDailyAggregateParameters(
        lat=40.7128,
        lon=-74.0060,
        date='2023-05-28',
        appid='API_KEY',
        units='metric',
        lang='fr'
    )
    assert params.units == 'metric'
    assert params.lang == 'fr'


def test_invalid_latitude():
    with pytest.raises(ValueError, match="Latitude must be between -90 and 90."):
        OpenWeatherAPIDailyAggregateParameters(
            lat=100,
            lon=-74.0060,
            date='2023-05-28',
            appid='API_KEY'
        )


def test_invalid_longitude():
    with pytest.raises(ValueError, match="Longitude must be between -180 and 180."):
        OpenWeatherAPIDailyAggregateParameters(
            lat=40.7128,
            lon=200,
            date='2023-05-28',
            appid='API_KEY'
        )
