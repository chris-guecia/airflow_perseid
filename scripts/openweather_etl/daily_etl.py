import requests
import pendulum
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple
from pydantic import BaseModel, Field, ValidationError

# EXTRACT LOGIC
# Load location make these a config file or add to a config file
locations = [
    {"name": "New York", "lat": 40.7128, "lon": -74.0059, "tz": "America/New_York"},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437, "tz": "America/Los_Angeles"},
]


@dataclass(frozen=True)
class OpenWeatherAPIDailyAggregateParameters:
    lat: float
    lon: float
    date: str
    appid: str
    units: str = 'standard'
    lang: str = 'en'
    url_endpoint: str = 'https://api.example.com/data'

    def __post_init__(self):
        if not -90 <= self.lat <= 90:
            raise ValueError("Latitude must be between -90 and 90.")
        if not -180 <= self.lon <= 180:
            raise ValueError("Longitude must be between -180 and 180.")

    def get_url(self):
        url = f"{self.url_endpoint}?lat={self.lat}&lon={self.lon}&date={self.date}&appid={self.appid}&units={self.units}&lang={self.lang}"
        return url


def generate_request_urls(request_parameters, url_endpoint) -> Tuple[str, ...]:
    for location in locations:
        lat, lon, tz = location["lat"], location["lon"], location["tz"]
        url = f"https://api.example.com/data/2.5/weather/daily?lat={lat}&lon={lon}&date={date}&tz={tz}&appid={appid}"
        yield url


# VALIDATION LOGIC
class Temperature(BaseModel):
    min: float
    max: float
    afternoon: float
    night: float
    evening: float
    morning: float


class Wind(BaseModel):
    max: Dict[str, float]


class WeatherData(BaseModel):
    lat: float
    lon: float
    tz: str
    date: str
    units: str
    cloud_cover: Dict[str, int] = Field(..., alias="cloud_cover")
    humidity: Dict[str, int]
    precipitation: Dict[str, float]
    temperature: Temperature
    pressure: Dict[str, int]
    wind: Wind


# In your fetch_weather_data function
def fetch_weather_data(urls: Tuple[str, ...], rate_limit: int = 5) -> Dict[str, WeatherData]:
    weather_data = {}
    failed_locations = []
    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            location_name = data["name"]  # Assuming the location name is not part of the response

            try:
                weather_obj = WeatherData(**data)
                weather_data[location_name] = weather_obj
                # task_logger.info(f"Successfully fetched weather data for {location_name}")
            except ValidationError as e:
                # task_logger.error(f"Validation error in the response for {location_name}: {e}")
                failed_locations.append(location_name)
        except requests.exceptions.RequestException as e:
            # task_logger.error(f"Error fetching weather data for {location_name}: {e}")
            failed_locations.append(location_name)
        time.sleep(rate_limit)  # Rate-limiting mechanism

    if failed_locations:
        raise Exception(f"Failed to fetch weather data for the following locations: {', '.join(failed_locations)}")

    return weather_data


def get_target_date(logical_date: str) -> str:
    # Parse the execution date string and create a datetime object
    execution_datetime = pendulum.parse(logical_date)

    # Subtract one day from the execution datetime to get the target date
    target_date = execution_datetime.subtract(days=1)
    print(target_date)

    return target_date.to_date_string()


test_3 = get_target_date(logical_date='2024-05-27')
print(f"{test_3=}")
