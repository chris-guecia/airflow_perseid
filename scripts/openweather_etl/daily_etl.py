import requests
import pendulum
import time
from dataclasses import dataclass
from typing import Dict, List, Union
from pydantic import BaseModel, Field, ValidationError
import os
from dotenv import load_dotenv
from pathlib import Path
import json

load_dotenv()

# EXTRACT LOGIC task 1
# Load location make these a config file or add to a config file
locations_config = [
    {
        "location_name": "New York City",
        "lat": 40.7128,
        "lon": -74.0059,
        "tz": "America/New_York",
    },
    {
        "location_name": "Philadelphia",
        "lat": 39.9526,
        "lon": -75.1652,
        "tz": "America/New_York",
    },
    {
        "location_name": "Los Angeles",
        "lat": 34.0522,
        "lon": -118.2437,
        "tz": "America/Los_Angeles",
    },
]

OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")
local_tmp_dir = os.environ.get("local_tmp_dir")


@dataclass(frozen=True)
class OpenWeatherAPIDailyAggregateParameters:
    location_name: str
    lat: float
    lon: float
    date: str
    appid: str | None
    units: str = "imperial"
    lang: str = "en"
    url_endpoint: str = "https://api.openweathermap.org/data/3.0/onecall/day_summary"

    def __post_init__(self):
        if not -90 <= self.lat <= 90:
            raise ValueError("Latitude must be between -90 and 90.")
        if not -180 <= self.lon <= 180:
            raise ValueError("Longitude must be between -180 and 180.")

    def get_url(self):
        url = f"{self.url_endpoint}?lat={self.lat}&lon={self.lon}&date={self.date}&appid={self.appid}&units={self.units}&lang={self.lang}"
        # print(url)
        return url


def set_target_date(logical_date: str) -> str:
    # Parse the execution date string and create a datetime object
    execution_datetime = pendulum.parse(logical_date)

    # Subtract one day from the execution datetime to get the target date
    target_date = execution_datetime.subtract(days=1)
    print(target_date)

    return target_date.to_date_string()


def create_requests(
    locations: [Dict[str, Union[str, float]]], api_key: str | None, date: str
) -> List[OpenWeatherAPIDailyAggregateParameters]:
    requests_to_send = []
    for item in locations:
        request = OpenWeatherAPIDailyAggregateParameters(
            location_name=item["location_name"],
            lat=item["lat"],
            lon=item["lon"],
            date=set_target_date(date),
            appid=api_key,
        )
        requests_to_send.append(request)
    return requests_to_send


def fetch_weather_data(
    get_requests: List[OpenWeatherAPIDailyAggregateParameters], rate_limit: int = 10
) -> List[Dict]:
    weather_data = []
    for request in get_requests:
        try:
            response = requests.get(request.get_url())
            response.raise_for_status()  # Raise an exception if the status code is not 200
            data = response.json()
            data["location_name"] = (
                request.location_name
            )  # Add the location name to the response data
            weather_data.append(data)
        except requests.exceptions.RequestException as e:
            error_message = f"Error fetching weather data for {request.location_name}. Status code: {response.status_code}"
            raise Exception(error_message) from e
        time.sleep(rate_limit)  # Rate-limiting mechanism

    return weather_data


def write_list_to_json(data: List[Dict], filename: str) -> Path:
    """
    Writes a list of dictionaries to a JSON file.

    Args:
        data: A list of dictionaries to be written to the file.
        filename: The name of the file to write to.
    """
    named_file = f"raw_source_{data[0]['date']}_{filename}.json"
    # Get the temporary directory using pathlib
    tmp_dir = Path(local_tmp_dir)  # Assuming the tmp folder is at the root
    # Create the file path with the filename in the temporary directory
    file_path = tmp_dir / named_file

    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)  # Add indentation for readability

    return file_path


# VALIDATION LOGIC Task 2
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
    location_name: str
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


def validate_json_data(json_path: Path, weather_data_model: type) -> List:
    """
    Validates the JSON data in the specified file against the provided Pydantic model.

    Args:
        json_path (Path): The path to the JSON file.
        weather_data_model (type): The Pydantic model class representing the expected data structure.

    Returns:
        List: A list of validated data objects if successful, raises an exception otherwise.

    Raises:
        ValidationError: If the JSON data fails validation against the Pydantic model.
    """

    try:
        with open(json_path, "r") as file:
            json_data = json.load(file)

        # Validate each item in the JSON data against the model
        validated_data = [weather_data_model.parse_obj(item) for item in json_data]
        return validated_data

    except ValidationError as e:
        raise Exception(f"Error validating JSON data: {e}") from e


if __name__ == "__main__":
    request_to_get = create_requests(
        locations=locations_config, api_key=OPENWEATHER_API_KEY, date="2024-06-08"
    )
    weather_data = fetch_weather_data(request_to_get)

    tmp_path = write_list_to_json(data=weather_data, filename="test_run")

    # Validate the data using Pydantic
    print("starting source validation")
    weather_data_model = WeatherData

    try:
        validated_data = validate_json_data(
            json_path=tmp_path, weather_data_model=WeatherData
        )
        # Access and process the validated data here (validated_data is a list)
    except Exception as e:
        print(f"An error occurred during validation: {e}")
    print("finished source validation")
