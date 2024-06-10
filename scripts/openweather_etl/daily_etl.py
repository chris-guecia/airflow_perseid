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
import polars as pl
import pandas as pd

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


def write_list_to_json(data: List[Dict], dir_path: str | Path) -> Path:
    """
    Writes a list of dictionaries to a JSON file.

    Args:
        data: A list of dictionaries to be written to the file.
        dir_path: The name of the file to write to.
    """
    file_name = f"raw_source_{data[0]['date']}_test_.json"
    # Get the temporary directory using pathlib
    tmp_dir = dir_path  # Assuming the tmp folder is at the root
    # Create the file path with the filename in the temporary directory
    file_path = tmp_dir / file_name

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


def validate_json_data(json_path: Path, valid_data_model: type) -> List:
    """
    Validates the JSON data in the specified file against the provided Pydantic model.

    Args:
        json_path (Path): The path to the JSON file.
        valid_data_model (type): The Pydantic model class representing the expected data structure.

    Returns:
        List: A list of validated data objects if successful, raises an exception otherwise.

    Raises:
        ValueError: If json_path is None.
        ValidationError: If the JSON data fails validation against the Pydantic model.
    """
    if json_path is None:
        raise ValueError(f"{json_path=}. json_path cannot be None")

    try:
        with open(json_path, "r") as file:
            json_data = json.load(file)

        # Validate each item in the JSON data against the model
        validated_data = [valid_data_model.parse_obj(item) for item in json_data]
        return validated_data

    except ValidationError as e:
        raise Exception(f"Error validating JSON data: {e}") from e


# TRANSFORM
def normalize_json_to_polars(json_file_path: Path) -> pl.DataFrame:
    """
    Reads a JSON file, flattens nested structures using pandas,
    and converts the result to a Polars DataFrame.

    Args:
        json_file_path: Path to the JSON file.

    Returns:
        A Polars DataFrame containing the flattened data.
    """

    with open(json_file_path, "r") as file:
        json_data = json.load(file)

    # Flatten with pandas, using '_' as separator
    flattened_df = pd.json_normalize(json_data, sep="_")

    return pl.from_pandas(flattened_df)


def stage_data(df_pl: pl.DataFrame, json_file_path: Path) -> pl.DataFrame:
    source_file = str(json_file_path.name)
    # Improved column renaming with dictionary comprehension
    column_map = {
        "lat": "latitude",
        "lon": "longitude",
        "tz": "time_zone",
        "units": "measurement_unit"
    }
    df_pl = df_pl.rename(column_map)

    df_staged = df_pl.with_columns(
        pl.concat_str(
            [
                pl.col("latitude"),
                pl.col("longitude"),
                pl.col("date"),
            ],
            separator="_",
        ).alias("weather_record_id"),

        pl.lit(source_file).alias("source_file_name"),

        pl.concat_str(
            [
                pl.col("latitude"),
                pl.col("longitude"),
                pl.col("time_zone"),
            ],
            separator="_",
        ).alias("location_id")
    )

    return df_staged


def split_to_fact_dimension(df):
    """
    Splits the prepared weather data dataframe into separate fact and dimension dataframes.

    Args:
        df (pl.DataFrame): The prepared weather data dataframe.

    Returns:
        tuple: A tuple containing two Polars dataframes - fact_table and dimension_table.
    """
    # Fact table columns (consider adjusting based on your schema)
    fact_table_columns = [
        "weather_record_id",
        'location_id',
        "measurement_unit",
        # Weather data columns
        "cloud_cover_afternoon",
        "humidity_afternoon",
        "precipitation_total",
        "temperature_min",
        "temperature_max",
        "temperature_afternoon",
        "temperature_night",
        "temperature_evening",
        "temperature_morning",
        "pressure_afternoon",
        "wind_max_speed",
        "wind_max_direction",
        "source_file_name"
    ]

    # Dimension table columns (consider adjusting based on your schema)
    dimension_table_columns = ["location_id",
                               "location_name",
                               "latitude",
                               "longitude",
                               "source_file_name"]

    fact_table = df.select(fact_table_columns)
    dimension_table = df.select(dimension_table_columns)

    return fact_table, dimension_table


def write_df_to_parquet(df: pl.DataFrame, dir_path: str | Path):
    file_name = f"raw_source_{df['date'].item(0)}_test_.json"
    # Get the temporary directory using pathlib
    tmp_dir = dir_path  # Assuming the tmp folder is at the root
    # Create the file path with the filename in the temporary directory
    file_path = tmp_dir / file_name
    df.write_parquet(file=file_path)

    return file_path


# Persist to Postgres

# db connection manager

# ddl dim fact sql

# temp table sql

# merge statements


if __name__ == "__main__":
    OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")
    local_tmp_dir = os.environ.get("local_tmp_dir")

    request_to_get = create_requests(
        locations=locations_config, api_key=OPENWEATHER_API_KEY, date="2024-06-08"
    )
    weather_data = fetch_weather_data(request_to_get)

    tmp_path = write_list_to_json(data=weather_data, dir_path=Path(local_tmp_dir))

    # Validate the data using Pydantic
    print("starting source validation")
    weather_data_model = WeatherData

    try:
        validated_data = validate_json_data(
            json_path=tmp_path, valid_data_model=WeatherData
        )
    except Exception as e:
        print(f"An error occurred during validation: {e}")
    print("finished source validation")
