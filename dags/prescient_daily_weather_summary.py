from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from scripts.openweather_etl.daily_etl import (
    locations_config,
)
import os
from pathlib import Path
import scripts.utils as utils

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(5),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    "daily_weather_data_summary_pipeline",
    default_args=default_args,
    description="Fetches daily summary weather data from API, writes to tmp dir, and validates using Pydantic, "
    "transforms data and persists to postgres fact and dim tables using merge statements",
    schedule_interval=timedelta(days=1),
    catchup=True,
)
def daily_weather_summary_etl():
    @task(
        on_retry_callback=utils.on_retry_callback,
        on_failure_callback=utils.on_failure_callback,
        on_success_callback=utils.on_success_callback,
    )
    def fetch_and_write_source_data(logical_date: str):
        from scripts.openweather_etl.daily_etl import (
            create_requests,
            fetch_weather_data,
            write_list_to_json,
        )

        OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")
        tmp_dir = "/opt/airflow/tmp"
        print(f"Received {logical_date=} from airflow context.")
        request_to_get = create_requests(
            locations=locations_config, api_key=OPENWEATHER_API_KEY, date=logical_date
        )
        print(
            f"Used {logical_date=} and generated date to pass to api in get request {request_to_get[0].date=}"
        )
        weather_data = fetch_weather_data(request_to_get)
        tmp_path = write_list_to_json(data=weather_data, dir_path=Path(tmp_dir))
        print(f"Request Successful. Received response.")
        return str(tmp_path)

    @task(
        on_retry_callback=utils.on_retry_callback,
        on_failure_callback=utils.on_failure_callback,
        on_success_callback=utils.on_success_callback,
    )
    def validate_source_data(tmp_path: str):
        from scripts.openweather_etl.daily_etl import validate_json_data, WeatherData

        print(tmp_path)
        if not tmp_path:
            raise ValueError(
                f"No valid path received from the previous task. Unable to validate {tmp_path=}."
            )

        tmp_path = Path(tmp_path)
        try:
            validated_data = validate_json_data(
                json_path=tmp_path, valid_data_model=WeatherData
            )
        except Exception as e:
            print(f"An error occurred during validation: {e}")
            raise
        print(f"Validation Successful for data in {tmp_path=}.")
        return str(tmp_path)

    @task(
        on_retry_callback=utils.on_retry_callback,
        on_failure_callback=utils.on_failure_callback,
        on_success_callback=utils.on_success_callback,
    )
    def transform_source_data(tmp_path: str):
        print(tmp_path)
        if not tmp_path:
            raise ValueError(
                f"No valid path received from the previous task. Unable to validate {tmp_path=}."
            )

        tmp_path = Path(tmp_path)

        from scripts.openweather_etl.daily_etl import (
            normalize_json_to_polars,
            stage_data,
            split_to_fact_dimension,
        )

        print(f"Creating flattened json polars dataframe using {tmp_path=}")
        df_flattened = normalize_json_to_polars(json_file_path=tmp_path)
        print(f"Successfully flattened json polars dataframe using {tmp_path=}")

        print("Creating staged dataframe from flattened dataframe.")
        df_staged = stage_data(df_pl=df_flattened, json_file_path=tmp_path)

        df_fact_daily_weather_summary, df_dim_location = split_to_fact_dimension(
            df=df_staged
        )
        df_fact_daily_weather_summary.glimpse()
        df_dim_location.glimpse()
        print("Successfully created dim and fact from stage")

    start_task = DummyOperator(task_id="start")

    fetch_write_task = fetch_and_write_source_data(logical_date="{{ ds }}")

    validate_task = validate_source_data(tmp_path=fetch_write_task)

    transform_source_data = transform_source_data(tmp_path=validate_task)

    start_task >> fetch_write_task >> validate_task >> transform_source_data


daily_weather_summary_etl_dag = daily_weather_summary_etl()
