from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
import os

# from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="hello_world_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)


def run_print_hello_world():
    dummy_value = os.environ.get("DUMMY_KEY")
    print(f"The value of DUMMY_KEY is: {dummy_value}")
    print("I am coming first Hello World")
    print("polars imported")
    print("boto3 imported")


def write_to_tmp_dir(**context):
    tmp_dir = "/opt/airflow/tmp"
    execution_date = context["execution_date"].strftime("%Y%m%d_%H%M%S")
    file_name = f"example_{execution_date}.txt"
    file_path = os.path.join(tmp_dir, file_name)
    with open(file_path, "w") as file:
        file.write("This is a test file.")
    print(f"File written to: {file_path}")
    return file_path


def run_print_hello_world_again(file_path):
    print("I am coming next Hello again")
    print(f"Processing file: {file_path}")


def run_print_final_hello():
    print("I am the last hello! See Ya!")


with dag:
    run_print_hello_world = PythonOperator(
        task_id="run_print_hello_world", python_callable=run_print_hello_world
    )

    run_write_to_tmp_dir_on_local_machine = PythonOperator(
        task_id="run_write_to_tmp_dir_on_local_machine",
        python_callable=write_to_tmp_dir,
        provide_context=True,
    )

    run_print_hello_world_again = PythonOperator(
        task_id="run_print_hello_world_again",
        python_callable=run_print_hello_world_again,
        op_kwargs={
            "file_path": '{{ ti.xcom_pull(task_ids="run_write_to_tmp_dir_on_local_machine") }}'
        },
    )

    run_print_final_hello = PythonOperator(
        task_id="run_print_final_hello", python_callable=run_print_final_hello
    )

    (
        run_print_hello_world
        >> run_write_to_tmp_dir_on_local_machine
        >> run_print_hello_world_again
        >> run_print_final_hello
    )
