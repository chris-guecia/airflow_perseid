FROM apache/airflow:2.9.1-python3.11

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt -c https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt

ENV PYTHONPATH="${PYTHONPATH}:/scripts:/opt/airflow/scripts"