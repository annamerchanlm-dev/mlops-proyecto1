FROM apache/airflow:2.10.5-python3.12

ARG AIRFLOW_VERSION=2.10.5

# Constraints oficiales de Airflow 2.10.5 / Python 3.12 versionadas en el repo (raw.githubusercontent.com esta bloqueado en la VM)
COPY constraints-2.10.5-py3.12.txt /constraints.txt
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt --constraint /constraints.txt
