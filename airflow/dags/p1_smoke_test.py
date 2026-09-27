"""
DAG p1_smoke_test - Diagnóstico de conectividad (Proyecto 1)

Verifica, desde DENTRO de Airflow, que:
  1. La Data API responde (solo GET /, que NO consume el contador de batches).
  2. postgres-data es alcanzable y tiene los esquemas raw / processed / train.

Solo se ejecuta a mano (schedule=None). NUNCA llama a /data.
"""
import os
from datetime import datetime

import requests
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_data_api():
    url = os.environ["DATA_API_URL"].rstrip("/")
    r = requests.get(f"{url}/", timeout=10)  # "/" es el endpoint de estado
    r.raise_for_status()
    print(f"[data-api] GET {url}/ -> {r.status_code} {r.json()}")


def check_covertype_db():
    engine = create_engine(os.environ["COVERTYPE_DB_URL"])
    with engine.connect() as conn:
        esquemas = [row[0] for row in conn.execute(text(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN ('raw', 'processed', 'train') ORDER BY 1"
        ))]
    engine.dispose()
    print(f"[postgres-data] esquemas encontrados: {esquemas}")
    assert esquemas == ["processed", "raw", "train"], "Faltan esquemas en postgres-data"


with DAG(
    dag_id="p1_smoke_test",
    description="Diagnóstico: conectividad con la Data API (GET /) y con postgres-data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["proyecto1", "diagnostico"],
) as dag:
    t1 = PythonOperator(task_id="check_data_api", python_callable=check_data_api)
    t2 = PythonOperator(task_id="check_covertype_db", python_callable=check_covertype_db)
    t1 >> t2