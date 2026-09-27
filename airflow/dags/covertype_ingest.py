"""
DAG covertype_ingest - Proyecto 1 MLOps (Grupo 2)

Cada ejecución hace UNA sola petición a la Data API y el proceso completo:

    fetch_and_store_raw >> preprocess >> build_train_table >> report

  fetch_and_store_raw  GET /data?group_number=N (una vez)  -> raw.fetch_log + raw.covertype
  preprocess           castear, validar, deduplicar          -> processed.covertype + processed.run_stats
  build_train_table    reconstruir la tabla de entrenamiento -> train.covertype
  report               resumen del estado acumulado en el log

Reglas:
  - Una petición por ejecución: fetch_and_store_raw tiene retries=0.
  - Idempotencia por run: cada tarea borra primero lo que este mismo run haya escrito.
  - Los datos viajan por Postgres, nunca por XCom.
  - Cuando la API responde 400 "ya se recolectó...", se registra status='exhausted',
    se pausa el DAG automáticamente y el run queda en 'skipped'.

Configuración (variables de entorno, ver .env):
  DATA_API_URL, GROUP_NUMBER, DAG_SCHEDULE_SECONDS, COVERTYPE_DB_URL
"""
import hashlib
import os
from contextlib import contextmanager
from datetime import datetime, timedelta

import psycopg2
import requests
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
DAG_ID = "covertype_ingest"
DATA_API_URL = os.environ.get("DATA_API_URL", "http://data-api:80").rstrip("/")
GROUP_NUMBER = int(os.environ.get("GROUP_NUMBER", "2"))
SCHEDULE_SECONDS = int(os.environ.get("DAG_SCHEDULE_SECONDS", "300"))
DB_DSN = os.environ.get("COVERTYPE_DB_URL", "").replace("postgresql+psycopg2://", "postgresql://", 1)

# Columnas en el orden en que las entrega la API
COLUMNS = [
    "elevation", "aspect", "slope",
    "horizontal_distance_to_hydrology", "vertical_distance_to_hydrology",
    "horizontal_distance_to_roadways",
    "hillshade_9am", "hillshade_noon", "hillshade_3pm",
    "horizontal_distance_to_fire_points",
    "wilderness_area", "soil_type", "cover_type",
]
N_COLUMNS = len(COLUMNS)  # 13

WILDERNESS = {"Rawah", "Neota", "Commanche", "Cache"}
SOIL_CODES = {
    "C2702", "C2703", "C2704", "C2705", "C2706", "C2717", "C3501", "C3502",
    "C4201", "C4703", "C4704", "C4744", "C4758", "C5101", "C5151", "C6101",
    "C6102", "C6731", "C7101", "C7102", "C7103", "C7201", "C7202", "C7700",
    "C7701", "C7702", "C7709", "C7710", "C7745", "C7746", "C7755", "C7756",
    "C7757", "C7790", "C8703", "C8707", "C8708", "C8771", "C8772", "C8776",
}

# Rangos físicos por posición de la columna numérica (los mismos CHECK de processed)
RANGES = {
    1: (0, 360),      # aspect
    2: (0, 90),       # slope
    3: (0, None),     # horizontal_distance_to_hydrology
    5: (0, None),     # horizontal_distance_to_roadways
    6: (0, 255),      # hillshade_9am
    7: (0, 255),      # hillshade_noon
    8: (0, 255),      # hillshade_3pm
    9: (0, None),     # horizontal_distance_to_fire_points
}

EXHAUSTED_MARKER = "recolect"  # "Ya se recolectó toda la información minima necesaria"


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
@contextmanager
def db():
    """Cursor en una transacción: commit si todo sale bien, rollback si hay excepción."""
    conn = psycopg2.connect(DB_DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                yield cur
    finally:
        conn.close()


def _insert_log(cur, run_id, group, batch, http_status, rows, status, detail):
    cur.execute(
        """
        INSERT INTO raw.fetch_log
            (dag_run_id, group_number, batch_number, http_status, rows_received, status, detail)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, group, batch, http_status, rows, status, detail),
    )


def _registrar_error(run_id, http_status, detail):
    with db() as cur:
        _insert_log(cur, run_id, GROUP_NUMBER, None, http_status, 0, "error", str(detail)[:1000])


def _pausar_dag():
    """Pausa este DAG cuando la API ya entregó todos los batches."""
    try:
        from airflow.models.dag import DagModel

        dag_model = DagModel.get_dagmodel(DAG_ID)
        if dag_model is not None:
            dag_model.set_is_paused(is_paused=True)
            print(f"[fetch] DAG '{DAG_ID}' pausado automáticamente.")
    except Exception as exc:  # no debe tumbar la tarea
        print(f"[fetch] AVISO: no se pudo pausar el DAG ({exc}). Páusalo desde la UI.")


def _to_int(valor):
    """'2596' -> 2596; rechaza vacíos, decimales no enteros, NaN e infinitos."""
    numero = float(str(valor).strip())
    if not numero.is_integer():
        raise ValueError(f"no entero: {valor}")
    return int(numero)


def _limpiar(valores):
    """Devuelve la fila tipada y validada, o None si es inválida."""
    if len(valores) != N_COLUMNS:
        return None
    try:
        numericas = [_to_int(v) for v in valores[:10]]
        cover_type = _to_int(valores[12])
    except (TypeError, ValueError, OverflowError):
        return None

    for idx, (minimo, maximo) in RANGES.items():
        if numericas[idx] < minimo or (maximo is not None and numericas[idx] > maximo):
            return None

    wilderness = str(valores[10]).strip()
    soil = str(valores[11]).strip()
    if wilderness not in WILDERNESS or soil not in SOIL_CODES:
        return None

    return (*numericas, wilderness, soil, cover_type)


def _row_hash(fila):
    return hashlib.sha256("|".join(str(v) for v in fila).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Tareas
# ---------------------------------------------------------------------------
def fetch_and_store_raw(**context):
    """UNA petición a la API; guarda la respuesta tal cual en raw."""
    run_id = context["run_id"]

    # Idempotencia: borra lo que este run haya escrito antes (CASCADE a raw.covertype y run_stats)
    with db() as cur:
        cur.execute("DELETE FROM raw.fetch_log WHERE dag_run_id = %s", (run_id,))

    url = f"{DATA_API_URL}/data"
    print(f"[fetch] GET {url}?group_number={GROUP_NUMBER}   run_id={run_id}")
    try:
        resp = requests.get(url, params={"group_number": GROUP_NUMBER}, timeout=60)
    except requests.RequestException as exc:
        _registrar_error(run_id, None, f"Error de red: {exc}")
        raise

    # Fin de la recolección: la API ya entregó todos los batches
    if resp.status_code == 400 and EXHAUSTED_MARKER in resp.text.lower():
        with db() as cur:
            _insert_log(cur, run_id, GROUP_NUMBER, None, 400, 0, "exhausted", resp.text[:1000])
        print("=" * 72)
        print("[fetch] RECOLECCIÓN COMPLETA: la API reporta todos los batches consumidos.")
        print(f"[fetch] Respuesta de la API: {resp.text}")
        _pausar_dag()
        print("=" * 72)
        raise AirflowSkipException("Recolección completa: la API no tiene batches nuevos")

    if resp.status_code != 200:
        _registrar_error(run_id, resp.status_code, resp.text)
        raise RuntimeError(f"La API respondió {resp.status_code}: {resp.text[:300]}")

    try:
        payload = resp.json()
        group = int(payload["group_number"])
        batch = int(payload["batch_number"])
        filas = payload["data"]
        if not isinstance(filas, list) or not filas:
            raise ValueError("'data' vacío o con formato inesperado")
        malas = sum(1 for f in filas if len(f) != N_COLUMNS)
        if malas:
            raise ValueError(f"{malas} filas no tienen {N_COLUMNS} columnas")
    except (ValueError, KeyError, TypeError) as exc:
        _registrar_error(run_id, resp.status_code, f"Respuesta inválida: {exc}")
        raise

    if group != GROUP_NUMBER:
        print(f"[fetch] AVISO: se pidió el grupo {GROUP_NUMBER} y la API respondió el grupo {group}")

    # fetch_log + filas en UNA transacción: o queda todo o no queda nada
    columnas_sql = ", ".join(COLUMNS)
    with db() as cur:
        _insert_log(cur, run_id, group, batch, resp.status_code, len(filas), "ok", None)
        execute_values(
            cur,
            f"INSERT INTO raw.covertype (dag_run_id, batch_number, {columnas_sql}) VALUES %s",
            [(run_id, batch, *fila) for fila in filas],
            page_size=1000,
        )
    print(f"[fetch] OK grupo={group} batch={batch} filas={len(filas)} -> raw.covertype")


def preprocess(**context):
    """raw (TEXT) de este run -> processed (tipado, validado, deduplicado)."""
    run_id = context["run_id"]
    columnas_sql = ", ".join(COLUMNS)

    with db() as cur:
        cur.execute(
            "SELECT batch_number FROM raw.fetch_log WHERE dag_run_id = %s AND status = 'ok'",
            (run_id,),
        )
        fila_log = cur.fetchone()
        if fila_log is None:
            raise RuntimeError(f"No hay fetch 'ok' para el run {run_id}")
        batch = fila_log[0]

        # Idempotencia: quita lo que este run haya insertado antes
        cur.execute("DELETE FROM processed.covertype WHERE first_dag_run_id = %s", (run_id,))
        cur.execute("DELETE FROM processed.run_stats WHERE dag_run_id = %s", (run_id,))

        cur.execute(f"SELECT {columnas_sql} FROM raw.covertype WHERE dag_run_id = %s", (run_id,))
        crudas = cur.fetchall()

        validas = [f for f in (_limpiar(c) for c in crudas) if f is not None]
        invalidas = len(crudas) - len(validas)

        # Hash por fila; dentro del mismo batch también puede haber repetidas
        unicas = {}
        for fila in validas:
            unicas.setdefault(_row_hash(fila), fila)

        insertadas = execute_values(
            cur,
            f"""
            INSERT INTO processed.covertype
                (row_hash, {columnas_sql}, first_dag_run_id, first_batch_number)
            VALUES %s
            ON CONFLICT (row_hash) DO NOTHING
            RETURNING row_hash
            """,
            [(h, *fila, run_id, batch) for h, fila in unicas.items()],
            page_size=1000,
            fetch=True,
        )
        nuevas = len(insertadas)
        duplicadas = len(validas) - nuevas

        cur.execute(
            """
            INSERT INTO processed.run_stats
                (dag_run_id, rows_raw, rows_invalid, rows_duplicate, rows_new)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (run_id, len(crudas), invalidas, duplicadas, nuevas),
        )

    print(
        f"[preprocess] batch={batch} crudas={len(crudas)} inválidas={invalidas} "
        f"duplicadas={duplicadas} nuevas={nuevas}"
    )


def build_train_table(**context):
    """Reconstruye train.covertype con todo lo acumulado en processed."""
    columnas_sql = ", ".join(COLUMNS)
    with db() as cur:
        cur.execute("TRUNCATE train.covertype")
        cur.execute(
            f"INSERT INTO train.covertype ({columnas_sql}) "
            f"SELECT {columnas_sql} FROM processed.covertype"
        )
        cur.execute("SELECT count(*) FROM train.covertype")
        total = cur.fetchone()[0]
    print(f"[train] train.covertype reconstruida: {total} filas")


def report(**context):
    """Resumen del estado acumulado: evidencia para la entrega."""
    run_id = context["run_id"]
    with db() as cur:
        cur.execute(
            "SELECT group_number, batch_number, rows_received FROM raw.fetch_log WHERE dag_run_id = %s",
            (run_id,),
        )
        group, batch, recibidas = cur.fetchone()

        cur.execute(
            "SELECT rows_invalid, rows_duplicate, rows_new FROM processed.run_stats WHERE dag_run_id = %s",
            (run_id,),
        )
        invalidas, duplicadas, nuevas = cur.fetchone()

        cur.execute(
            """
            SELECT count(*) FILTER (WHERE status = 'ok'),
                   count(DISTINCT batch_number) FILTER (WHERE status = 'ok'),
                   count(*) FILTER (WHERE status = 'exhausted'),
                   count(*) FILTER (WHERE status = 'error')
            FROM raw.fetch_log
            """
        )
        runs_ok, batches, exhausted, errores = cur.fetchone()

        cur.execute(
            """
            SELECT (SELECT count(*) FROM raw.covertype),
                   (SELECT count(*) FROM processed.covertype),
                   (SELECT count(*) FROM train.covertype)
            """
        )
        n_raw, n_processed, n_train = cur.fetchone()

        cur.execute("SELECT cover_type, count(*) FROM train.covertype GROUP BY 1 ORDER BY 1")
        clases = dict(cur.fetchall())

    print("=" * 72)
    print(f"Run: {run_id}   grupo {group} · batch {batch}")
    print(f"Batches distintos recolectados: {batches} | runs ok: {runs_ok} | "
          f"exhausted: {exhausted} | errores: {errores}")
    print(f"Filas acumuladas: raw {n_raw} | processed {n_processed} | train {n_train}")
    print(f"Este run: {recibidas} recibidas · {nuevas} nuevas · "
          f"{duplicadas} duplicadas · {invalidas} inválidas")
    print(f"Clases en train: {clases}")
    print("=" * 72)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "grupo2",
    "retries": 1,                       # preprocess / train / report son idempotentes
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id=DAG_ID,
    description="Una petición a la Data API por ejecución: raw -> processed -> train",
    start_date=datetime(2026, 9, 1),
    schedule=timedelta(seconds=SCHEDULE_SECONDS),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=4),
    default_args=default_args,
    tags=["proyecto1", "ingesta"],
    doc_md=__doc__,
) as dag:
    t_fetch = PythonOperator(
        task_id="fetch_and_store_raw",
        python_callable=fetch_and_store_raw,
        retries=0,                      # un reintento sería una SEGUNDA petición en el mismo run
        execution_timeout=timedelta(minutes=2),
    )
    t_prep = PythonOperator(task_id="preprocess", python_callable=preprocess)
    t_train = PythonOperator(task_id="build_train_table", python_callable=build_train_table)
    t_report = PythonOperator(task_id="report", python_callable=report)

    t_fetch >> t_prep >> t_train >> t_report
