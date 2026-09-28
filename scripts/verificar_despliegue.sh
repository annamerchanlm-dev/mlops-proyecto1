#!/usr/bin/env bash
# =============================================================================
# Verifica que el despliegue completo del Proyecto 1 esté sano.
#   Uso (desde la raíz del proyecto):  ./scripts/verificar_despliegue.sh
# No modifica nada: solo consulta estados y permisos.
# =============================================================================
set -u
cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
    echo "No existe .env (cópialo desde .env.example y complétalo)"; exit 1
fi
set -a; . ./.env; set +a

OK=0; FALLA=0
check() {  # $1 = descripción, $2 = comando (éxito = OK)
    if eval "$2" >/dev/null 2>&1; then
        echo "  [OK]    $1"; OK=$((OK + 1))
    else
        echo "  [FALLA] $1"; FALLA=$((FALLA + 1))
    fi
}

MINIO_IMG="pgsty/minio:RELEASE.2026-08-04T00-00-00Z"
MC="docker run --rm --network mlops-proyecto1_default --env-file .env --entrypoint sh $MINIO_IMG -c"
PSQL="docker compose exec -T postgres-data psql -U $POSTGRES_DATA_USER -d covertype -tAc"

echo "== Contenedores"
for s in postgres-airflow postgres-data minio airflow-webserver airflow-scheduler jupyter inference-api adminer; do
    check "$s en ejecución" "docker compose ps --status running --services | grep -qx $s"
done
check "minio-init terminó sin errores"   "[ \"\$(docker inspect -f '{{.State.ExitCode}}' p1_minio_init)\" = 0 ]"
check "airflow-init terminó sin errores" "[ \"\$(docker inspect -f '{{.State.ExitCode}}' p1_airflow_init)\" = 0 ]"

echo "== Postgres (datos)"
check "esquemas raw / processed / train" \
      "[ \"\$($PSQL \"SELECT count(*) FROM information_schema.schemata WHERE schema_name IN ('raw','processed','train')\")\" = 3 ]"
check "6 tablas (incluida la cuarentena processed.rejected)" \
      "[ \"\$($PSQL \"SELECT count(*) FROM information_schema.tables WHERE table_schema IN ('raw','processed','train')\")\" = 6 ]"
check "diccionario de datos (comentarios en train.covertype)" \
      "[ \"\$($PSQL \"SELECT count(*) FROM pg_description d JOIN pg_class c ON c.oid = d.objoid WHERE c.relname = 'covertype' AND d.objsubid > 0\")\" -ge 13 ]"
check "covertype_reader NO puede escribir" \
      "! docker compose exec -T postgres-data psql -U covertype_reader -d covertype -c \"INSERT INTO raw.fetch_log (dag_run_id, group_number, status) VALUES ('prueba_permisos', 2, 'ok')\""

echo "== MinIO"
check "buckets models y datasets" \
      "$MC 'mc alias set l http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null && mc ls l/models && mc ls l/datasets'"
check "trainer puede escribir en models" \
      "$MC 'mc alias set t http://minio:9000 \"\$MINIO_TRAINER_USER\" \"\$MINIO_TRAINER_PASSWORD\" >/dev/null && echo x | mc pipe t/models/_verificacion && mc rm t/models/_verificacion'"
check "inference NO puede escribir en models" \
      "! $MC 'mc alias set i http://minio:9000 \"\$MINIO_INFERENCE_USER\" \"\$MINIO_INFERENCE_PASSWORD\" >/dev/null && echo x | mc pipe i/models/_no_permitido'"

echo "== Airflow"
check "DAG covertype_ingest registrado" \
      "docker compose exec -T airflow-scheduler airflow dags list | grep -q covertype_ingest"
check "sin errores de importación de DAGs" \
      "docker compose exec -T airflow-scheduler airflow dags list-import-errors | grep -q 'No data found'"
check "UI de Airflow responde (8080)" "curl -sf http://localhost:8080/health"

echo "== Jupyter e Inference API"
check "JupyterLab responde (8888)" "curl -s -o /dev/null -w '%{http_code}' http://localhost:8888/ | grep -qE '200|302'"
check "Inference API responde (8000)" "curl -sf http://localhost:8000/health"
check "Swagger de la API disponible (/docs)" "curl -sf http://localhost:8000/docs"

echo "== Data API configurada: ${DATA_API_URL}"
check "Data API alcanzable (solo GET /, no consume batches)" "curl -sf -m 10 ${DATA_API_URL%/}/"

echo
echo "Resultado: ${OK} OK, ${FALLA} con falla"
[ "$FALLA" -eq 0 ]
