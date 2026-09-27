#!/bin/sh
# Inicializa MinIO: buckets, políticas limitadas por bucket y usuarios de servicio.
# Idempotente: se puede ejecutar varias veces sin error.
#   models    -> artefactos de modelos (trainer escribe, inference solo lee)
#   datasets  -> snapshots de los datos de entrenamiento (solo trainer; sin borrado: son historia)
set -e

echo ">> Conectando a MinIO como root"
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo ">> Buckets"
mc mb --ignore-existing local/models
mc mb --ignore-existing local/datasets

echo ">> Políticas"
mc admin policy create local models-readwrite   /policies/models-readwrite.json
mc admin policy create local models-readonly    /policies/models-readonly.json
mc admin policy create local datasets-readwrite /policies/datasets-readwrite.json

echo ">> Usuarios"
mc admin user add local "$MINIO_TRAINER_USER"   "$MINIO_TRAINER_PASSWORD"
mc admin user add local "$MINIO_INFERENCE_USER" "$MINIO_INFERENCE_PASSWORD"

echo ">> Asignación de políticas (si ya estaban asignadas, se ignora)"
mc admin policy attach local models-readwrite   --user "$MINIO_TRAINER_USER"   || true
mc admin policy attach local datasets-readwrite --user "$MINIO_TRAINER_USER"   || true
mc admin policy attach local models-readonly    --user "$MINIO_INFERENCE_USER" || true

echo ">> Estado final"
mc admin user list local
echo ">> MinIO inicializado"
