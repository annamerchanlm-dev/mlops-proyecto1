#!/bin/sh
# Inicializa MinIO: bucket 'models', políticas limitadas al bucket y usuarios de servicio.
# Idempotente: se puede ejecutar varias veces sin error.
set -e

echo ">> Conectando a MinIO como root"
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

echo ">> Bucket models"
mc mb --ignore-existing local/models

echo ">> Políticas"
mc admin policy create local models-readwrite /policies/models-readwrite.json
mc admin policy create local models-readonly  /policies/models-readonly.json

echo ">> Usuarios"
mc admin user add local "$MINIO_TRAINER_USER"   "$MINIO_TRAINER_PASSWORD"
mc admin user add local "$MINIO_INFERENCE_USER" "$MINIO_INFERENCE_PASSWORD"

echo ">> Asignación de políticas (si ya estaban asignadas, se ignora)"
mc admin policy attach local models-readwrite --user "$MINIO_TRAINER_USER"   || true
mc admin policy attach local models-readonly  --user "$MINIO_INFERENCE_USER" || true

echo ">> Estado final"
mc admin user list local
echo ">> MinIO inicializado"
