#!/bin/bash
# Crea el rol de solo lectura que usa Jupyter.
# Es .sh (no .sql) porque necesita leer la contraseña desde una variable de entorno.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
CREATE ROLE covertype_reader LOGIN PASSWORD '${POSTGRES_READER_PASSWORD}';
GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO covertype_reader;
GRANT USAGE ON SCHEMA raw, processed, train TO covertype_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA raw, processed, train TO covertype_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw, processed, train
    GRANT SELECT ON TABLES TO covertype_reader;
EOSQL

echo "Rol covertype_reader creado (solo lectura)"
