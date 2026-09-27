-- =====================================================================
-- Proyecto 1 MLOps - Base covertype: tres etapas de datos
--   raw        -> respuesta de la API tal cual (TEXT)
--   processed  -> tipado, validado y deduplicado
--   train      -> listo para entrenamiento
-- Se ejecuta UNA sola vez: cuando el volumen pg_data se crea vacío.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;
CREATE SCHEMA IF NOT EXISTS train;

COMMENT ON SCHEMA raw       IS 'Etapa 1: datos sin procesar, tal cual los entrega la API';
COMMENT ON SCHEMA processed IS 'Etapa 2: datos tipados, validados y deduplicados';
COMMENT ON SCHEMA train     IS 'Etapa 3: datos listos para entrenamiento';

-- ---------------------------------------------------------------------
-- RAW
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.fetch_log (
    dag_run_id     TEXT        PRIMARY KEY,
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    group_number   INTEGER     NOT NULL,
    batch_number   INTEGER,
    http_status    INTEGER,
    rows_received  INTEGER     NOT NULL DEFAULT 0,
    status         TEXT        NOT NULL CHECK (status IN ('ok', 'exhausted', 'error')),
    detail         TEXT
);
COMMENT ON TABLE raw.fetch_log IS 'Una fila por ejecucion del DAG: grupo, batch servido y resultado';

CREATE TABLE IF NOT EXISTS raw.covertype (
    id                                  BIGSERIAL   PRIMARY KEY,
    dag_run_id                          TEXT        NOT NULL
                                        REFERENCES raw.fetch_log (dag_run_id) ON DELETE CASCADE,
    batch_number                        INTEGER     NOT NULL,
    ingested_at                         TIMESTAMPTZ NOT NULL DEFAULT now(),
    elevation                           TEXT,
    aspect                              TEXT,
    slope                               TEXT,
    horizontal_distance_to_hydrology    TEXT,
    vertical_distance_to_hydrology      TEXT,
    horizontal_distance_to_roadways     TEXT,
    hillshade_9am                       TEXT,
    hillshade_noon                      TEXT,
    hillshade_3pm                       TEXT,
    horizontal_distance_to_fire_points  TEXT,
    wilderness_area                     TEXT,
    soil_type                           TEXT,
    cover_type                          TEXT
);
CREATE INDEX IF NOT EXISTS ix_raw_covertype_run ON raw.covertype (dag_run_id);

-- ---------------------------------------------------------------------
-- PROCESSED
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed.covertype (
    row_hash                            TEXT        PRIMARY KEY,
    elevation                           INTEGER     NOT NULL,
    aspect                              INTEGER     NOT NULL CHECK (aspect BETWEEN 0 AND 360),
    slope                               INTEGER     NOT NULL CHECK (slope BETWEEN 0 AND 90),
    horizontal_distance_to_hydrology    INTEGER     NOT NULL CHECK (horizontal_distance_to_hydrology >= 0),
    vertical_distance_to_hydrology      INTEGER     NOT NULL,
    horizontal_distance_to_roadways     INTEGER     NOT NULL CHECK (horizontal_distance_to_roadways >= 0),
    hillshade_9am                       SMALLINT    NOT NULL CHECK (hillshade_9am  BETWEEN 0 AND 255),
    hillshade_noon                      SMALLINT    NOT NULL CHECK (hillshade_noon BETWEEN 0 AND 255),
    hillshade_3pm                       SMALLINT    NOT NULL CHECK (hillshade_3pm  BETWEEN 0 AND 255),
    horizontal_distance_to_fire_points  INTEGER     NOT NULL CHECK (horizontal_distance_to_fire_points >= 0),
    wilderness_area                     TEXT        NOT NULL,
    soil_type                           TEXT        NOT NULL,
    cover_type                          INTEGER     NOT NULL,
    first_dag_run_id                    TEXT        NOT NULL,
    first_batch_number                  INTEGER     NOT NULL,
    processed_at                        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_processed_first_run ON processed.covertype (first_dag_run_id);

CREATE TABLE IF NOT EXISTS processed.run_stats (
    dag_run_id      TEXT        PRIMARY KEY
                    REFERENCES raw.fetch_log (dag_run_id) ON DELETE CASCADE,
    rows_raw        INTEGER     NOT NULL,
    rows_invalid    INTEGER     NOT NULL,
    rows_duplicate  INTEGER     NOT NULL,
    rows_new        INTEGER     NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE processed.run_stats IS 'Resultado del preprocesamiento de cada ejecucion';

-- ---------------------------------------------------------------------
-- TRAIN
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS train.covertype (
    elevation                           INTEGER  NOT NULL,
    aspect                              INTEGER  NOT NULL,
    slope                               INTEGER  NOT NULL,
    horizontal_distance_to_hydrology    INTEGER  NOT NULL,
    vertical_distance_to_hydrology      INTEGER  NOT NULL,
    horizontal_distance_to_roadways     INTEGER  NOT NULL,
    hillshade_9am                       SMALLINT NOT NULL,
    hillshade_noon                      SMALLINT NOT NULL,
    hillshade_3pm                       SMALLINT NOT NULL,
    horizontal_distance_to_fire_points  INTEGER  NOT NULL,
    wilderness_area                     TEXT     NOT NULL,
    soil_type                           TEXT     NOT NULL,
    cover_type                          INTEGER  NOT NULL
);
COMMENT ON TABLE train.covertype IS 'Features + label; se reconstruye en cada ejecucion del DAG';
