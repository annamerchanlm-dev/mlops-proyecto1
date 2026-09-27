-- =====================================================================
-- Migración 03: cuarentena de filas rechazadas + diccionario de datos
--
-- Idempotente: se puede aplicar varias veces sin error.
--   * En un volumen nuevo la ejecuta el entrypoint de Postgres (después de 01 y 02).
--   * En la base que ya existe se aplica a mano:
--       docker compose exec -T postgres-data psql -U covertype -d covertype \
--         -v ON_ERROR_STOP=1 < db/init/03_rejected_and_dictionary.sql
-- =====================================================================

-- ---------------------------------------------------------------------
-- Cuarentena: ninguna fila se descarta sin dejar rastro
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS processed.rejected (
    id            BIGSERIAL   PRIMARY KEY,
    dag_run_id    TEXT        NOT NULL
                  REFERENCES raw.fetch_log (dag_run_id) ON DELETE CASCADE,
    batch_number  INTEGER     NOT NULL,
    reason        TEXT        NOT NULL,
    raw_row       JSONB       NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejected_run    ON processed.rejected (dag_run_id);
CREATE INDEX IF NOT EXISTS ix_rejected_reason ON processed.rejected (reason);

COMMENT ON TABLE  processed.rejected            IS 'Cuarentena: filas de raw que no pasaron la validacion, con el motivo';
COMMENT ON COLUMN processed.rejected.reason     IS 'Motivo del rechazo (ej. "slope fuera de rango", "soil_type desconocido")';
COMMENT ON COLUMN processed.rejected.raw_row    IS 'Fila original tal como llego de la API (columna -> valor texto)';

-- El lector (Jupyter) puede consultar la cuarentena
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'covertype_reader') THEN
        GRANT SELECT ON processed.rejected TO covertype_reader;
    END IF;
END $$;

-- ---------------------------------------------------------------------
-- Diccionario de datos (visible en Adminer y con \d+ en psql)
-- ---------------------------------------------------------------------
-- raw.fetch_log
COMMENT ON COLUMN raw.fetch_log.dag_run_id    IS 'Run ID de Airflow que hizo la peticion (una peticion por run)';
COMMENT ON COLUMN raw.fetch_log.requested_at  IS 'Momento de la peticion a la Data API';
COMMENT ON COLUMN raw.fetch_log.group_number  IS 'Numero de grupo enviado a la API (grupo 2)';
COMMENT ON COLUMN raw.fetch_log.batch_number  IS 'Batch que respondio la API; NULL si no hubo datos';
COMMENT ON COLUMN raw.fetch_log.http_status   IS 'Codigo HTTP de la respuesta';
COMMENT ON COLUMN raw.fetch_log.rows_received IS 'Filas recibidas en la respuesta';
COMMENT ON COLUMN raw.fetch_log.status        IS 'ok | exhausted (API sin batches nuevos) | error';
COMMENT ON COLUMN raw.fetch_log.detail        IS 'Detalle del error o respuesta de la API cuando no es ok';

-- processed.run_stats
COMMENT ON COLUMN processed.run_stats.rows_raw       IS 'Filas crudas del run';
COMMENT ON COLUMN processed.run_stats.rows_invalid   IS 'Filas enviadas a cuarentena (processed.rejected)';
COMMENT ON COLUMN processed.run_stats.rows_duplicate IS 'Filas validas que ya existian en processed';
COMMENT ON COLUMN processed.run_stats.rows_new       IS 'Filas validas insertadas por primera vez';

-- processed.covertype (metadatos propios de la etapa)
COMMENT ON COLUMN processed.covertype.row_hash           IS 'SHA-256 de los 13 valores normalizados; llave de deduplicacion';
COMMENT ON COLUMN processed.covertype.first_dag_run_id   IS 'Run en que la fila llego por primera vez';
COMMENT ON COLUMN processed.covertype.first_batch_number IS 'Batch en que la fila llego por primera vez';

-- Features y label: misma descripcion en processed y train
DO $$
DECLARE
    tabla TEXT;
BEGIN
    FOREACH tabla IN ARRAY ARRAY['processed.covertype', 'train.covertype'] LOOP
        EXECUTE format('COMMENT ON COLUMN %s.elevation IS %L', tabla,
            'Elevacion en metros');
        EXECUTE format('COMMENT ON COLUMN %s.aspect IS %L', tabla,
            'Orientacion en grados azimut (0-360)');
        EXECUTE format('COMMENT ON COLUMN %s.slope IS %L', tabla,
            'Pendiente en grados (0-90)');
        EXECUTE format('COMMENT ON COLUMN %s.horizontal_distance_to_hydrology IS %L', tabla,
            'Distancia horizontal a la fuente de agua mas cercana, metros (>= 0)');
        EXECUTE format('COMMENT ON COLUMN %s.vertical_distance_to_hydrology IS %L', tabla,
            'Distancia vertical a la fuente de agua mas cercana, metros (puede ser negativa)');
        EXECUTE format('COMMENT ON COLUMN %s.horizontal_distance_to_roadways IS %L', tabla,
            'Distancia horizontal a la via mas cercana, metros (>= 0)');
        EXECUTE format('COMMENT ON COLUMN %s.hillshade_9am IS %L', tabla,
            'Indice de sombreado a las 9am, solsticio de verano (0-255)');
        EXECUTE format('COMMENT ON COLUMN %s.hillshade_noon IS %L', tabla,
            'Indice de sombreado al mediodia, solsticio de verano (0-255)');
        EXECUTE format('COMMENT ON COLUMN %s.hillshade_3pm IS %L', tabla,
            'Indice de sombreado a las 3pm, solsticio de verano (0-255)');
        EXECUTE format('COMMENT ON COLUMN %s.horizontal_distance_to_fire_points IS %L', tabla,
            'Distancia horizontal al punto de ignicion de incendios mas cercano, metros (>= 0)');
        EXECUTE format('COMMENT ON COLUMN %s.wilderness_area IS %L', tabla,
            'Area silvestre: Rawah | Neota | Commanche | Cache (antes 4 columnas one-hot)');
        EXECUTE format('COMMENT ON COLUMN %s.soil_type IS %L', tabla,
            'Tipo de suelo, codigo USFS ELU (40 codigos, antes 40 columnas one-hot)');
        EXECUTE format('COMMENT ON COLUMN %s.cover_type IS %L', tabla,
            'LABEL: tipo de cobertura forestal (7 clases; el rango 0-6 o 1-7 depende de la fuente)');
    END LOOP;
END $$;
