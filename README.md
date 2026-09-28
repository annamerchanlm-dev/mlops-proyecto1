# Proyecto 1 · MLOps: orquestación, entrenamiento y modelos

Operaciones de Machine Learning, Pontificia Universidad Javeriana (2026-2)
Profesor: Cristian Javier Díaz Álvarez
Grupo 2: Ana María Merchán León Y Daniel Niño

Pipeline de MLOps desplegado con Docker Compose en la VM del curso. Airflow consume la API de datos del profesor una vez por ejecución, guarda la información en PostgreSQL en tres etapas (crudo, procesado y listo para entrenar), un notebook de JupyterLab entrena y publica los modelos en MinIO, y una API en FastAPI sirve el modelo que esté marcado como producción.

El problema de fondo es el dataset Covertype (UCI): predecir el tipo de cobertura forestal de una celda de 30 x 30 m del Bosque Nacional Roosevelt (Colorado) a partir de variables cartográficas.

---

## Contenido

1. [Arquitectura](#arquitectura)
2. [Servicios y puertos](#servicios-y-puertos)
3. [Despliegue](#despliegue)
4. [Acceso a las interfaces](#acceso-a-las-interfaces)
5. [Flujo de datos](#flujo-de-datos)
6. [Entrenamiento y registro de modelos](#entrenamiento-y-registro-de-modelos)
7. [API de inferencia](#api-de-inferencia)
8. [Volúmenes y persistencia](#volúmenes-y-persistencia)
9. [Decisiones de diseño](#decisiones-de-diseño)
10. [Hallazgos sobre la API de datos](#hallazgos-sobre-la-api-de-datos)
11. [Problemas encontrados en la VM](#problemas-encontrados-en-la-vm)
12. [Resultados de la corrida oficial](#resultados-de-la-corrida-oficial)
13. [Estructura del repositorio](#estructura-del-repositorio)

---

## Arquitectura

```
                    ┌──────────────────── docker compose · VM 10.43.97.102 ────────────────────┐
                    │                                                                          │
 API de datos ──────┼─► Airflow (DAG covertype_ingest: 1 petición por ejecución)               │
 10.43.97.110:8080  │        │                                                                 │
                    │        ▼                                                                 │
                    │   postgres-data (base covertype)                                         │
                    │     raw ──► processed ──► train          postgres-airflow (metadatos)    │
                    │                             │                                            │
                    │                             ▼                                            │
                    │                  JupyterLab (02_train.ipynb)                             │
                    │                             │ modelo .joblib + metadata + snapshot       │
                    │                             ▼                                            │
                    │                 MinIO: buckets models / datasets                         │
                    │                             │ (usuario de solo lectura)                  │
                    │                             ▼                                            │
                    │                  inference-api (FastAPI) ◄── POST /predict               │
                    └──────────────────────────────────────────────────────────────────────────┘
```

Cada servicio hace una sola cosa: Airflow recolecta y prepara datos, Jupyter entrena, MinIO guarda artefactos y la API predice. Los datos tabulares viven en PostgreSQL y los binarios (modelos y snapshots) en MinIO.

## Servicios y puertos

| Servicio | Contenedor | Imagen | Puerto VM | Función |
|---|---|---|---|---|
| postgres-airflow | `p1_postgres_airflow` | postgres:16-alpine | interno | Metadatos de Airflow |
| postgres-data | `p1_postgres_data` | postgres:16-alpine | interno | Base `covertype`: esquemas `raw`, `processed`, `train` |
| adminer | `p1_adminer` | adminer | 8082 | Consulta de las tablas desde el navegador |
| minio | `p1_minio` | pgsty/minio (versión fija) | 9000 API / 9001 consola | Almacenamiento de modelos y snapshots |
| minio-init | `p1_minio_init` | pgsty/minio | — | Crea buckets, políticas y usuarios; termina |
| airflow-init | `p1_airflow_init` | propia (Airflow 2.10.5) | — | Migra la base de metadatos y crea el admin; termina |
| airflow-webserver | `p1_airflow_web` | propia | 8080 | Interfaz de Airflow |
| airflow-scheduler | `p1_airflow_scheduler` | propia | — | Ejecuta los DAGs (LocalExecutor) |
| jupyter | `p1_jupyter` | propia (uv) | 8888 | EDA y entrenamiento |
| inference-api | `p1_inference_api` | propia (uv) | 8000 | Predicción; Swagger en `/docs` |
| data-api | `p1_data_api` | código del profesor | 8081 | Copia local de la API de datos. Solo con `--profile dev` |

PostgreSQL no publica puertos: solo es accesible desde la red interna de Compose.

## Despliegue

Requisitos: Docker con Compose v2, unos 6 GB de disco libre en la partición de Docker y acceso a Docker Hub y PyPI.

```bash
git clone https://github.com/annamerchanlm-dev/mlops-proyecto1.git
cd mlops-proyecto1

cp .env.example .env
# Completar las contraseñas (openssl rand -hex 12) y AIRFLOW_UID:
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env

docker compose up -d --build
./scripts/verificar_despliegue.sh
```

El script revisa 24 puntos: contenedores, esquemas y tablas, diccionario de datos, permisos del usuario de solo lectura en Postgres, buckets y permisos en MinIO, DAGs sin errores de importación, interfaces y conectividad con la API de datos. Termina con `Resultado: 24 OK, 0 con falla` si todo está bien.

Todo lo necesario se crea al arrancar, sin pasos manuales:

- `db/init/01_schema.sql`, `02_roles.sh` y `03_rejected_and_dictionary.sql` crean las tablas, el rol de solo lectura, la cuarentena y el diccionario de datos.
- `minio-init` crea los buckets `models` y `datasets`, las políticas por bucket y los usuarios `trainer` e `inference`.
- `airflow-init` migra la base de metadatos y crea el usuario `admin`.

### Modo desarrollo y modo entrega

La diferencia está solo en el `.env`:

| Variable | Desarrollo | Entrega |
|---|---|---|
| `DATA_API_URL` | `http://data-api:80` | `http://10.43.97.110:8080` |
| `DAG_SCHEDULE_SECONDS` | `40` | `300` |
| `MIN_UPDATE_TIME` (API local) | `30` | no aplica |

En desarrollo se levanta además la copia local de la API, que necesita el CSV del dataset (no se versiona por tamaño):

```bash
docker run --rm -v "$PWD":/w:Z -w /w python:3.11-slim \
  bash -c "pip install -q pandas && python scripts/prepare_covertype.py"
docker compose --profile dev up -d --build
```

`prepare_covertype.py` descarga el dataset original de UCI y aplica la misma transformación del notebook de preparación del curso (one-hot a categórico, etiqueta 0–6).

## Acceso a las interfaces

Los puertos se publican en todas las interfaces de la VM, así que desde la red de la universidad se abren directamente:

| Interfaz | URL | Credenciales |
|---|---|---|
| Airflow | http://10.43.97.102:8080 | `admin` / `AIRFLOW_ADMIN_PASSWORD` |
| JupyterLab | http://10.43.97.102:8888 | `JUPYTER_TOKEN` |
| Consola MinIO | http://10.43.97.102:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| API de inferencia (Swagger) | http://10.43.97.102:8000/docs | — |
| Adminer | http://10.43.97.102:8082 | servidor `postgres-data`, usuario `covertype` |

Si la red desde la que se accede solo deja pasar SSH, las mismas interfaces quedan en `http://localhost:<puerto>` con un túnel:

```bash
ssh -L 8080:localhost:8080 -L 8888:localhost:8888 -L 9001:localhost:9001 \
    -L 8000:localhost:8000 -L 8082:localhost:8082 estudiante@10.43.97.102
```

La imagen de MinIO que se usa es un fork comunitario; su consola se presenta con el nombre "SILO", pero es el mismo servidor y la misma API S3.

## Flujo de datos

### DAG `covertype_ingest`

```
fetch_and_store_raw >> preprocess >> build_train_table >> report
```

| Tarea | Qué hace |
|---|---|
| `fetch_and_store_raw` | Hace **una** petición `GET /data?group_number=2`. Guarda el registro de la petición en `raw.fetch_log` y las filas tal cual (texto) en `raw.covertype`, en una sola transacción. `retries=0`: un reintento sería una segunda petición dentro de la misma ejecución |
| `preprocess` | Toma las filas de esa ejecución, las convierte a enteros, valida rangos y dominios, calcula un hash por fila y las inserta en `processed.covertype` con `ON CONFLICT DO NOTHING`. Las filas inválidas van a `processed.rejected` con el motivo |
| `build_train_table` | Reconstruye `train.covertype` con todo lo acumulado en `processed` |
| `report` | Imprime en el log el grupo, el batch, las filas nuevas, duplicadas e inválidas y la distribución de clases |

Configuración: ejecución cada `DAG_SCHEDULE_SECONDS`, `catchup=False`, `max_active_runs=1`. Los datos pasan entre tareas por PostgreSQL, no por XCom.

Cuando la API responde `400 "Ya se recolectó toda la información minima necesaria"`, la tarea registra `status='exhausted'`, escribe `RECOLECCIÓN COMPLETA` en el log, **pausa el DAG** y termina en estado *skipped*. Así el DAG no sigue generando ejecuciones vacías.

Efecto secundario conocido: como el DAG queda pausado antes de que el scheduler evalúe `report`, esa última ejecución se queda en *running* (el scheduler no revisa ejecuciones de DAGs pausados). En la corrida oficial se cerró a mano desde la interfaz con *Mark state as → success*, lo que también dejó en verde las tareas que habían quedado *skipped*. El registro que cuenta es `raw.fetch_log`, donde esa ejecución aparece con HTTP 400 y `status='exhausted'`. Una versión siguiente del DAG debería pausar desde una tarea final con `trigger_rule="all_done"`.

Cada tarea es idempotente respecto a su ejecución: antes de escribir, borra lo que esa misma ejecución hubiera escrito. `raw.covertype` y `processed.run_stats` dependen de `raw.fetch_log` con `ON DELETE CASCADE`.

### Base `covertype`

| Esquema | Tabla | Contenido |
|---|---|---|
| raw | `fetch_log` | Una fila por ejecución: `dag_run_id`, hora, grupo, batch, código HTTP, filas recibidas, estado |
| raw | `covertype` | Filas tal como llegan de la API (13 columnas `TEXT`), con `dag_run_id` y `batch_number` |
| processed | `covertype` | Filas tipadas, validadas y deduplicadas. PK `row_hash`; `CHECK` de rangos |
| processed | `run_stats` | Por ejecución: filas crudas, inválidas, duplicadas y nuevas |
| processed | `rejected` | Cuarentena: fila original (JSONB) y motivo del rechazo |
| train | `covertype` | 12 variables y la etiqueta; se reconstruye en cada ejecución |

Las columnas tienen `COMMENT` con descripción, unidad y rango (visibles en Adminer o con `\d+`).

La tabla `processed.rejected` se agregó después de tener datos. Se aplicó como migración (`03_rejected_and_dictionary.sql`), idempotente: en una base existente se ejecuta a mano con `psql`, y en un volumen nuevo la ejecuta el entrypoint de Postgres junto con los otros scripts de `db/init/`.

## Entrenamiento y registro de modelos

Jupyter lee PostgreSQL con el usuario `covertype_reader` (solo lectura) y escribe en MinIO con el usuario `trainer`.

`notebooks/01_eda.ipynb`: trazabilidad de las peticiones, filas nuevas y duplicadas por ejecución, cuarentena, distribución de clases, variables categóricas y numéricas, y separación de clases por variable.

`notebooks/02_train.ipynb`:

1. **Huella del dataset.** Se recalcula el hash de cada fila con la misma regla del DAG y la huella del conjunto (hash de los `row_hash` ordenados). El dataset se guarda en `datasets/covertype/<huella>/train.parquet` con un `manifest.json`; si la huella ya existe no se vuelve a subir.
2. **Split 60/20/20 determinístico por hash** (`int(row_hash[:8], 16) % 100`). Una fila cae siempre en el mismo conjunto aunque el dataset crezca, y como los datos ya están deduplicados no hay filas repetidas entre train y test. Se verifica que las proporciones de clase se mantengan en cada conjunto.
3. **Pipeline.** Imputación y escalado de las numéricas; imputación y `OneHotEncoder(handle_unknown="ignore")` de las categóricas. El preprocesamiento va dentro del artefacto, así la API recibe los datos en el mismo formato crudo que entrega la API de datos.
4. **Modelos.** `DummyClassifier` como referencia, regresión logística, Random Forest y `HistGradientBoostingClassifier`.
5. **Ajuste de hiperparámetros.** `RandomizedSearchCV` con `StratifiedKFold(5)` sobre train, métrica F1-macro (las clases están muy desbalanceadas). La configuración final se elige con el F1 de validación cruzada penalizado por el exceso de brecha train-CV sobre 0.10, para no premiar configuraciones que memorizan la clase minoritaria. `class_weight` es un hiperparámetro más.
6. **Diagnóstico** en validación:

   | Diagnóstico | Regla |
   |---|---|
   | underfitting | F1 val < F1 Dummy + 0.10 |
   | underfitting (alto sesgo) | F1 train más de 0.20 por debajo del mejor F1 val |
   | overfitting | brecha train-val > 0.10 |
   | ignora clase minoritaria | recall de la clase minoritaria = 0 |
   | ok (gap moderado) / ok | brecha entre 0.05 y 0.10 / hasta 0.05 |

7. **Curvas** de aprendizaje y de validación (profundidad del Random Forest).
8. **Evaluación final.** Los modelos se reajustan con train + validación y se evalúan una sola vez en test, con intervalo de confianza del 95 % por bootstrap del F1-macro.
9. **Importancia por permutación** sobre validación y análisis de errores de la clase minoritaria.
10. **Publicación y promoción.** Cada modelo se publica en `models/covertype/<algoritmo>_<fecha>/` con `model.joblib` y `metadata.json`, y se registra en `registry.json`. Publicar no cambia el modelo en producción: la promoción es un paso aparte que exige ser el mejor elegible en validación, superar al Dummy por 0.10 de F1-macro, brecha ≤ 0.10 y recall de la clase minoritaria > 0. Cada promoción queda en el historial del registro con su motivo.

El `metadata.json` incluye métricas de CV, validación y test, hiperparámetros, clases y categorías vistas, huella y ruta del snapshot, regla del split, tamaño del artefacto, latencia por fila, versiones del entorno y limitaciones de uso.

**Contrato de versiones.** El `.joblib` se crea en Jupyter y se carga en la API, así que ambas imágenes usan exactamente las mismas versiones (Python 3.12, scikit-learn 1.9.1, numpy 2.4.6, pandas 3.0.6, joblib 1.6.0), fijadas en `pyproject.toml` y `uv.lock`.

## API de inferencia

Lee MinIO con el usuario `inference`, que solo tiene permiso de lectura sobre `models`.

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/health` | Estado de la API, de MinIO y versión en producción |
| GET | `/models` | Versiones publicadas con sus métricas e historial de promociones |
| GET | `/models/{version}` | `metadata.json` de una versión |
| POST | `/predict` | Predicción con el modelo en producción |
| POST | `/predict/{version}` | Predicción con una versión específica |
| POST | `/compare` | La misma entrada contra varias versiones |

```bash
curl -X POST http://localhost:8000/predict -H "Content-Type: application/json" -d '{
  "elevation": 3070, "aspect": 267, "slope": 9,
  "horizontal_distance_to_hydrology": 85, "vertical_distance_to_hydrology": 9,
  "horizontal_distance_to_roadways": 5580,
  "hillshade_9am": 198, "hillshade_noon": 244, "hillshade_3pm": 185,
  "horizontal_distance_to_fire_points": 2177,
  "wilderness_area": "Rawah", "soil_type": "C7202"}'
```

```json
{
  "cover_type": 1,
  "cover_type_nombre": "Lodgepole Pine",
  "probabilidades": {"0": 0.0362, "1": 0.9638, "4": 0},
  "modelo_usado": "rf_20260927-205139",
  "es_produccion": true,
  "advertencias": []
}
```

Comportamiento:

- La entrada se valida con las mismas reglas que el DAG. Un valor fuera de rango, un código de suelo inexistente o un campo extra devuelven 422.
- Si un valor es válido pero el modelo no lo vio en entrenamiento (otra zona, otro suelo), predice igual y lo informa en `advertencias`.
- Las columnas se reordenan según el `metadata.json` antes de predecir, así que el orden de los campos en el JSON no importa.
- `registry.json` se relee cada 5 segundos: si se promueve otro modelo, la API lo usa sin reiniciarse. Cada versión se descarga una vez y queda en memoria.
- Si no hay modelo en producción responde 503; la API no se cae.

## Volúmenes y persistencia

La regla es simple: el estado vive en volúmenes con nombre y el código en el repositorio. Ningún contenedor guarda nada importante en su propia capa, así que cualquiera se puede borrar y recrear sin perder datos.

### Volúmenes con nombre (estado)

| Volumen | Servicio | Ruta en el contenedor | Qué guarda |
|---|---|---|---|
| `pg_airflow` | postgres-airflow | `/var/lib/postgresql/data` | Metadatos de Airflow: DAG runs, estados de tareas, usuarios, conexiones |
| `pg_data` | postgres-data | `/var/lib/postgresql/data` | Base `covertype`: esquemas `raw`, `processed`, `train` |
| `minio_data` | minio | `/data` | Buckets `models` y `datasets`: artefactos, metadata, registro y snapshots |

Docker les antepone el nombre del proyecto (`mlops-proyecto1_pg_data`, etc.). Se usan volúmenes con nombre y no carpetas del host porque los administra Docker (permisos, SELinux, ubicación en `/var/lib/docker`) y porque así los datos no terminan por error dentro del repositorio.

### Montajes del host (código y configuración)

| Origen | Destino | Modo | Motivo |
|---|---|---|---|
| `./db/init` | postgres-data `/docker-entrypoint-initdb.d` | `ro,Z` | Migraciones; solo se ejecutan cuando el volumen está vacío |
| `./minio/init.sh`, `./minio/policies` | minio-init | `ro,Z` | Buckets, políticas y usuarios |
| `./airflow/dags` | webserver y scheduler | `Z` | Editar un DAG no requiere reconstruir la imagen |
| `./airflow/logs` | webserver y scheduler | `Z` | Logs de las tareas visibles desde el host (ignorados por Git) |
| `./notebooks` | jupyter | `Z` | Los notebooks editados en JupyterLab quedan en el repositorio |
| `./data-api/data` | data-api (solo `dev`) | `Z` | CSV del dataset para la copia local de la API |

- `ro`: el contenedor no puede modificar lo que solo necesita leer.
- `Z`: la VM usa Rocky Linux con SELinux en modo *enforcing*. Sin esta opción el contenedor recibe *permission denied* al leer la carpeta montada; `Z` le asigna la etiqueta SELinux correcta.
- `inference-api` y `airflow-init` no montan nada: la API es *stateless* (lee el modelo de MinIO y lo guarda en memoria) y `airflow-init` solo escribe en la base de metadatos.

### Qué sobrevive a cada operación

| Operación | Contenedores | Volúmenes (datos y modelos) |
|---|---|---|
| `docker compose restart` / reinicio de la VM | se reinician | se conservan |
| `docker compose up -d --build` | se recrean los que cambiaron | se conservan |
| `docker compose down` | se eliminan | se conservan |
| `docker compose down -v` | se eliminan | **se borran** |

`down -v` se usó una sola vez y a propósito: para pasar del modo desarrollo al de entrega con la base y MinIO vacíos, de modo que los resultados oficiales no se mezclen con los de prueba. Como las migraciones de `db/init/` y `minio-init` recrean todo al arrancar, el sistema se reconstruye solo sobre volúmenes nuevos.

### Inspección y respaldo

```bash
docker volume ls --filter name=mlops-proyecto1       # volúmenes del proyecto
docker system df -v | grep mlops-proyecto1           # tamaño de cada uno

mkdir -p backups                                      # ignorado por Git
# Base de datos (formato custom, se restaura con pg_restore)
docker compose exec -T postgres-data pg_dump -U covertype -d covertype -Fc > backups/covertype_$(date +%F).dump
# Modelos y snapshots
docker run --rm --network mlops-proyecto1_default --env-file .env -v "$PWD/backups":/backups:Z \
  --entrypoint sh pgsty/minio:RELEASE.2026-08-04T00-00-00Z -c \
  'mc alias set l http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc mirror l/models /backups/models && mc mirror l/datasets /backups/datasets'
```

Los respaldos se hacen con las herramientas de cada servicio (`pg_dump`, `mc mirror`) y no copiando la carpeta interna del volumen, porque una copia en caliente de los archivos de PostgreSQL puede quedar inconsistente.

## Decisiones de diseño

| Decisión | Motivo |
|---|---|
| Dos PostgreSQL (metadatos y datos) | Los datos del negocio no se mezclan con el estado interno de Airflow |
| Tres esquemas en una base de datos | Etapas separadas con transacciones y llaves foráneas entre ellas |
| `row_hash` como llave de `processed` | La API entrega filas repetidas entre peticiones; la base las descarta sola |
| Validación en el DAG y `CHECK` en la base | Si el código falla, la base rechaza el dato en lugar de guardarlo |
| Cuarentena en lugar de descartar | Ninguna fila se pierde sin dejar rastro del motivo |
| Pausa automática al agotar la API | Evita ejecuciones vacías cada 5 minutos |
| Imagen propia de Airflow con constraints versionadas | Arranque inmediato y build reproducible, sin depender de GitHub |
| LocalExecutor | Tres contenedores de Airflow en lugar de seis; la VM se comparte con otro stack |
| Usuarios con permisos mínimos | Jupyter no puede modificar la base; la API no puede modificar modelos; `trainer` solo opera en sus buckets |
| Snapshot del dataset por huella | Cada modelo apunta a los datos exactos con que se entrenó |
| Split por hash | Métricas comparables entre reentrenamientos |
| Entrenar y promover por separado | Publicar un modelo no lo pone en producción; la promoción sigue reglas explícitas |
| Volúmenes con nombre para el estado, montajes del host solo para código | Los contenedores son desechables; los datos no dependen de ellos ni entran al repositorio |
| Imágenes con versión fija | El despliegue no cambia porque una etiqueta `latest` se actualice o desaparezca |

## Hallazgos sobre la API de datos

**Todas las peticiones muestrean la misma décima del dataset.** En `main.py` de la API, `get_batch_data(group_number)` recibe el número de grupo en lugar del número de batch. El `batch_number` de la respuesta avanza, pero las filas salen siempre del mismo tramo de 58 101 filas (el tramo 2 para el grupo 2). Cada petición toma 5 810 filas al azar de ese tramo, así que con `k` peticiones las filas únicas esperadas son `N · (1 − (1 − m/N)^k)`:

| Corrida | Peticiones | Únicas esperadas | Únicas observadas |
|---|---|---|---|
| Prueba con la API local (12 peticiones) | 12 | 41 693 | 41 724 (+0.07 %) |
| Corrida oficial (API del profesor) | 16 | 47 334 | 47 356 (+0.05 %) |

La proporción de duplicados por ejecución también sigue la predicción (10 % en la segunda petición, 19.5 % en la tercera, etc.). Por eso la deduplicación en `processed` es necesaria y no un detalle.

**El tramo del grupo 2 es una sola zona.** Con el dataset original ordenado como lo publica UCI, el tramo 2 corresponde solo al área Rawah, con 15 de los 40 tipos de suelo y tres clases: Spruce/Fir (≈28 %), Lodgepole Pine (≈71 %) y Aspen (≈0.6 %). Por eso la métrica de decisión es F1-macro y no accuracy: un modelo que siempre predice Lodgepole Pine ya tiene 71 % de accuracy. El pipeline no asume clases ni zonas fijas; las descubre de los datos.

**Ventana de 5 minutos.** El batch solo avanza si pasaron más de 300 s desde el último avance. Con el DAG cada 300 s, algunas ejecuciones caen justo por debajo y repiten batch. Son peticiones válidas: quedan en `fetch_log` y sus filas se deduplican.

**Primer batch de la corrida oficial.** La primera ejecución oficial recibió el batch 2: el contador del grupo 2 ya había sido iniciado por una petición anterior, externa a este sistema (antes de la corrida solo se consultó `GET /`, que no mueve el contador). Se recolectaron los batches 2 a 11, es decir, 10 batches distintos. Como todos provienen del mismo tramo, el batch 1 no habría aportado datos diferentes.

## Problemas encontrados en la VM

| Problema | Solución |
|---|---|
| Las imágenes oficiales `minio/minio` y `minio/mc` ya no están disponibles en Docker Hub | Se usa `pgsty/minio`, fork mantenido, con versión fija. La misma imagen trae `mc`, así que `minio-init` no necesita otra |
| `raw.githubusercontent.com` está bloqueado desde la VM | El archivo de constraints de Airflow se descargó por jsDelivr y se versiona en `airflow/`; `uv` se instala desde PyPI |
| La partición `/var` (15 GB), donde Docker guarda imágenes y volúmenes, se llenó | El disco virtual tenía unos 90 GB sin asignar. Se creó una partición, se agregó al grupo LVM y se amplió `/var` a 45 GB en caliente (`lvextend -r`) |
| Desde algunas redes externas solo pasa SSH hacia la VM | Túnel SSH o reenvío de puertos de VS Code; desde la red de la universidad los puertos se abren directamente |
| Airflow tardaba hasta 5 minutos en ver un DAG nuevo | `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30` |

## Resultados de la corrida oficial

### Recolección

| Métrica | Valor |
|---|---|
| Inicio / fin | 2026-09-27 19:12 / 20:30 (hora de Bogotá) |
| Ejecuciones del DAG (ok / exhausted / error) | 16 / 1 / 0 |
| Peticiones a la API | 17, una por ejecución |
| Batches distintos | 10 (del 2 al 11) |
| Filas recibidas / únicas / duplicadas / en cuarentena | 92 960 / 47 356 / 45 604 (49.1 %) / 0 |
| Distribución de clases | Spruce/Fir 27.9 %, Lodgepole Pine 71.5 %, Aspen 0.63 % (297 filas) |
| Huella del dataset de entrenamiento | `a6edf31e48e73aaf` |

Split por hash: 28 453 filas en train, 9 395 en validación y 9 508 en test. La proporción de cada clase no varía más de 0.4 puntos entre conjuntos; Aspen tiene 190, 47 y 60 filas respectivamente.

### Modelos

| Modelo | F1-macro CV | F1-macro val | F1-macro test (IC 95 %) | Brecha train-val | Balanced acc. test | Recall Aspen test | Diagnóstico |
|---|---|---|---|---|---|---|---|
| Dummy | — | 0.278 | 0.279 | — | — | 0.00 | referencia |
| Regresión logística | 0.436 | 0.430 | 0.431 (0.422–0.440) | 0.008 | 0.733 | 1.00 | underfitting (alto sesgo) |
| **Random Forest** | 0.874 | 0.848 | **0.891 (0.864–0.915)** | 0.088 | **0.949** | **0.95** | ok (gap moderado) |
| HistGradientBoosting | 0.923 | 0.888 | 0.920 (0.893–0.943) | 0.106 | 0.895 | 0.77 | overfitting |

Modelo en producción: **`rf_20260927-205139`** (Random Forest, 8.9 MB, ~0.06 ms por fila).

Por qué Random Forest y no HistGradientBoosting, que tiene mejor F1-macro en validación y en test:

- HistGradientBoosting quedó con una brecha train-val de 0.106, por encima del límite de 0.10 fijado antes de entrenar, y por eso no era elegible. Promoverlo después de ver el test sería usar el test para seleccionar, y su métrica dejaría de ser una estimación honesta.
- Random Forest es mejor en lo que más pesa en un problema tan desbalanceado: balanced accuracy de 0.949 frente a 0.895 y recall de Aspen de 0.95 frente a 0.77. HistGradientBoosting gana en F1-macro porque es más preciso con Aspen (menos falsos positivos), no porque encuentre más.
- Los intervalos de confianza del F1-macro en test se solapan, así que esa diferencia no es concluyente con 60 ejemplos de Aspen en test.

Las tres versiones quedan publicadas en MinIO con su `metadata.json`. `POST /compare` permite ver cómo responde cada una a la misma entrada, y promover otra versión solo requiere actualizar `registry.json`.

La regresión logística encuentra todos los Aspen de test, pero con una precisión de 0.03 para esa clase: predice Aspen para una gran parte de las filas. Es un caso de alto sesgo, no un buen modelo para la clase minoritaria.

Comparado con la prueba de desarrollo (41 724 filas), el ranking de F1 es el mismo. Allí HistGradientBoosting quedó dentro del límite de brecha y fue el promovido; con los datos oficiales quedó por encima por 0.006. La regla se mantuvo igual en ambas corridas.

### Evidencias

| Evidencia | Archivo |
|---|---|
| Ejecuciones del DAG en Airflow | `docs/img/airflow_grid.png` |
| Registro de peticiones (`raw.fetch_log`) | `docs/img/fetch_log.png` y `docs/evidencia/fetch_log.txt` |
| Modelos en MinIO | `docs/img/minio_models.png` |
| Snapshot del dataset en MinIO | `docs/img/minio_datasets.png` |
| Predicción desde Swagger | `docs/img/swagger_predict.png` |
| Verificación del despliegue | `docs/evidencia/verificacion.txt` |

## Estructura del repositorio

```
mlops-proyecto1/
├── docker-compose.yml
├── .env.example
├── data-api/                  API de datos del profesor (solo modo dev)
├── db/init/                   esquema, roles y migración 03
├── minio/                     init.sh y políticas por bucket
├── airflow/
│   ├── Dockerfile, requirements.txt, constraints-2.10.5-py3.12.txt
│   └── dags/                  covertype_ingest.py, p1_smoke_test.py
├── jupyter/                   Dockerfile, pyproject.toml, uv.lock
├── notebooks/                 01_eda.ipynb, 02_train.ipynb
├── inference-api/             Dockerfile, pyproject.toml, uv.lock, app/main.py
├── scripts/                   prepare_covertype.py, probe_api.py, verificar_despliegue.sh
└── docs/                      img/ (capturas) y evidencia/ (fetch_log.txt)
```

No se versionan: `.env` (credenciales), el CSV del dataset ni los logs de Airflow. Los modelos y snapshots viven en MinIO.
