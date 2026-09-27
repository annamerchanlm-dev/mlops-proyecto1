"""
API de inferencia - Proyecto 1 MLOps (Covertype, grupo 2)

Sirve los modelos publicados por notebooks/02_train.ipynb en MinIO:

    models/covertype/registry.json            <- catálogo de versiones + alias "production"
    models/covertype/<version>/model.joblib   <- Pipeline completo (preprocesamiento + modelo)
    models/covertype/<version>/metadata.json  <- ficha del modelo

Diseño:
  - Credenciales de SOLO LECTURA (usuario MinIO "inference"): la API consume modelos, no los modifica.
  - registry.json se relee con un TTL corto: si el notebook promueve otro modelo, la API lo usa sin reiniciarse.
  - Cada versión se descarga una sola vez y queda en caché en memoria.
  - Sin modelo en producción -> 503 (la API no se cae).
  - Entrada validada con las mismas reglas del DAG -> 422 si algo está fuera de rango o de dominio.
  - Categorías válidas pero no vistas en entrenamiento -> predice y lo informa en "advertencias".
"""
import io
import json
import os
import threading
import time
from typing import Any, Literal

import boto3
import joblib
import pandas as pd
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
MODELS_BUCKET = os.environ.get("MODELS_BUCKET", "models")
MODEL_PREFIX = os.environ.get("MODEL_PREFIX", "covertype")
REGISTRY_KEY = f"{MODEL_PREFIX}/registry.json"
REGISTRY_TTL_SECONDS = float(os.environ.get("REGISTRY_TTL_SECONDS", "5"))

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
    region_name="us-east-1",
    config=Config(signature_version="s3v4", connect_timeout=5, read_timeout=30,
                  retries={"max_attempts": 2}),
)

# Dominios: los mismos que valida el DAG (preprocess)
WildernessArea = Literal["Rawah", "Neota", "Commanche", "Cache"]
SoilType = Literal[
    "C2702", "C2703", "C2704", "C2705", "C2706", "C2717", "C3501", "C3502",
    "C4201", "C4703", "C4704", "C4744", "C4758", "C5101", "C5151", "C6101",
    "C6102", "C6731", "C7101", "C7102", "C7103", "C7201", "C7202", "C7700",
    "C7701", "C7702", "C7709", "C7710", "C7745", "C7746", "C7755", "C7756",
    "C7757", "C7790", "C8703", "C8707", "C8708", "C8771", "C8772", "C8776",
]

EJEMPLO = {
    "elevation": 3100, "aspect": 51, "slope": 9,
    "horizontal_distance_to_hydrology": 258, "vertical_distance_to_hydrology": 30,
    "horizontal_distance_to_roadways": 1510,
    "hillshade_9am": 221, "hillshade_noon": 232, "hillshade_3pm": 148,
    "horizontal_distance_to_fire_points": 2279,
    "wilderness_area": "Rawah", "soil_type": "C7745",
}


# ---------------------------------------------------------------------------
# Esquemas
# ---------------------------------------------------------------------------
class Celda(BaseModel):
    """Una celda de 30x30 m descrita por variables cartográficas (formato crudo, como lo entrega la API de datos)."""

    model_config = ConfigDict(extra="forbid", json_schema_extra={"example": EJEMPLO})

    elevation: int = Field(..., description="Elevación, metros")
    aspect: int = Field(..., ge=0, le=360, description="Orientación, grados azimut (0-360)")
    slope: int = Field(..., ge=0, le=90, description="Pendiente, grados (0-90)")
    horizontal_distance_to_hydrology: int = Field(..., ge=0, description="Distancia horizontal al agua, m")
    vertical_distance_to_hydrology: int = Field(..., description="Distancia vertical al agua, m (puede ser negativa)")
    horizontal_distance_to_roadways: int = Field(..., ge=0, description="Distancia horizontal a vías, m")
    hillshade_9am: int = Field(..., ge=0, le=255, description="Índice de sombra 9am (0-255)")
    hillshade_noon: int = Field(..., ge=0, le=255, description="Índice de sombra mediodía (0-255)")
    hillshade_3pm: int = Field(..., ge=0, le=255, description="Índice de sombra 3pm (0-255)")
    horizontal_distance_to_fire_points: int = Field(..., ge=0, description="Distancia a puntos de ignición, m")
    wilderness_area: WildernessArea = Field(..., description="Área silvestre")
    soil_type: SoilType = Field(..., description="Tipo de suelo (código USFS)")


class Prediccion(BaseModel):
    cover_type: int = Field(..., description="Clase predicha")
    cover_type_nombre: str = Field(..., description="Especie dominante")
    probabilidades: dict[str, float] = Field(..., description="Probabilidad por clase conocida por el modelo")
    modelo_usado: str
    es_produccion: bool
    advertencias: list[str] = Field(default_factory=list)


class PeticionComparacion(BaseModel):
    model_config = ConfigDict(json_schema_extra={"example": {"celda": EJEMPLO, "versiones": None}})

    celda: Celda
    versiones: list[str] | None = Field(None, description="Versiones a comparar; si se omite, todas las del registro")


# ---------------------------------------------------------------------------
# Registro de modelos y caché
# ---------------------------------------------------------------------------
_lock = threading.Lock()
_registry: dict[str, Any] = {"data": None, "leido_en": 0.0}
_cache: dict[str, dict[str, Any]] = {}      # version -> {"modelo": Pipeline, "metadata": dict}


def leer_registry(forzar: bool = False) -> dict[str, Any]:
    """registry.json con un TTL corto: detecta promociones sin reiniciar la API."""
    with _lock:
        vigente = time.monotonic() - _registry["leido_en"] < REGISTRY_TTL_SECONDS
        if _registry["data"] is not None and vigente and not forzar:
            return _registry["data"]
    try:
        cuerpo = s3.get_object(Bucket=MODELS_BUCKET, Key=REGISTRY_KEY)["Body"].read()
        data = json.loads(cuerpo)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404", "NoSuchBucket"):
            data = {"production": None, "models": {}, "history": []}
        else:
            raise HTTPException(status_code=503, detail=f"No se pudo leer el registro de modelos: {exc}")
    except BotoCoreError as exc:
        raise HTTPException(status_code=503, detail=f"MinIO no disponible: {exc}")
    with _lock:
        _registry.update(data=data, leido_en=time.monotonic())
    return data


def resolver_version(version: str | None) -> tuple[str, bool]:
    reg = leer_registry()
    produccion = reg.get("production")
    if version is None:
        if not produccion:
            raise HTTPException(
                status_code=503,
                detail="No hay un modelo en producción. Ejecute notebooks/02_train.ipynb para entrenar y promover uno.",
            )
        return produccion, True
    if version not in reg.get("models", {}):
        raise HTTPException(
            status_code=404,
            detail=f"La versión '{version}' no existe. Disponibles: {sorted(reg.get('models', {}))}",
        )
    return version, version == produccion


def cargar(version: str) -> dict[str, Any]:
    """Descarga modelo + metadata una sola vez por versión."""
    with _lock:
        if version in _cache:
            return _cache[version]
    entrada = leer_registry()["models"][version]
    try:
        metadata = json.loads(s3.get_object(Bucket=MODELS_BUCKET, Key=entrada["metadata"])["Body"].read())
        binario = s3.get_object(Bucket=MODELS_BUCKET, Key=entrada["path"])["Body"].read()
    except (ClientError, BotoCoreError) as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo descargar la versión {version}: {exc}")
    modelo = joblib.load(io.BytesIO(binario))
    with _lock:
        _cache[version] = {"modelo": modelo, "metadata": metadata}
    return _cache[version]


def predecir(celda: Celda, version: str | None) -> Prediccion:
    version, es_prod = resolver_version(version)
    item = cargar(version)
    modelo, meta = item["modelo"], item["metadata"]

    fila = pd.DataFrame([celda.model_dump()])[meta["features"]["order"]]
    proba = modelo.predict_proba(fila)[0]
    clases = [int(c) for c in modelo.classes_]
    predicha = clases[int(proba.argmax())]

    advertencias = [
        f"{col}='{getattr(celda, col)}' no apareció en el entrenamiento de {version}: "
        "el modelo lo ignora y la predicción puede ser menos confiable"
        for col, vistas in meta.get("categories_seen", {}).items()
        if str(getattr(celda, col)) not in vistas
    ]
    nombres = meta.get("class_names", {})
    return Prediccion(
        cover_type=predicha,
        cover_type_nombre=nombres.get(str(predicha), f"clase {predicha}"),
        probabilidades={str(c): round(float(p), 4) for c, p in zip(clases, proba)},
        modelo_usado=version,
        es_produccion=es_prod,
        advertencias=advertencias,
    )


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Covertype - API de inferencia",
    description=(
        "Predice el **tipo de cobertura forestal** de una celda de 30x30 m a partir de variables cartográficas.\n\n"
        "Sirve los modelos entrenados en `notebooks/02_train.ipynb` y publicados en **MinIO** "
        "(`models/covertype/`). Por defecto usa la versión marcada como **production** en `registry.json`."
    ),
    version="1.0.0",
    openapi_tags=[
        {"name": "estado", "description": "Salud del servicio"},
        {"name": "modelos", "description": "Catálogo de versiones publicadas en MinIO"},
        {"name": "inferencia", "description": "Predicciones"},
    ],
)


@app.get("/", tags=["estado"], summary="Información del servicio")
def raiz():
    return {"servicio": "Covertype - API de inferencia", "docs": "/docs",
            "endpoints": ["/health", "/models", "/models/{version}", "/predict", "/predict/{version}", "/compare"]}


@app.get("/health", tags=["estado"], summary="Estado de la API, de MinIO y del modelo en producción")
def health():
    try:
        reg = leer_registry(forzar=True)
        minio = "ok"
    except HTTPException as exc:
        return {"status": "degraded", "minio": exc.detail, "production": None, "en_cache": sorted(_cache)}
    return {
        "status": "ok" if reg.get("production") else "sin_modelo",
        "minio": minio,
        "production": reg.get("production"),
        "versiones_publicadas": len(reg.get("models", {})),
        "en_cache": sorted(_cache),
    }


@app.get("/models", tags=["modelos"], summary="Versiones publicadas y cuál está en producción")
def listar_modelos():
    reg = leer_registry()
    modelos = [
        {"version": v, **info, "es_produccion": v == reg.get("production")}
        for v, info in sorted(reg.get("models", {}).items(), key=lambda kv: kv[1].get("created_at", ""), reverse=True)
    ]
    return {"production": reg.get("production"), "total": len(modelos), "modelos": modelos,
            "historial_promociones": reg.get("history", [])}


@app.get("/models/{version}", tags=["modelos"], summary="Ficha completa (metadata.json) de una versión")
def detalle_modelo(version: str = Path(..., examples=["hgb_20260927-181117"])):
    version, _ = resolver_version(version)
    return cargar(version)["metadata"]


@app.post("/predict", tags=["inferencia"], response_model=Prediccion,
          summary="Predice con el modelo en producción")
def predict(celda: Celda):
    return predecir(celda, None)


@app.post("/predict/{version}", tags=["inferencia"], response_model=Prediccion,
          summary="Predice con una versión específica")
def predict_version(celda: Celda, version: str = Path(..., examples=["rf_20260927-181116"])):
    return predecir(celda, version)


@app.post("/compare", tags=["inferencia"], summary="Misma celda contra varias versiones, lado a lado")
def compare(peticion: PeticionComparacion):
    versiones = peticion.versiones or sorted(leer_registry().get("models", {}))
    if not versiones:
        raise HTTPException(status_code=503, detail="No hay modelos publicados.")
    return {"entrada": peticion.celda.model_dump(),
            "resultados": [predecir(peticion.celda, v) for v in versiones]}
