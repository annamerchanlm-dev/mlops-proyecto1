"""
Reconstruye data-api/data/covertype.csv (el archivo que lee la Data API local).

Replica las transformaciones del notebook de Google (mlops-on-gcp/datasets/covertype/wrangle/prepare.ipynb):
  - Wilderness_Area: 4 columnas one-hot  -> nombre corto (Rawah, Neota, Commanche, Cache)
  - Soil_Type:      40 columnas one-hot -> código USFS (C2702 ... C8776)
  - Cover_Type:     1..7 -> 0..6

Uso (desde la raíz del proyecto):
  docker run --rm -v "$PWD":/w:Z -w /w python:3.11-slim \
    bash -c "pip install -q pandas && python scripts/prepare_covertype.py"
"""
import gzip
import io
import os
import sys
import urllib.request
import zipfile

import pandas as pd

UCI_URL = "https://archive.ics.uci.edu/static/public/31/covertype.zip"
RAW_ZIP = "scripts/covertype_uci.zip"
OUT_CSV = "data-api/data/covertype.csv"

SOIL_CODES = [
    "C2702", "C2703", "C2704", "C2705", "C2706", "C2717", "C3501", "C3502",
    "C4201", "C4703", "C4704", "C4744", "C4758", "C5101", "C5151", "C6101",
    "C6102", "C6731", "C7101", "C7102", "C7103", "C7201", "C7202", "C7700",
    "C7701", "C7702", "C7709", "C7710", "C7745", "C7746", "C7755", "C7756",
    "C7757", "C7790", "C8703", "C8707", "C8708", "C8771", "C8772", "C8776",
]
WILDERNESS = ["Rawah", "Neota", "Commanche", "Cache"]

COLUMN_NAMES = [
    "Elevation", "Aspect", "Slope",
    "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points",
    "Wilderness_Area", "Soil_Type", "Cover_Type",
]


def load_raw() -> pd.DataFrame:
    if not os.path.exists(RAW_ZIP):
        print(f"Descargando {UCI_URL} ...")
        urllib.request.urlretrieve(UCI_URL, RAW_ZIP)
    with zipfile.ZipFile(RAW_ZIP) as z:
        name = next(n for n in z.namelist() if n.endswith(".data.gz"))
        with gzip.open(io.BytesIO(z.read(name))) as f:
            return pd.read_csv(f, header=None)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    assert df.shape[1] == 55, f"Se esperaban 55 columnas, llegaron {df.shape[1]}"
    wilderness_oh = df.iloc[:, 10:14].to_numpy()
    soil_oh = df.iloc[:, 14:54].to_numpy()
    assert (wilderness_oh.sum(axis=1) == 1).all(), "Wilderness one-hot inválido"
    assert (soil_oh.sum(axis=1) == 1).all(), "Soil one-hot inválido"

    out = df.iloc[:, 0:10].copy()
    out["Wilderness_Area"] = [WILDERNESS[i] for i in wilderness_oh.argmax(axis=1)]
    out["Soil_Type"] = [SOIL_CODES[i] for i in soil_oh.argmax(axis=1)]
    out["Cover_Type"] = df.iloc[:, 54] - 1
    out.columns = COLUMN_NAMES
    return out


def main() -> int:
    df = transform(load_raw())
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    df.to_csv(OUT_CSV, header=True, index=False)
    print(f"OK -> {OUT_CSV}  shape={df.shape}")
    print(df.head(3).to_string())
    print("Cover_Type:", sorted(df["Cover_Type"].unique()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
