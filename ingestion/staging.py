import json
import os
from pathlib import Path
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

RAW_DIR = Path("data/raw")
STAGING_DIR = Path("data/staging")
STAGING_FILE = STAGING_DIR / "trips.parquet"

# Campos a descartar (GeoJSON redundante)
DROP_FIELDS = ["pickup_centroid_location", "dropoff_centroid_location"]

# Mapeo de tipos
FLOAT_FIELDS = [
    "trip_miles",
    "pickup_centroid_latitude", "pickup_centroid_longitude",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude",
]
DECIMAL_FIELDS = ["fare", "tips", "tolls", "extras", "trip_total"]
INT_FIELDS = ["trip_seconds", "pickup_community_area", "dropoff_community_area"]
DATETIME_FIELDS = ["trip_start_timestamp", "trip_end_timestamp"]


def load_raw_pages() -> pd.DataFrame:
    pages = sorted(RAW_DIR.glob("page_*.json"))
    if not pages:
        raise FileNotFoundError(f"No se encontraron archivos en {RAW_DIR}")

    print(f"📂 Encontrados {len(pages)} archivos raw")
    frames = []
    for i, page in enumerate(pages, 1):
        with open(page) as f:
            data = json.load(f)
        frames.append(pd.DataFrame(data))
        print(f"  ✅ {page.name} → {len(data):,} registros", end="\r")

    print()
    df = pd.concat(frames, ignore_index=True)
    print(f"📦 Total raw: {len(df):,} registros")
    return df


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    # Descartar campos GeoJSON
    for field in DROP_FIELDS:
        if field in df.columns:
            df = df.drop(columns=[field])

    # Castear floats
    for col in FLOAT_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Castear decimales
    for col in DECIMAL_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Castear enteros (nullable)
    for col in INT_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Castear datetimes
    for col in DATETIME_FIELDS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    df["trip_date"] = df["trip_start_timestamp"].dt.date
    df["trip_hour"] = df["trip_start_timestamp"].dt.hour
    df["trip_weekday"] = df["trip_start_timestamp"].dt.dayofweek  # 0=Lunes, 6=Domingo

    # revenue_per_mile: evitar división por cero
    df["revenue_per_mile"] = df.apply(
        lambda r: round(r["trip_total"] / r["trip_miles"], 4)
        if pd.notna(r["trip_miles"]) and r["trip_miles"] > 0
        else None,
        axis=1,
    )

    # tip_rate: propina sobre tarifa base
    df["tip_rate"] = df.apply(
        lambda r: round(r["tips"] / r["fare"], 4)
        if pd.notna(r["fare"]) and r["fare"] > 0
        else None,
        axis=1,
    )

    # is_outlier: reglas heurísticas
    df["is_outlier"] = (
        (df["trip_seconds"] > 10_800) |          # más de 3 horas
        (df["trip_miles"] > 100) |               # más de 100 millas
        (df["fare"] < 0) |                       # tarifa negativa
        (df["trip_end_timestamp"] < df["trip_start_timestamp"])  # tiempo incoherente
    ).fillna(False).astype(int)

    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["trip_id"], keep="first")
    after = len(df)
    if before != after:
        print(f"⚠️  Duplicados eliminados: {before - after:,}")
    return df


def main():
    print("=" * 60)
    print("WindyCity Cabs — Staging")
    print("=" * 60)
    start = datetime.now()

    # 1. Cargar raw
    df = load_raw_pages()

    # 2. Castear tipos
    print("\n🔄 Casteando tipos...")
    df = cast_types(df)

    # 3. Campos derivados
    print("🔧 Calculando campos derivados...")
    df = add_derived_fields(df)

    # 4. Deduplicar
    print("🔍 Verificando duplicados...")
    df = deduplicate(df)

    # 5. Guardar Parquet
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(STAGING_FILE, index=False)

    elapsed = (datetime.now() - start).seconds
    print(f"\n✅ Staging completo")
    print(f"   Registros        : {len(df):,}")
    print(f"   Columnas         : {len(df.columns)}")
    print(f"   Outliers flagueados: {df['is_outlier'].sum():,}")
    print(f"   Archivo          : {STAGING_FILE}")
    print(f"   Tiempo           : {elapsed}s")
    print("=" * 60)

    # Preview de tipos resultantes
    print("\n📋 Schema final:")
    for col, dtype in df.dtypes.items():
        print(f"   {col:<45} {dtype}")


if __name__ == "__main__":
    main()