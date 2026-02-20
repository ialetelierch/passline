import os
from pathlib import Path
from datetime import datetime

import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

STAGING_FILE = Path("data/staging/trips.parquet")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME", "windycity"),
    "user": os.getenv("DB_USER", "wc_user"),
    "password": os.getenv("DB_PASSWORD", "wc_pass123"),
    "use_pure": True,
}

BATCH_SIZE = 5_000


def load_staging() -> pd.DataFrame:
    if not STAGING_FILE.exists():
        raise FileNotFoundError(f"No se encontró {STAGING_FILE}. Ejecuta staging.py primero.")
    df = pd.read_parquet(STAGING_FILE)
    print(f"📂 Staging cargado: {len(df):,} registros")
    return df


def insert_fact_trips(cursor, df: pd.DataFrame):
    print("\n📥 Cargando fact_trips...")

    sql = """
        INSERT IGNORE INTO fact_trips (
            trip_id, taxi_id, trip_start_timestamp, trip_end_timestamp,
            trip_seconds, trip_miles, pickup_community_area, dropoff_community_area,
            fare, tips, tolls, extras, trip_total, payment_type, company,
            pickup_centroid_latitude, pickup_centroid_longitude,
            dropoff_centroid_latitude, dropoff_centroid_longitude,
            trip_date, trip_hour, trip_weekday,
            revenue_per_mile, tip_rate, is_outlier
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    cols = [
        "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp",
        "trip_seconds", "trip_miles", "pickup_community_area", "dropoff_community_area",
        "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company",
        "pickup_centroid_latitude", "pickup_centroid_longitude",
        "dropoff_centroid_latitude", "dropoff_centroid_longitude",
        "trip_date", "trip_hour", "trip_weekday",
        "revenue_per_mile", "tip_rate", "is_outlier"
    ]

    total = len(df)
    inserted = 0

    for i in range(0, total, BATCH_SIZE):
        batch = df[cols].iloc[i:i + BATCH_SIZE]
        # Convertir NaN/NaT a None para MySQL
        rows = [
            tuple(None if pd.isna(v) else v.item() if hasattr(v, 'item') else v for v in row)
            for row in batch.itertuples(index=False)
        ]
        cursor.executemany(sql, rows)
        inserted += len(rows)
        pct = inserted / total * 100
        print(f"  ↳ {inserted:,} / {total:,} ({pct:.1f}%)", end="\r")

    print(f"  ✅ fact_trips: {inserted:,} registros insertados")
    return inserted


def insert_daily_kpis(cursor, df: pd.DataFrame):
    print("\n📥 Calculando y cargando daily_kpis...")

    agg = df[df["is_outlier"] == 0].groupby("trip_date").agg(
        total_trips=("trip_id", "count"),
        active_taxis=("taxi_id", "nunique"),
        total_revenue=("trip_total", "sum"),
        total_fare=("fare", "sum"),
        total_tips=("tips", "sum"),
        total_tolls=("tolls", "sum"),
        total_extras=("extras", "sum"),
        total_trip_miles=("trip_miles", "sum"),
        total_trip_seconds=("trip_seconds", "sum"),
    ).reset_index()

    outliers = df.groupby("trip_date")["is_outlier"].sum().reset_index()
    outliers.columns = ["trip_date", "outlier_count"]
    agg = agg.merge(outliers, on="trip_date", how="left")

    sql = """
        INSERT INTO daily_kpis (
            trip_date, total_trips, active_taxis,
            total_revenue, total_fare, total_tips, total_tolls, total_extras,
            total_trip_miles, total_trip_seconds, outlier_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_trips=VALUES(total_trips),
            active_taxis=VALUES(active_taxis),
            total_revenue=VALUES(total_revenue),
            total_fare=VALUES(total_fare),
            total_tips=VALUES(total_tips),
            total_tolls=VALUES(total_tolls),
            total_extras=VALUES(total_extras),
            total_trip_miles=VALUES(total_trip_miles),
            total_trip_seconds=VALUES(total_trip_seconds),
            outlier_count=VALUES(outlier_count)
    """

    rows = [
        tuple(None if pd.isna(v) else v.item() if hasattr(v, 'item') else v for v in row)
        for row in agg.itertuples(index=False)
    ]
    cursor.executemany(sql, rows)
    print(f"  ✅ daily_kpis: {len(rows):,} días")


def insert_hourly_kpis(cursor, df: pd.DataFrame):
    print("\n📥 Calculando y cargando hourly_kpis...")

    agg = df[df["is_outlier"] == 0].groupby(["trip_date", "trip_hour"]).agg(
        trip_weekday=("trip_weekday", "first"),
        total_trips=("trip_id", "count"),
        active_taxis=("taxi_id", "nunique"),
        total_revenue=("trip_total", "sum"),
        total_fare=("fare", "sum"),
        total_tips=("tips", "sum"),
        total_tolls=("tolls", "sum"),
        total_extras=("extras", "sum"),
        total_trip_seconds=("trip_seconds", "sum"),
        total_trip_miles=("trip_miles", "sum"),
    ).reset_index()

    sql = """
        INSERT INTO hourly_kpis (
            trip_date, trip_hour, trip_weekday, total_trips, active_taxis,
            total_revenue, total_fare, total_tips, total_tolls, total_extras,
            total_trip_seconds, total_trip_miles
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            trip_weekday=VALUES(trip_weekday),
            total_trips=VALUES(total_trips),
            active_taxis=VALUES(active_taxis),
            total_revenue=VALUES(total_revenue),
            total_fare=VALUES(total_fare),
            total_tips=VALUES(total_tips),
            total_tolls=VALUES(total_tolls),
            total_extras=VALUES(total_extras),
            total_trip_seconds=VALUES(total_trip_seconds),
            total_trip_miles=VALUES(total_trip_miles)
    """

    rows = [
        tuple(None if pd.isna(v) else v.item() if hasattr(v, 'item') else v for v in row)
        for row in agg.itertuples(index=False)
    ]
    cursor.executemany(sql, rows)
    print(f"  ✅ hourly_kpis: {len(rows):,} filas")


def insert_zone_kpis(cursor, df: pd.DataFrame):
    print("\n📥 Calculando y cargando zone_kpis...")

    # NULL en área = -1 (viajes fuera de Chicago, según documentación oficial)
    clean = df[df["is_outlier"] == 0].copy()
    clean["pickup_community_area"] = clean["pickup_community_area"].fillna(-1).astype(int)
    clean["dropoff_community_area"] = clean["dropoff_community_area"].fillna(-1).astype(int)

    agg = clean.groupby(["pickup_community_area", "dropoff_community_area"]).agg(
        total_trips=("trip_id", "count"),
        active_taxis=("taxi_id", "nunique"),
        total_revenue=("trip_total", "sum"),
        total_fare=("fare", "sum"),
        total_trip_miles=("trip_miles", "sum"),
    ).reset_index()

    sql = """
        INSERT INTO zone_kpis (
            pickup_community_area, dropoff_community_area,
            total_trips, active_taxis, total_revenue, total_fare, total_trip_miles
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_trips=VALUES(total_trips),
            active_taxis=VALUES(active_taxis),
            total_revenue=VALUES(total_revenue),
            total_fare=VALUES(total_fare),
            total_trip_miles=VALUES(total_trip_miles)
    """

    rows = [
        tuple(None if pd.isna(v) else v.item() if hasattr(v, 'item') else v for v in row)
        for row in agg.itertuples(index=False)
    ]
    cursor.executemany(sql, rows)
    print(f"  ✅ zone_kpis: {len(rows):,} pares origen-destino")


def insert_zone_coords(cursor, df: pd.DataFrame):
    print("\n📥 Calculando y cargando zone_coords...")

    # Lado pickup
    pickup = df[df["pickup_community_area"].notna()][
        ["pickup_community_area", "pickup_centroid_latitude", "pickup_centroid_longitude"]
    ].copy()
    pickup.columns = ["community_area", "lat", "lon"]

    # Lado dropoff
    dropoff = df[df["dropoff_community_area"].notna()][
        ["dropoff_community_area", "dropoff_centroid_latitude", "dropoff_centroid_longitude"]
    ].copy()
    dropoff.columns = ["community_area", "lat", "lon"]

    # Combinar y promediar coordenadas por zona
    combined = pd.concat([pickup, dropoff], ignore_index=True)
    agg = combined.dropna(subset=["lat", "lon"]).groupby("community_area").agg(
        avg_latitude=("lat", "mean"),
        avg_longitude=("lon", "mean"),
    ).reset_index()

    sql = """
        INSERT INTO zone_coords (community_area, avg_latitude, avg_longitude)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            avg_latitude=VALUES(avg_latitude),
            avg_longitude=VALUES(avg_longitude)
    """

    rows = [
        tuple(None if pd.isna(v) else v.item() if hasattr(v, 'item') else v for v in row)
        for row in agg.itertuples(index=False)
    ]
    cursor.executemany(sql, rows)
    print(f"  ✅ zone_coords: {len(rows):,} zonas")


def insert_payment_kpis(cursor, df: pd.DataFrame):
    print("\n📥 Calculando y cargando payment_kpis...")

    agg = df[df["is_outlier"] == 0].groupby(["trip_date", "payment_type", "company"]).agg(
        total_trips=("trip_id", "count"),
        total_revenue=("trip_total", "sum"),
        total_tips=("tips", "sum"),
        total_fare=("fare", "sum"),
    ).reset_index()

    # Reemplazar company NULL con cadena vacía para la PK
    agg["company"] = agg["company"].fillna("")

    sql = """
        INSERT INTO payment_kpis (
            trip_date, payment_type, company,
            total_trips, total_revenue, total_tips, total_fare
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_trips=VALUES(total_trips),
            total_revenue=VALUES(total_revenue),
            total_tips=VALUES(total_tips),
            total_fare=VALUES(total_fare)
    """

    rows = [
        tuple(None if pd.isna(v) else v.item() if hasattr(v, 'item') else v for v in row)
        for row in agg.itertuples(index=False)
    ]
    cursor.executemany(sql, rows)
    print(f"  ✅ payment_kpis: {len(rows):,} filas")


def main():
    print("=" * 60)
    print("WindyCity Cabs — Load MySQL")
    print("=" * 60)
    start = datetime.now()

    df = load_staging()

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_fact_trips(cursor, df)
        conn.commit()

        insert_daily_kpis(cursor, df)
        conn.commit()

        insert_hourly_kpis(cursor, df)
        conn.commit()

        insert_zone_kpis(cursor, df)
        conn.commit()

        insert_zone_coords(cursor, df)
        conn.commit()

        insert_payment_kpis(cursor, df)
        conn.commit()

        cursor.close()
        conn.close()

    except Error as e:
        print(f"\n❌ Error MySQL: {e}")
        raise

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'=' * 60}")
    print(f"✅ Carga completa en {elapsed}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
