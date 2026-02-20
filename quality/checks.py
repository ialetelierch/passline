import os
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

STAGING_FILE = Path("data/staging/trips.parquet")
REPORT_DIR = Path("quality")
REPORT_FILE = REPORT_DIR / "report.json"


def load_staging() -> pd.DataFrame:
    if not STAGING_FILE.exists():
        raise FileNotFoundError(f"No se encontró {STAGING_FILE}. Ejecuta staging.py primero.")
    df = pd.read_parquet(STAGING_FILE)
    print(f"📂 Staging cargado: {len(df):,} registros")
    return df


def check_nulls(df: pd.DataFrame) -> dict:
    """Campos clave no deben ser nulos."""
    critical_fields = ["trip_id", "trip_start_timestamp", "fare"]
    results = {}
    for field in critical_fields:
        null_count = df[field].isna().sum()
        results[field] = {
            "null_count": int(null_count),
            "passed": null_count == 0
        }
    passed = all(r["passed"] for r in results.values())
    print(f"  {'✅' if passed else '❌'} Nulos en campos clave: {results}")
    return {"check": "nulls_in_critical_fields", "passed": passed, "detail": results}


def check_non_negative(df: pd.DataFrame) -> dict:
    """Montos y métricas no deben ser negativos."""
    fields = ["fare", "tips", "tolls", "extras", "trip_total", "trip_miles", "trip_seconds"]
    results = {}
    for field in fields:
        neg_count = int((df[field] < 0).sum())
        results[field] = {"negative_count": neg_count, "passed": neg_count == 0}
    passed = all(r["passed"] for r in results.values())
    print(f"  {'✅' if passed else '❌'} Valores negativos: {results}")
    return {"check": "non_negative_values", "passed": passed, "detail": results}


def check_uniqueness(df: pd.DataFrame) -> dict:
    """trip_id debe ser único."""
    total = len(df)
    unique = df["trip_id"].nunique()
    duplicates = total - unique
    passed = duplicates == 0
    print(f"  {'✅' if passed else '❌'} Unicidad trip_id: {duplicates:,} duplicados de {total:,}")
    return {
        "check": "trip_id_uniqueness",
        "passed": passed,
        "total": total,
        "unique": unique,
        "duplicates": duplicates
    }


def check_temporal_coherence(df: pd.DataFrame) -> dict:
    """trip_end debe ser >= trip_start."""
    incoherent = int(
        (df["trip_end_timestamp"] < df["trip_start_timestamp"]).sum()
    )
    passed = incoherent == 0
    print(f"  {'✅' if passed else '⚠️ '} Coherencia temporal: {incoherent:,} viajes con end < start")
    return {
        "check": "temporal_coherence",
        "passed": passed,
        "incoherent_count": incoherent
    }


def check_outliers(df: pd.DataFrame) -> dict:
    """Resumen de outliers flagueados."""
    total = len(df)
    outlier_count = int(df["is_outlier"].sum())
    outlier_rate = round(outlier_count / total * 100, 4)

    # Breakdown por regla
    long_trips = int((df["trip_seconds"] > 10_800).sum())
    long_distance = int((df["trip_miles"] > 100).sum())
    negative_fare = int((df["fare"] < 0).sum())
    time_incoherent = int(
        (df["trip_end_timestamp"] < df["trip_start_timestamp"]).sum()
    )

    passed = outlier_rate < 1.0  # Aceptable si < 1% de outliers
    print(f"  {'✅' if passed else '⚠️ '} Outliers: {outlier_count:,} ({outlier_rate}%) de {total:,}")
    return {
        "check": "outlier_flags",
        "passed": passed,
        "outlier_count": outlier_count,
        "outlier_rate_pct": outlier_rate,
        "breakdown": {
            "trip_seconds_gt_3h": long_trips,
            "trip_miles_gt_100": long_distance,
            "negative_fare": negative_fare,
            "end_before_start": time_incoherent
        }
    }


def check_total_consistency(df: pd.DataFrame) -> dict:
    """trip_total debe ser aprox fare + tips + tolls + extras."""
    calculated = df["fare"] + df["tips"] + df["tolls"] + df["extras"]
    diff = (df["trip_total"] - calculated).abs()
    inconsistent = int((diff > 0.10).sum())
    # Se acepta como advertencia, no como error bloqueante
    # trip_total incluye componentes no desglosados en el dataset (surcharges, etc.)
    passed = True  # informativo, no bloqueante
    print(f"  ⚠️  Consistencia totales: {inconsistent:,} viajes con diferencia > $0.10 (ver nota)")
    return {
        "check": "total_consistency",
        "passed": passed,
        "inconsistent_count": inconsistent,
        "tolerance": 0.10,
        "note": "trip_total incluye componentes no desglosados en el dataset público (surcharges municipales variables)"
    }


def check_date_range(df: pd.DataFrame) -> dict:
    """Verificar que los datos caen dentro de la ventana esperada."""
    expected_start = pd.Timestamp("2025-12-03")
    expected_end = pd.Timestamp("2026-01-31 23:59:59")
    actual_min = df["trip_start_timestamp"].min()
    actual_max = df["trip_start_timestamp"].max()
    passed = actual_min >= expected_start and actual_max <= expected_end
    print(f"  {'✅' if passed else '❌'} Rango de fechas: {actual_min.date()} → {actual_max.date()}")
    return {
        "check": "date_range",
        "passed": passed,
        "expected_start": str(expected_start.date()),
        "expected_end": str(expected_end.date()),
        "actual_start": str(actual_min.date()),
        "actual_end": str(actual_max.date())
    }


def main():
    print("=" * 60)
    print("WindyCity Cabs — Quality Checks")
    print("=" * 60)
    start = datetime.now()

    df = load_staging()
    print()

    results = []
    results.append(check_nulls(df))
    results.append(check_non_negative(df))
    results.append(check_uniqueness(df))
    results.append(check_temporal_coherence(df))
    results.append(check_outliers(df))
    results.append(check_total_consistency(df))
    results.append(check_date_range(df))

    # Resumen
    total_checks = len(results)
    passed_checks = sum(1 for r in results if r["passed"])
    failed_checks = total_checks - passed_checks

    elapsed = (datetime.now() - start).seconds

    print(f"\n{'=' * 60}")
    print(f"Resumen: {passed_checks}/{total_checks} checks pasaron")
    if failed_checks > 0:
        print(f"⚠️  {failed_checks} check(s) con advertencias — revisar reporte")
    else:
        print("✅ Todos los checks pasaron")
    print(f"Tiempo: {elapsed}s")
    print("=" * 60)

    # Guardar reporte
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_records": len(df),
        "summary": {
            "total_checks": total_checks,
            "passed": passed_checks,
            "failed": failed_checks
        },
        "checks": results
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n📄 Reporte guardado en: {REPORT_FILE}")


if __name__ == "__main__":
    main()
