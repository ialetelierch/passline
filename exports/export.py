import os
import csv
from pathlib import Path
from datetime import datetime

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3307)),
    "database": os.getenv("DB_NAME", "windycity"),
    "user": os.getenv("DB_USER", "wc_user"),
    "password": os.getenv("DB_PASSWORD", "wc_pass123"),
    "use_pure": True,
}

EXPORT_DIR = Path("exports")

QUERIES = {
    "daily_kpis": "SELECT * FROM daily_kpis ORDER BY trip_date ASC",
    "hourly_kpis": "SELECT * FROM hourly_kpis ORDER BY trip_date ASC, trip_hour ASC",
    "zone_kpis": "SELECT * FROM zone_kpis ORDER BY total_trips DESC",
    "zone_coords": "SELECT * FROM zone_coords ORDER BY community_area ASC",
    "payment_kpis": "SELECT * FROM payment_kpis ORDER BY trip_date ASC, payment_type ASC",
}


def export_table(cursor, name: str, query: str) -> int:
    filepath = EXPORT_DIR / f"{name}.csv"
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"  ✅ {name}.csv → {len(rows):,} filas → {filepath}")
    return len(rows)


def main():
    print("=" * 60)
    print("WindyCity Cabs — Export CSV para Looker Studio")
    print("=" * 60)
    start = datetime.now()

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    total_rows = 0
    for name, query in QUERIES.items():
        total_rows += export_table(cursor, name, query)

    cursor.close()
    conn.close()

    elapsed = (datetime.now() - start).seconds
    print(f"\n✅ Export completo — {total_rows:,} filas totales en {elapsed}s")
    print(f"📁 Archivos en: {EXPORT_DIR}/")
    print("\nPróximo paso: subir los CSV a Google Sheets y conectar Looker Studio")
    print("=" * 60)


if __name__ == "__main__":
    main()
