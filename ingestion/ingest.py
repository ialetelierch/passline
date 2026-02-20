import requests
import json
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL")
PAGE_SIZE = 50_000
START_DATE = "2025-12-03T00:00:00"
END_DATE = "2026-01-31T23:59:59"
OUTPUT_DIR = Path("data/raw")

def fetch_page(offset: int) -> list:
    params = {
        "$where": f"trip_start_timestamp >= '{START_DATE}' AND trip_start_timestamp <= '{END_DATE}'",
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": "trip_start_timestamp ASC"
    }
    response = requests.get(API_BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def save_page(data: list, page_num: int):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / f"page_{page_num:04d}.json"
    with open(filepath, "w") as f:
        json.dump(data, f)
    return filepath

def main():
    print("=" * 60)
    print("WindyCity Cabs — Ingesta inicial")
    print(f"Ventana: {START_DATE} → {END_DATE}")
    print(f"Page size: {PAGE_SIZE:,}")
    print("=" * 60)

    total_records = 0
    page_num = 1
    offset = 0
    start_time = datetime.now()

    while True:
        print(f"\n📦 Página {page_num} | offset={offset:,}", end=" ... ")

        data = fetch_page(offset)
        records_in_page = len(data)

        if records_in_page == 0:
            print("sin datos, fin de ingesta.")
            break

        filepath = save_page(data, page_num)
        total_records += records_in_page
        elapsed = (datetime.now() - start_time).seconds

        print(f"✅ {records_in_page:,} registros → {filepath.name} | total acumulado: {total_records:,} | {elapsed}s")

        if records_in_page < PAGE_SIZE:
            print("\n🏁 Última página alcanzada.")
            break

        offset += PAGE_SIZE
        page_num += 1

    elapsed_total = (datetime.now() - start_time).seconds
    print("\n" + "=" * 60)
    print(f"✅ Ingesta completa")
    print(f"   Total registros : {total_records:,}")
    print(f"   Páginas          : {page_num}")
    print(f"   Archivos en      : {OUTPUT_DIR}/")
    print(f"   Tiempo total     : {elapsed_total}s")
    print("=" * 60)

if __name__ == "__main__":
    main()