import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME", "windycity"),
    "user": os.getenv("DB_USER", "wc_user"),
    "password": os.getenv("DB_PASSWORD", "wc_pass123"),
    "use_pure": True,
}

TABLES = {
    "fact_trips": """
        CREATE TABLE IF NOT EXISTS fact_trips (
            trip_id                     VARCHAR(64)     NOT NULL,
            taxi_id                     VARCHAR(128),
            trip_start_timestamp        DATETIME,
            trip_end_timestamp          DATETIME,
            trip_seconds                INT,
            trip_miles                  FLOAT,
            pickup_community_area       INT,
            dropoff_community_area      INT,
            fare                        DECIMAL(10,2),
            tips                        DECIMAL(10,2),
            tolls                       DECIMAL(10,2),
            extras                      DECIMAL(10,2),
            trip_total                  DECIMAL(10,2),
            payment_type                VARCHAR(32),
            company                     VARCHAR(128),
            pickup_centroid_latitude    FLOAT,
            pickup_centroid_longitude   FLOAT,
            dropoff_centroid_latitude   FLOAT,
            dropoff_centroid_longitude  FLOAT,
            -- Campos derivados
            trip_date                   DATE,
            trip_hour                   TINYINT,
            trip_weekday                TINYINT,
            revenue_per_mile            FLOAT,
            tip_rate                    FLOAT,
            is_outlier                  TINYINT(1)      DEFAULT 0,
            PRIMARY KEY (trip_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    "daily_kpis": """
        DROP TABLE IF EXISTS daily_kpis;
        CREATE TABLE daily_kpis (
            trip_date               DATE            NOT NULL,
            total_trips             INT,
            active_taxis            INT,
            total_revenue           DECIMAL(12,2),
            total_fare              DECIMAL(12,2),
            total_tips              DECIMAL(12,2),
            total_tolls             DECIMAL(12,2),
            total_extras            DECIMAL(12,2),
            total_trip_miles        FLOAT,
            total_trip_seconds      INT,
            outlier_count           INT,
            PRIMARY KEY (trip_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    "hourly_kpis": """
        DROP TABLE IF EXISTS hourly_kpis;
        CREATE TABLE hourly_kpis (
            trip_date           DATE        NOT NULL,
            trip_hour           TINYINT     NOT NULL,
            trip_weekday        TINYINT,
            total_trips         INT,
            active_taxis        INT,
            total_revenue       DECIMAL(12,2),
            total_fare          DECIMAL(12,2),
            total_tips          DECIMAL(12,2),
            total_tolls         DECIMAL(12,2),
            total_extras        DECIMAL(12,2),
            total_trip_seconds  INT,
            total_trip_miles    FLOAT,
            PRIMARY KEY (trip_date, trip_hour)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    "zone_kpis": """
        DROP TABLE IF EXISTS zone_kpis;
        CREATE TABLE zone_kpis (
            pickup_community_area   INT             NOT NULL,
            dropoff_community_area  INT             NOT NULL,
            total_trips             INT,
            active_taxis            INT,
            total_revenue           DECIMAL(12,2),
            total_fare              DECIMAL(12,2),
            total_trip_miles        FLOAT,
            PRIMARY KEY (pickup_community_area, dropoff_community_area)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    "zone_coords": """
        DROP TABLE IF EXISTS zone_coords;
        CREATE TABLE zone_coords (
            community_area  INT     NOT NULL,
            avg_latitude    FLOAT,
            avg_longitude   FLOAT,
            PRIMARY KEY (community_area)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    "payment_kpis": """
        DROP TABLE IF EXISTS payment_kpis;
        CREATE TABLE payment_kpis (
            trip_date       DATE        NOT NULL,
            payment_type    VARCHAR(32) NOT NULL,
            company         VARCHAR(128) NOT NULL DEFAULT '',
            total_trips     INT,
            total_revenue   DECIMAL(12,2),
            total_tips      DECIMAL(12,2),
            total_fare      DECIMAL(12,2),
            PRIMARY KEY (trip_date, payment_type, company)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
}


def main():
    print("=" * 60)
    print("WindyCity Cabs — Schema MySQL")
    print("=" * 60)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for table_name, ddl in TABLES.items():
        # Ejecutar cada statement por separado (DROP + CREATE para KPI tables)
        statements = [s.strip() for s in ddl.strip().split(";") if s.strip()]
        for stmt in statements:
            cursor.execute(stmt)
        print(f"  ✅ Tabla creada/verificada: {table_name}")

    conn.commit()
    cursor.close()
    conn.close()

    print("\n✅ Schema listo en MySQL")
    print("=" * 60)


if __name__ == "__main__":
    main()
