#!/usr/bin/env python3
"""
CLI & Helper Skrip Interaksi Database Data Warehouse Kualitas Udara Indonesia.
Mendukung koneksi ke SQLite lokal (data/processed/air_quality_warehouse.db)
dan PostgreSQL (localhost:5433, database: airflow).
"""

import os
import sys
import sqlite3
import pandas as pd

DB_PATH = os.path.join("data", "processed", "air_quality_warehouse.db")

def get_connection():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database warehouse belum ditemukan di {DB_PATH}. Jalankan pipeline ETL terlebih dahulu.")
    return sqlite3.connect(DB_PATH)

def show_warehouse_status():
    print("=" * 68)
    print("      STATUS DATA WAREHOUSE KUALITAS UDARA INDONESIA")
    print("=" * 68)
    print(f"• Lokasi Berkas Database : {os.path.abspath(DB_PATH)}")
    print(f"• Ukuran Berkas Database : {os.path.getsize(DB_PATH) / (1024 * 1024):.2f} MB")
    print(f"• Engine Utama (Lokal)   : SQLite (Embedded Relational DB)")
    print(f"• Engine Server (Docker) : PostgreSQL (localhost:5433, db: airflow)")
    print("-" * 68)

    conn = get_connection()
    c = conn.cursor()
    
    # 1. Tabel air_quality_daily
    c.execute("SELECT COUNT(1) FROM air_quality_daily")
    daily_count = c.fetchone()[0]
    
    # 2. Tabel air_quality_province_summary
    c.execute("SELECT COUNT(1) FROM air_quality_province_summary")
    summary_count = c.fetchone()[0]
    
    print(f"1. Tabel 'air_quality_daily'            : {daily_count:,} baris")
    print(f"2. Tabel 'air_quality_province_summary' : {summary_count:,} baris")
    print("=" * 68)
    
    # Sample queries
    print("\n[SAMPLE 1] 5 Wilayah dengan Rata-rata US AQI Tertinggi (Paling Berpolusi):")
    q1 = """
        SELECT province, island, ROUND(AVG(us_aqi_mean), 1) AS avg_aqi, ROUND(AVG(pm2_5_mean), 1) AS avg_pm2_5
        FROM air_quality_daily
        GROUP BY province, island
        ORDER BY avg_aqi DESC
        LIMIT 5;
    """
    df1 = pd.read_sql_query(q1, conn)
    print(df1.to_string(index=False))

    print("\n[SAMPLE 2] 5 Wilayah dengan Kualitas Udara Paling Sehat:")
    q2 = """
        SELECT province, island, ROUND(AVG(us_aqi_mean), 1) AS avg_aqi, ROUND(AVG(pm2_5_mean), 1) AS avg_pm2_5
        FROM air_quality_daily
        GROUP BY province, island
        ORDER BY avg_aqi ASC
        LIMIT 5;
    """
    df2 = pd.read_sql_query(q2, conn)
    print(df2.to_string(index=False))
    conn.close()

def execute_custom_query(sql):
    conn = get_connection()
    print(f"\nMenjalankan Kueri SQL: {sql}")
    print("-" * 60)
    try:
        df = pd.read_sql_query(sql, conn)
        print(df.to_string())
        print(f"\n({len(df)} baris dikembalikan)")
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        execute_custom_query(query)
    else:
        show_warehouse_status()
