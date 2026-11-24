from datetime import datetime, timedelta
import os
import pandas as pd
import sqlalchemy
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_air_quality_indonesia",
    default_args=default_args,
    description="Pipeline ETL Air Quality Index Indonesia",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "air_quality", "uas_data_engineering"],
)

def extract_air_quality_data():
    base_dir = os.path.join(os.sep, "opt", "airflow", "data")
    source_path = os.path.join(base_dir, "raw", "idn_air_quality_daily.csv")
    if not os.path.exists(source_path):
        source_path = os.path.join("data", "raw", "idn_air_quality_daily.csv")
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"File raw data tidak ditemukan: {source_path}")
    df = pd.read_csv(source_path)
    required_cols = [
        "city",
        "province",
        "island",
        "date",
        "pm2_5_mean",
        "pm2_5_max",
        "pm10_mean",
        "us_aqi_mean",
        "us_aqi_max",
        "hours",
        "exceeds_who_pm25",
    ]
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Kolom wajib tidak ditemukan: {col}")
    total_records = len(df)
    if total_records == 0:
        raise ValueError("Dataset raw kosong")
    print(f"Extract berhasil: total {total_records} baris terbaca.")
    return total_records

def transform_air_quality_data():
    base_dir = os.path.join(os.sep, "opt", "airflow", "data")
    source_path = os.path.join(base_dir, "raw", "idn_air_quality_daily.csv")
    if not os.path.exists(source_path):
        source_path = os.path.join("data", "raw", "idn_air_quality_daily.csv")
    df = pd.read_csv(source_path)

    city_median_mean = df.groupby("city")["us_aqi_mean"].transform("median")
    city_median_max = df.groupby("city")["us_aqi_max"].transform("median")
    df["us_aqi_mean"] = df["us_aqi_mean"].fillna(city_median_mean)
    df["us_aqi_max"] = df["us_aqi_max"].fillna(city_median_max)
    df["us_aqi_mean"] = df["us_aqi_mean"].bfill()
    df["us_aqi_max"] = df["us_aqi_max"].bfill()

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["quarter"] = df["date"].dt.quarter
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    aqi_bins = [0, 50, 100, 150, 200, 300, 5000]
    aqi_labels = [
        "Baik",
        "Sedang",
        "Tidak Sehat Kelompok Sensitif",
        "Tidak Sehat",
        "Sangat Tidak Sehat",
        "Berbahaya",
    ]
    df["aqi_category"] = pd.cut(
        df["us_aqi_mean"], bins=aqi_bins, labels=aqi_labels, include_lowest=True
    ).astype(str)

    df["is_severe_pollution"] = df["us_aqi_mean"].ge(150)
    df["pm2_5_mean"] = df["pm2_5_mean"].round(2)
    df["pm2_5_max"] = df["pm2_5_max"].round(2)
    df["pm10_mean"] = df["pm10_mean"].round(2)
    df["us_aqi_mean"] = df["us_aqi_mean"].round(2)
    df["us_aqi_max"] = df["us_aqi_max"].round(2)

    summary_df = (
        df.groupby(["province", "island", "year"])
        .agg(
            total_observations=("city", "count"),
            avg_pm2_5=("pm2_5_mean", "mean"),
            max_pm2_5=("pm2_5_max", "max"),
            avg_aqi=("us_aqi_mean", "mean"),
            max_aqi=("us_aqi_max", "max"),
            who_exceed_days=("exceeds_who_pm25", "sum"),
        )
        .reset_index()
    )

    summary_df["who_exceed_ratio"] = (
        summary_df["who_exceed_days"]
        .div(summary_df["total_observations"])
        .round(4)
    )
    summary_df["avg_pm2_5"] = summary_df["avg_pm2_5"].round(2)
    summary_df["avg_aqi"] = summary_df["avg_aqi"].round(2)

    out_dir = os.path.join(base_dir, "processed")
    if not os.path.exists(out_dir):
        out_dir = os.path.join("data", "processed")
    os.makedirs(out_dir, exist_ok=True)

    clean_path = os.path.join(out_dir, "clean_air_quality_daily.csv")
    summary_path = os.path.join(out_dir, "air_quality_province_summary.csv")

    df.to_csv(clean_path, index=False)
    summary_df.to_csv(summary_path, index=False)
    print(f"Transform berhasil: clean data {len(df)} baris tersimpan.")
    return len(df)

def load_data_to_warehouse():
    base_dir = os.path.join(os.sep, "opt", "airflow", "data")
    out_dir = os.path.join(base_dir, "processed")
    if not os.path.exists(out_dir):
        out_dir = os.path.join("data", "processed")

    clean_path = os.path.join(out_dir, "clean_air_quality_daily.csv")
    summary_path = os.path.join(out_dir, "air_quality_province_summary.csv")

    df = pd.read_csv(clean_path)
    summary_df = pd.read_csv(summary_path)

    db_url = sqlalchemy.engine.URL.create(
        drivername="postgresql+psycopg",
        username="airflow",
        password="airflow",
        host="postgres",
        port=5432,
        database="airflow",
    )
    engine = sqlalchemy.create_engine(db_url)

    df.to_sql(
        name="air_quality_daily",
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=5000,
    )
    summary_df.to_sql(
        name="air_quality_province_summary",
        con=engine,
        if_exists="replace",
        index=False,
    )
    print("Load berhasil: data harian dan agregasi termuat di PostgreSQL.")
    return len(df)

def verify_warehouse_data():
    db_url = sqlalchemy.engine.URL.create(
        drivername="postgresql+psycopg",
        username="airflow",
        password="airflow",
        host="postgres",
        port=5432,
        database="airflow",
    )
    engine = sqlalchemy.create_engine(db_url)

    with engine.connect() as conn:
        daily_res = conn.execute(
            sqlalchemy.text("SELECT COUNT(1) FROM air_quality_daily")
        )
        daily_count = daily_res.scalar()

        summary_res = conn.execute(
            sqlalchemy.text("SELECT COUNT(1) FROM air_quality_province_summary")
        )
        summary_count = summary_res.scalar()

        sample_res = conn.execute(
            sqlalchemy.text(
                "SELECT city, date, pm2_5_mean, aqi_category FROM air_quality_daily LIMIT 3"
            )
        )
        samples = sample_res.fetchall()

    if daily_count == 0:
        raise ValueError("Tabel air_quality_daily kosong di warehouse")
    if summary_count == 0:
        raise ValueError("Tabel air_quality_province_summary kosong di warehouse")

    print(f"Verifikasi sukses: {daily_count} baris di tabel air_quality_daily.")
    print(
        f"Verifikasi sukses: {summary_count} baris di tabel air_quality_province_summary."
    )
    for sample in samples:
        print(f"Sample data: {sample}")
    return daily_count

task_extract = PythonOperator(
    task_id="extract_raw_data",
    python_callable=extract_air_quality_data,
    dag=dag,
)

task_transform = PythonOperator(
    task_id="transform_air_quality_data",
    python_callable=transform_air_quality_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id="load_data_to_warehouse",
    python_callable=load_data_to_warehouse,
    dag=dag,
)

task_verify = PythonOperator(
    task_id="verify_warehouse_data",
    python_callable=verify_warehouse_data,
    dag=dag,
)

task_extract.set_downstream(task_transform)
task_transform.set_downstream(task_load)
task_load.set_downstream(task_verify)
