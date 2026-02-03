from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
from psycopg2 import sql
import os
import kagglehub

default_args = {
    'owner': 'renxxie',
    'start_date': days_ago(2),
    'retries': 1,
}

BASE_DIR = Path('/app')
DATA_DIR = BASE_DIR / 'data' / 'task_02'
OUTPUT_DIR = BASE_DIR / 'output' / 'task_02'

DB_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def download_dataset():
    import kagglehub
    import shutil

    print("Downloading dataset")

    path = kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices")

    print(f"Path to dataset files: {path}")

    download_path = Path(path)
    csv_files = list(download_path.glob('*.csv'))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {path}")

    csv_path = csv_files[0]
    print(f"Found CSV file: {csv_path}")
    print(f"File size: {csv_path.stat().st_size} bytes")

    raw_path = DATA_DIR / 'temperature_raw.csv'
    shutil.copy(csv_path, raw_path)
    print(f"Copied to {raw_path}")

    return str(raw_path)


def filter_indoor_readings(**context):
    ti = context['ti']

    raw_path = DATA_DIR / 'temperature_raw.csv'
    print(f"Reading data from {raw_path}")

    df = pd.read_csv(raw_path)

    print(f"Original data shape: {df.shape}")
    print(f"Original columns: {df.columns.tolist()}")
    print(f"First few rows:\n{df.head()}")

    if 'out/in' in df.columns:
        df['out/in'] = df['out/in'].str.lower()
        df_filtered = df[df['out/in'] == 'in'].copy()
        print(f"\nAfter filtering out/in='in': {df_filtered.shape} rows")
        print(f"Removed {len(df) - len(df_filtered)} outdoor readings")
    else:
        print("\nWarning: 'out/in' column not found, skipping this filter")
        df_filtered = df.copy()

    filtered_path = DATA_DIR / 'temperature_filtered.csv'
    df_filtered.to_csv(filtered_path, index=False)
    print(f"Filtered data saved to {filtered_path}")

    return str(filtered_path)


def convert_dates(**context):
    ti = context['ti']

    filtered_path = DATA_DIR / 'temperature_filtered.csv'
    print(f"Reading filtered data from {filtered_path}")

    df = pd.read_csv(filtered_path)
    print(f"Input data shape: {df.shape}")

    if 'noted_date' in df.columns:
        print(f"Original date format sample: {df['noted_date'].head()}")

        df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
        df['noted_date'] = df['noted_date'].dt.strftime('%Y-%m-%d')
        df['noted_date'] = pd.to_datetime(df['noted_date']).dt.date

        print(f"Date conversion completed. Sample dates: {df['noted_date'].head()}")
        print(f"Invalid dates: {df['noted_date'].isna().sum()}")
    else:
        print("\nWarning: 'noted_date' column not found")

    dated_path = DATA_DIR / 'temperature_dated.csv'
    df.to_csv(dated_path, index=False)
    print(f"Data with converted dates saved to {dated_path}")

    return str(dated_path)


def clean_temperature_outliers(**context):
    ti = context['ti']

    dated_path = DATA_DIR / 'temperature_dated.csv'
    print(f"Reading dated data from {dated_path}")

    df = pd.read_csv(dated_path)
    print(f"Input data shape: {df.shape}")

    if 'temp' in df.columns:
        df['temp'] = pd.to_numeric(df['temp'], errors='coerce')

        df = df.dropna(subset=['temp'])

        p5 = df['temp'].quantile(0.05)
        p95 = df['temp'].quantile(0.95)
        print(f"\nTemperature statistics:")
        print(f"  Min: {df['temp'].min()}")
        print(f"  Max: {df['temp'].max()}")
        print(f"  Mean: {df['temp'].mean()}")
        print(f"  5th percentile: {p5}")
        print(f"  95th percentile: {p95}")

        df_cleaned = df[
            (df['temp'] >= p5) & (df['temp'] <= p95)
        ].copy()

        outliers_count = len(df) - len(df_cleaned)
        print(f"Removed {outliers_count} temperature outliers ({outliers_count/len(df)*100:.2f}%)")
        print(f"After cleaning: {df_cleaned.shape} rows")
    else:
        print("\nWarning: 'temp' column not found, skipping temperature cleaning")
        df_cleaned = df.copy()

    cleaned_path = OUTPUT_DIR / 'temperature_cleaned.csv'
    df_cleaned.to_csv(cleaned_path, index=False)
    print(f"Cleaned data saved to {cleaned_path}")

    return str(cleaned_path)


def find_extreme_days(**context):
    ti = context['ti']

    cleaned_path = OUTPUT_DIR / 'temperature_cleaned.csv'
    print(f"Reading cleaned data from {cleaned_path}")

    df = pd.read_csv(cleaned_path)
    print(f"Input data shape: {df.shape}")

    if 'noted_date' in df.columns and 'temp' in df.columns:
        df['noted_date_dt'] = pd.to_datetime(df['noted_date'])

        daily_avg = df.groupby('noted_date_dt')['temp'].agg(['mean', 'count']).reset_index()
        daily_avg.columns = ['date', 'avg_temp', 'reading_count']

        print(f"\nCalculated averages for {len(daily_avg)} unique days")

        hottest_days = daily_avg.nlargest(5, 'avg_temp').sort_values('avg_temp', ascending=False)
        print("\n5 Hottest days:")
        for _, row in hottest_days.iterrows():
            print(f"  {row['date'].strftime('%Y-%m-%d')}: {row['avg_temp']:.2f}°C ({int(row['reading_count'])} readings)")

        coldest_days = daily_avg.nsmallest(5, 'avg_temp').sort_values('avg_temp', ascending=True)
        print("\n5 Coldest days:")
        for _, row in coldest_days.iterrows():
            print(f"  {row['date'].strftime('%Y-%m-%d')}: {row['avg_temp']:.2f}°C ({int(row['reading_count'])} readings)")

        hottest_path = OUTPUT_DIR / 'hottest_days.csv'
        coldest_path = OUTPUT_DIR / 'coldest_days.csv'

        hottest_days[['date', 'avg_temp']].to_csv(hottest_path, index=False)
        coldest_days[['date', 'avg_temp']].to_csv(coldest_path, index=False)

        print(f"\nHottest days saved to {hottest_path}")
        print(f"Coldest days saved to {coldest_path}")

        return len(hottest_days), len(coldest_days)
    else:
        print("\nWarning: Cannot calculate hottest/coldest days - required columns missing")
        return 0, 0


def get_db_connection():
    return psycopg2.connect(**DB_CONN)


def init_temperature_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS temperature_readings;
        CREATE TABLE temperature_readings (
            id SERIAL PRIMARY KEY,
            noted_date DATE,
            out_in VARCHAR(10),
            temp NUMERIC(10, 2),
            sensor_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Table temperature_readings created successfully")


def init_extreme_days_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS extreme_days;
        CREATE TABLE extreme_days (
            id SERIAL PRIMARY KEY,
            date DATE,
            avg_temp NUMERIC(10, 2),
            day_type VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Table extreme_days created successfully")


def load_readings_to_db(**context):
    cleaned_path = OUTPUT_DIR / 'temperature_cleaned.csv'

    print(f"Loading readings from {cleaned_path}")

    df = pd.read_csv(cleaned_path)
    print(f"Read {len(df)} rows from cleaned data")
    print(f"Columns: {df.columns.tolist()}")

    init_temperature_table()

    conn = get_db_connection()
    cur = conn.cursor()

    loaded_count = 0
    for idx, row in df.iterrows():
        try:
            noted_date = row.get('noted_date')
            out_in = row.get('out/in', 'in')
            temp = row.get('temp')

            if pd.notna(noted_date):
                if isinstance(noted_date, str):
                    noted_date = datetime.strptime(noted_date, '%Y-%m-%d').date()
                noted_date_val = noted_date
            else:
                noted_date_val = None

            sensor_id = idx

            cur.execute("""
                INSERT INTO temperature_readings (noted_date, out_in, temp, sensor_id)
                VALUES (%s, %s, %s, %s)
            """, (noted_date_val, out_in, float(temp) if pd.notna(temp) else None, sensor_id))
            loaded_count += 1
        except Exception as e:
            print(f"Error loading row {idx}: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()

    print(f"Successfully loaded {loaded_count} rows to temperature_readings table")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM temperature_readings')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"Verified: {count} rows in temperature_readings table")

    return count


def load_extreme_days_to_db(**context):
    hottest_path = OUTPUT_DIR / 'hottest_days.csv'
    coldest_path = OUTPUT_DIR / 'coldest_days.csv'

    if not (hottest_path.exists() and coldest_path.exists()):
        print("Warning: Extreme days files not found")
        return 0

    init_extreme_days_table()

    conn = get_db_connection()
    cur = conn.cursor()

    hottest_df = pd.read_csv(hottest_path)
    print(f"Loading {len(hottest_df)} hottest days")
    for _, row in hottest_df.iterrows():
        date_val = datetime.strptime(row['date'], '%Y-%m-%d').date() if isinstance(row['date'], str) else row['date']
        cur.execute("""
            INSERT INTO extreme_days (date, avg_temp, day_type)
            VALUES (%s, %s, %s)
        """, (date_val, row['avg_temp'], 'hottest'))

    coldest_df = pd.read_csv(coldest_path)
    print(f"Loading {len(coldest_df)} coldest days")
    for _, row in coldest_df.iterrows():
        date_val = datetime.strptime(row['date'], '%Y-%m-%d').date() if isinstance(row['date'], str) else row['date']
        cur.execute("""
            INSERT INTO extreme_days (date, avg_temp, day_type)
            VALUES (%s, %s, %s)
        """, (date_val, row['avg_temp'], 'coldest'))

    conn.commit()
    cur.close()
    conn.close()

    total_extreme = len(hottest_df) + len(coldest_df)
    print(f"Successfully loaded {total_extreme} extreme days to database")

    return total_extreme


with DAG(
    'temperature_etl_dag',
    default_args=default_args,
    description='Temperature data pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['task_02'],
) as dag:

    download_task = PythonOperator(
        task_id='download_dataset',
        python_callable=download_dataset
    )

    filter_task = PythonOperator(
        task_id='filter_indoor_readings',
        python_callable=filter_indoor_readings
    )

    convert_dates_task = PythonOperator(
        task_id='convert_dates',
        python_callable=convert_dates
    )

    clean_outliers_task = PythonOperator(
        task_id='clean_temperature_outliers',
        python_callable=clean_temperature_outliers
    )

    find_extremes_task = PythonOperator(
        task_id='find_extreme_days',
        python_callable=find_extreme_days
    )

    load_readings_task = PythonOperator(
        task_id='load_readings_to_db',
        python_callable=load_readings_to_db
    )

    load_extremes_task = PythonOperator(
        task_id='load_extreme_days_to_db',
        python_callable=load_extreme_days_to_db
    )

    (
        download_task
        >> filter_task
        >> convert_dates_task
        >> clean_outliers_task
        >> find_extremes_task
        >> [load_readings_task, load_extremes_task]
    )
