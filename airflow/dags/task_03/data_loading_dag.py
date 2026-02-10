from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'renxxie',
    'start_date': days_ago(2),
    'retries': 1,
}

BASE_DIR = Path('/app')
DATA_DIR = BASE_DIR / 'output' / 'task_02'
OUTPUT_DIR = BASE_DIR / 'output' / 'task_03'

DB_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_db_connection():
    return psycopg2.connect(**DB_CONN)


def prepare_source_data():
    cleaned_path = DATA_DIR / 'temperature_cleaned.csv'
    hottest_path = DATA_DIR / 'hottest_days.csv'
    coldest_path = DATA_DIR / 'coldest_days.csv'

    df_temp = pd.read_csv(cleaned_path)
    df_hottest = pd.read_csv(hottest_path)
    df_coldest = pd.read_csv(coldest_path)

    temp_copy = OUTPUT_DIR / 'temperature_source.csv'
    hottest_copy = OUTPUT_DIR / 'hottest_days_source.csv'
    coldest_copy = OUTPUT_DIR / 'coldest_days_source.csv'

    df_temp.to_csv(temp_copy, index=False)
    df_hottest.to_csv(hottest_copy, index=False)
    df_coldest.to_csv(coldest_copy, index=False)

    return len(df_temp), len(df_hottest), len(df_coldest)


def init_temperature_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS temperature_readings_final;
        CREATE TABLE temperature_readings_final (
            id SERIAL PRIMARY KEY,
            noted_date DATE,
            out_in VARCHAR(10),
            temp NUMERIC(10, 2),
            sensor_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(noted_date, out_in, sensor_id)
        );
        CREATE INDEX idx_temperature_readings_final_date ON temperature_readings_final(noted_date);
        CREATE INDEX idx_temperature_readings_final_out_in ON temperature_readings_final(out_in);
    """)

    conn.commit()
    cur.close()
    conn.close()


def init_extreme_days_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS extreme_days_final;
        CREATE TABLE extreme_days_final (
            id SERIAL PRIMARY KEY,
            date DATE,
            avg_temp NUMERIC(10, 2),
            day_type VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, day_type)
        );
        CREATE INDEX idx_extreme_days_final_date ON extreme_days_final(date);
        CREATE INDEX idx_extreme_days_final_type ON extreme_days_final(day_type);
    """)

    conn.commit()
    cur.close()
    conn.close()


def full_load_temperature_data(**context):
    cleaned_path = DATA_DIR / 'temperature_cleaned.csv'

    df = pd.read_csv(cleaned_path)

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
                INSERT INTO temperature_readings_final (noted_date, out_in, temp, sensor_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (noted_date, out_in, sensor_id)
                DO UPDATE SET
                    temp = EXCLUDED.temp,
                    updated_at = CURRENT_TIMESTAMP
            """, (noted_date_val, out_in, float(temp) if pd.notna(temp) else None, sensor_id))
            loaded_count += 1
        except Exception as e:
            print(f"Error loading row {idx}: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM temperature_readings_final')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    log_path = OUTPUT_DIR / 'full_load_log.txt'
    with open(log_path, 'w') as f:
        f.write(f"Full load completed at: {datetime.now()}\n")
        f.write(f"Rows loaded: {loaded_count}\n")
        f.write(f"Total rows in table: {count}\n")
    print(f"Load log saved to {log_path}")

    return count


def full_load_extreme_days(**context):
    hottest_path = DATA_DIR / 'hottest_days.csv'
    coldest_path = DATA_DIR / 'coldest_days.csv'

    if not (hottest_path.exists() and coldest_path.exists()):
        print("Warning: Extreme days files not found")
        return 0

    init_extreme_days_table()

    conn = get_db_connection()
    cur = conn.cursor()

    total_loaded = 0

    hottest_df = pd.read_csv(hottest_path)
    for _, row in hottest_df.iterrows():
        date_val = datetime.strptime(row['date'], '%Y-%m-%d').date() if isinstance(row['date'], str) else row['date']
        cur.execute("""
            INSERT INTO extreme_days_final (date, avg_temp, day_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, day_type)
            DO UPDATE SET
                avg_temp = EXCLUDED.avg_temp,
                updated_at = CURRENT_TIMESTAMP
        """, (date_val, row['avg_temp'], 'hottest'))
        total_loaded += 1

    coldest_df = pd.read_csv(coldest_path)
    for _, row in coldest_df.iterrows():
        date_val = datetime.strptime(row['date'], '%Y-%m-%d').date() if isinstance(row['date'], str) else row['date']
        cur.execute("""
            INSERT INTO extreme_days_final (date, avg_temp, day_type)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, day_type)
            DO UPDATE SET
                avg_temp = EXCLUDED.avg_temp,
                updated_at = CURRENT_TIMESTAMP
        """, (date_val, row['avg_temp'], 'coldest'))
        total_loaded += 1

    conn.commit()
    cur.close()
    conn.close()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM extreme_days_final')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    log_path = OUTPUT_DIR / 'full_load_extremes_log.txt'
    with open(log_path, 'w') as f:
        f.write(f"Full extreme days load completed at: {datetime.now()}\n")
        f.write(f"Rows loaded: {total_loaded}\n")
        f.write(f"Total rows in table: {count}\n")
    print(f"Load log saved to {log_path}")

    return total_loaded


def incremental_load_temperature_data(**context):
    cleaned_path = DATA_DIR / 'temperature_cleaned.csv'
    days_to_load = 7

    df = pd.read_csv(cleaned_path)
    df['noted_date_dt'] = pd.to_datetime(df['noted_date'])

    cutoff_date = datetime.now() - timedelta(days=days_to_load)

    df_recent = df[df['noted_date_dt'] >= cutoff_date].copy()

    if len(df_recent) == 0:
        print("No recent data to load")
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'temperature_readings_final'
        )
    """)
    table_exists = cur.fetchone()[0]
    cur.close()
    conn.close()

    if not table_exists:
        print("Table does not exist, creating...")
        init_temperature_table()

    conn = get_db_connection()
    cur = conn.cursor()

    loaded_count = 0
    updated_count = 0

    for idx, row in df_recent.iterrows():
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
                SELECT id FROM temperature_readings_final
                WHERE noted_date = %s AND out_in = %s AND sensor_id = %s
            """, (noted_date_val, out_in, sensor_id))

            exists = cur.fetchone()

            if exists:
                cur.execute("""
                    UPDATE temperature_readings_final
                    SET temp = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE noted_date = %s AND out_in = %s AND sensor_id = %s
                """, (float(temp) if pd.notna(temp) else None, noted_date_val, out_in, sensor_id))
                updated_count += 1
            else:
                cur.execute("""
                    INSERT INTO temperature_readings_final (noted_date, out_in, temp, sensor_id)
                    VALUES (%s, %s, %s, %s)
                """, (noted_date_val, out_in, float(temp) if pd.notna(temp) else None, sensor_id))
                loaded_count += 1
        except Exception as e:
            print(f"Error loading row {idx}: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM temperature_readings_final')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    log_path = OUTPUT_DIR / 'incremental_load_log.txt'
    with open(log_path, 'w') as f:
        f.write(f"Incremental load completed at: {datetime.now()}\n")
        f.write(f"Days covered: last {days_to_load} days\n")
        f.write(f"New rows loaded: {loaded_count}\n")
        f.write(f"Rows updated: {updated_count}\n")
        f.write(f"Total rows in table: {count}\n")
    print(f"Load log saved to {log_path}")

    return loaded_count + updated_count


def incremental_load_extreme_days(**context):
    hottest_path = DATA_DIR / 'hottest_days.csv'
    coldest_path = DATA_DIR / 'coldest_days.csv'

    if not (hottest_path.exists() and coldest_path.exists()):
        print("Warning: Extreme days files not found")
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'extreme_days_final'
        )
    """)
    table_exists = cur.fetchone()[0]
    cur.close()
    conn.close()

    if not table_exists:
        print("Table does not exist, creating...")
        init_extreme_days_table()

    conn = get_db_connection()
    cur = conn.cursor()

    total_processed = 0
    loaded_count = 0
    updated_count = 0

    hottest_df = pd.read_csv(hottest_path)
    for _, row in hottest_df.iterrows():
        date_val = datetime.strptime(row['date'], '%Y-%m-%d').date() if isinstance(row['date'], str) else row['date']

        cur.execute("""
            SELECT id FROM extreme_days_final
            WHERE date = %s AND day_type = 'hottest'
        """, (date_val,))
        exists = cur.fetchone()

        if exists:
            cur.execute("""
                UPDATE extreme_days_final
                SET avg_temp = %s, updated_at = CURRENT_TIMESTAMP
                WHERE date = %s AND day_type = 'hottest'
            """, (row['avg_temp'], date_val))
            updated_count += 1
        else:
            cur.execute("""
                INSERT INTO extreme_days_final (date, avg_temp, day_type)
                VALUES (%s, %s, 'hottest')
            """, (date_val, row['avg_temp']))
            loaded_count += 1
        total_processed += 1

    coldest_df = pd.read_csv(coldest_path)
    for _, row in coldest_df.iterrows():
        date_val = datetime.strptime(row['date'], '%Y-%m-%d').date() if isinstance(row['date'], str) else row['date']

        cur.execute("""
            SELECT id FROM extreme_days_final
            WHERE date = %s AND day_type = 'coldest'
        """, (date_val,))
        exists = cur.fetchone()

        if exists:
            cur.execute("""
                UPDATE extreme_days_final
                SET avg_temp = %s, updated_at = CURRENT_TIMESTAMP
                WHERE date = %s AND day_type = 'coldest'
            """, (row['avg_temp'], date_val))
            updated_count += 1
        else:
            cur.execute("""
                INSERT INTO extreme_days_final (date, avg_temp, day_type)
                VALUES (%s, %s, 'coldest')
            """, (date_val, row['avg_temp']))
            loaded_count += 1
        total_processed += 1

    conn.commit()
    cur.close()
    conn.close()

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM extreme_days_final')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    log_path = OUTPUT_DIR / 'incremental_load_extremes_log.txt'
    with open(log_path, 'w') as f:
        f.write(f"Incremental extreme days load completed at: {datetime.now()}\n")
        f.write(f"New rows loaded: {loaded_count}\n")
        f.write(f"Rows updated: {updated_count}\n")
        f.write(f"Total rows in table: {count}\n")
    print(f"Load log saved to {log_path}")

    return total_processed


def verify_data_integrity(**context):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute('SELECT COUNT(*) FROM temperature_readings_final')
    temp_count = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(DISTINCT noted_date) FROM temperature_readings_final
        WHERE noted_date IS NOT NULL
    """)
    distinct_days = cur.fetchone()[0]

    cur.execute("""
        SELECT MIN(noted_date), MAX(noted_date) FROM temperature_readings_final
        WHERE noted_date IS NOT NULL
    """)
    min_date, max_date = cur.fetchone()

    cur.execute("""
        SELECT AVG(temp), MIN(temp), MAX(temp) FROM temperature_readings_final
        WHERE temp IS NOT NULL
    """)
    avg_temp, min_temp, max_temp = cur.fetchone()

    cur.execute('SELECT COUNT(*) FROM extreme_days_final')
    extreme_count = cur.fetchone()[0]

    cur.execute("""
        SELECT day_type, COUNT(*) FROM extreme_days_final
        GROUP BY day_type
    """)
    for day_type, count in cur.fetchall():
        print(f"  {day_type}: {count} days")

    cur.close()
    conn.close()

    report_path = OUTPUT_DIR / 'data_integrity_report.txt'
    with open(report_path, 'w') as f:
        f.write(f"Data Integrity Report - {datetime.now()}\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"temperature_readings_final:\n")
        f.write(f"  Total rows: {temp_count}\n")
        f.write(f"  Distinct days: {distinct_days}\n")
        f.write(f"  Date range: {min_date} to {max_date}\n")
        f.write(f"  Temperature: avg={avg_temp:.2f}, min={min_temp:.2f}, max={max_temp:.2f}\n\n")
        f.write(f"extreme_days_final:\n")
        f.write(f"  Total rows: {extreme_count}\n")

    return temp_count, extreme_count


with DAG(
    'data_loading_dag',
    default_args=default_args,
    description='Task 03: Data loading with full and incremental processes',
    schedule_interval=None,
    catchup=False,
    tags=['task_03', 'loading', 'etl'],
) as dag:

    prepare_task = PythonOperator(
        task_id='prepare_source_data',
        python_callable=prepare_source_data
    )

    full_load_temp_task = PythonOperator(
        task_id='full_load_temperature',
        python_callable=full_load_temperature_data
    )

    full_load_extremes_task = PythonOperator(
        task_id='full_load_extreme_days',
        python_callable=full_load_extreme_days
    )

    incremental_load_temp_task = PythonOperator(
        task_id='incremental_load_temperature',
        python_callable=incremental_load_temperature_data
    )

    incremental_load_extremes_task = PythonOperator(
        task_id='incremental_load_extreme_days',
        python_callable=incremental_load_extreme_days
    )

    verify_task = PythonOperator(
        task_id='verify_data_integrity',
        python_callable=verify_data_integrity
    )

    prepare_task >> [full_load_temp_task, full_load_extremes_task]
    full_load_temp_task >> incremental_load_temp_task
    full_load_extremes_task >> incremental_load_extremes_task