from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import requests
import json
import psycopg2

default_args = {
    'owner': 'renxxie',
    'start_date': days_ago(1),
    'retries': 1,
}

BASE_DIR = Path('/app')
DATA_DIR = BASE_DIR / 'data' / 'task_01'
OUTPUT_DIR = BASE_DIR / 'output' / 'task_01'

JSON_URL = 'https://raw.githubusercontent.com/LearnWebCode/json-example/master/pets-data.json'

DB_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def download_json_file():
    response = requests.get(JSON_URL)
    response.raise_for_status()

    json_path = DATA_DIR / 'pets-data.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        f.write(response.text)

    print(f"JSON data downloaded to {json_path}")
    print(f"File size: {len(response.text)} bytes")
    return str(json_path)

def parse_json_file():
    json_path = DATA_DIR / 'pets-data.json'

    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print("Original JSON structure:")
    print(json.dumps(data, indent=2))

    flattened_rows = []
    for pet in data.get('pets', []):
        name = pet.get('name')
        species = pet.get('species')
        birth_year = pet.get('birthYear')
        photo = pet.get('photo')
        fav_foods = pet.get('favFoods', [])

        if fav_foods:
            for food in fav_foods:
                flattened_rows.append({
                    'name': name,
                    'species': species,
                    'birth_year': birth_year,
                    'photo': photo,
                    'fav_food': food
                })
        else:
            flattened_rows.append({
                'name': name,
                'species': species,
                'birth_year': birth_year,
                'photo': photo,
                'fav_food': None
            })

    print(f"\nFlattened {len(flattened_rows)} rows from JSON structure")

    parsed_path = OUTPUT_DIR / 'pets_parsed.json'
    with open(parsed_path, 'w', encoding='utf-8') as f:
        json.dump(flattened_rows, f, indent=2)

    print(f"Parsed data saved to {parsed_path}")
    return len(flattened_rows)

def get_db_connection():
    return psycopg2.connect(**DB_CONN)


def init_pets_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS pets_linear;
        CREATE TABLE pets_linear (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            species VARCHAR(100),
            birth_year INTEGER,
            photo TEXT,
            fav_food TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Table pets_linear created successfully")


def load_json_to_database():
    parsed_path = OUTPUT_DIR / 'pets_parsed.json'

    with open(parsed_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Loading {len(data)} rows to database")

    init_pets_table()

    conn = get_db_connection()
    cur = conn.cursor()

    for row in data:
        cur.execute("""
            INSERT INTO pets_linear (name, species, birth_year, photo, fav_food)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row['name'],
            row['species'],
            row['birth_year'],
            row['photo'],
            row['fav_food']
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Successfully loaded {len(data)} rows to pets_linear table")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM pets_linear')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"Verified: {count} rows in pets_linear table")
    return count

with DAG(
    'json_etl_dag',
    default_args=default_args,
    description='JSON pipeline: download -> parse -> load to db',
    schedule_interval=None,
    catchup=False,
    tags=['task_01', 'json', 'etl', 'unified'],
) as dag:

    download_task = PythonOperator(
        task_id='download_json',
        python_callable=download_json_file
    )

    parse_task = PythonOperator(
        task_id='parse_json',
        python_callable=parse_json_file
    )

    load_task = PythonOperator(
        task_id='load_json_to_db',
        python_callable=load_json_to_database
    )

    download_task >> parse_task >> load_task
