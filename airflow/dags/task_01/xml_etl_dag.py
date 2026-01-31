from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import requests
import json
import psycopg2
from psycopg2 import sql
import xml.etree.ElementTree as ET

default_args = {
    'owner': 'renxxie',
    'start_date': days_ago(1),
    'retries': 1,
}

BASE_DIR = Path('/app')
DATA_DIR = BASE_DIR / 'data' / 'task_01'
OUTPUT_DIR = BASE_DIR / 'output' / 'task_01'

XML_URL = 'https://gist.githubusercontent.com/pamelafox/3000322/raw/nutrition.xml'

DB_CONN = {
    'host': 'postgres',
    'port': 5432,
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow'
}

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def download_xml_file():
    response = requests.get(XML_URL)
    response.raise_for_status()

    xml_path = DATA_DIR / 'nutrition.xml'
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(response.text)

    print(f"XML data downloaded to {xml_path}")
    print(f"File size: {len(response.text)} bytes")
    return str(xml_path)

def parse_xml_file():
    xml_path = DATA_DIR / 'nutrition.xml'
    tree = ET.parse(xml_path)
    root = tree.getroot()

    print("Original XML structure (first food item):")
    first_food = root.find('.//food')
    if first_food is not None:
        print(ET.tostring(first_food, encoding='unicode'))

    flattened_rows = []
    for food in root.findall('.//food'):
        row = {}

        name = food.find('name')
        row['name'] = name.text if name is not None else ''

        mfr = food.find('mfr')
        row['manufacturer'] = mfr.text if mfr is not None else ''

        serving = food.find('serving')
        row['serving_value'] = serving.text if serving is not None else ''
        row['serving_units'] = serving.get('units', '') if serving is not None else ''

        calories = food.find('calories')
        if calories is not None:
            row['calories_total'] = calories.get('total', '')
            row['calories_fat'] = calories.get('fat', '')
        else:
            row['calories_total'] = ''
            row['calories_fat'] = ''

        row['total_fat'] = food.find('total-fat').text if food.find('total-fat') is not None else ''
        row['saturated_fat'] = food.find('saturated-fat').text if food.find('saturated-fat') is not None else ''
        row['cholesterol'] = food.find('cholesterol').text if food.find('cholesterol') is not None else ''
        row['sodium'] = food.find('sodium').text if food.find('sodium') is not None else ''
        row['carb'] = food.find('carb').text if food.find('carb') is not None else ''
        row['fiber'] = food.find('fiber').text if food.find('fiber') is not None else ''
        row['protein'] = food.find('protein').text if food.find('protein') is not None else ''

        vitamins = food.find('vitamins')
        if vitamins is not None:
            row['vitamin_a'] = vitamins.find('a').text if vitamins.find('a') is not None else ''
            row['vitamin_c'] = vitamins.find('c').text if vitamins.find('c') is not None else ''
        else:
            row['vitamin_a'] = ''
            row['vitamin_c'] = ''

        minerals = food.find('minerals')
        if minerals is not None:
            row['calcium'] = minerals.find('ca').text if minerals.find('ca') is not None else ''
            row['iron'] = minerals.find('fe').text if minerals.find('fe') is not None else ''
        else:
            row['calcium'] = ''
            row['iron'] = ''

        flattened_rows.append(row)

    print(f"\nFlattened {len(flattened_rows)} rows from XML structure")
    print("Sample row:")
    print(flattened_rows[0] if flattened_rows else "No data")

    parsed_path = OUTPUT_DIR / 'nutrition_parsed.json'
    with open(parsed_path, 'w', encoding='utf-8') as f:
        json.dump(flattened_rows, f, indent=2)

    print(f"Parsed data saved to {parsed_path}")
    return len(flattened_rows)

def get_db_connection():
    return psycopg2.connect(**DB_CONN)


def init_nutrition_table():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        DROP TABLE IF EXISTS nutrition_linear;
        CREATE TABLE nutrition_linear (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            manufacturer VARCHAR(255),
            serving_value VARCHAR(50),
            serving_units VARCHAR(50),
            calories_total VARCHAR(50),
            calories_fat VARCHAR(50),
            total_fat VARCHAR(50),
            saturated_fat VARCHAR(50),
            cholesterol VARCHAR(50),
            sodium VARCHAR(50),
            carb VARCHAR(50),
            fiber VARCHAR(50),
            protein VARCHAR(50),
            vitamin_a VARCHAR(50),
            vitamin_c VARCHAR(50),
            calcium VARCHAR(50),
            iron VARCHAR(50)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Table nutrition_linear created successfully")


def load_xml_to_database():
    parsed_path = OUTPUT_DIR / 'nutrition_parsed.json'

    with open(parsed_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Loading {len(data)} rows to database")

    init_nutrition_table()

    conn = get_db_connection()
    cur = conn.cursor()

    fields = ['name', 'manufacturer', 'serving_value', 'serving_units',
              'calories_total', 'calories_fat', 'total_fat', 'saturated_fat',
              'cholesterol', 'sodium', 'carb', 'fiber', 'protein',
              'vitamin_a', 'vitamin_c', 'calcium', 'iron']

    columns_sql = sql.SQL(', ').join([sql.Identifier(f) for f in fields])
    placeholders = sql.SQL(', ').join([sql.Placeholder()] * len(fields))
    query = sql.SQL('INSERT INTO nutrition_linear ({}) VALUES ({})').format(columns_sql, placeholders)

    values_list = []
    for row in data:
        values_list.append(tuple(row[f] for f in fields))

    cur.executemany(query, values_list)
    conn.commit()
    cur.close()
    conn.close()

    print(f"Successfully loaded {len(data)} rows to nutrition_linear table")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM nutrition_linear')
    count = cur.fetchone()[0]

    cur.execute('SELECT name, manufacturer FROM nutrition_linear LIMIT 3')
    samples = cur.fetchall()
    print(f"Sample data: {samples}")

    cur.close()
    conn.close()

    print(f"Verified: {count} rows in nutrition_linear table")
    return count

with DAG(
    'xml_etl_dag',
    default_args=default_args,
    description='XML pipeline: download -> parse -> load to db',
    schedule_interval=None,
    catchup=False,
    tags=['task_01', 'xml', 'etl', 'unified'],
) as dag:

    download_task = PythonOperator(
        task_id='download_xml',
        python_callable=download_xml_file
    )

    parse_task = PythonOperator(
        task_id='parse_xml',
        python_callable=parse_xml_file
    )

    load_task = PythonOperator(
        task_id='load_xml_to_db',
        python_callable=load_xml_to_database
    )

    download_task >> parse_task >> load_task
