from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from cryptography.fernet import Fernet
import os
import psycopg2
from psycopg2.extras import execute_batch

# Encryption key: Save and reuse this securely!
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# File paths
BASE_DIR = '/home/hadoop/data/'
input_file = os.path.join(BASE_DIR, 'master_pembeli_kel2.csv')
output_file = os.path.join(BASE_DIR, 'Output_Masked_Encrypted_Master_Pembeli.csv')

# Database connection settings
db_url = 'postgresql://postgres:salsaaputri25@127.0.0.1:5432/msib_batch7'

# Function to mask phone numbers
def mask_phone_number(phone):
    try:
        return f"{'*' * (len(phone) - 4)}{phone[-4:]}"
    except Exception as e:
        return "Error"

# Function to encrypt address
def encrypt_address(address):
    try:
        return cipher_suite.encrypt(address.encode()).decode()
    except Exception as e:
        return "Error"

# Function to create the table if it does not exist
def create_table():
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    # SQL to create the processed_data table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS masterpembeli_data (
        id SERIAL PRIMARY KEY,
        nama VARCHAR(255),
        umur INTEGER,
        status VARCHAR(100),
        no_hp INTEGER,
        alamat VARCHAR(200)
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()

# Task: Process data (masking and encryption)
def process_data():
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    # Load data
    data = pd.read_csv(input_file, delimiter=';')

    # Mask phone numbers
    data['no_hp'] = data['no_hp'].astype(str).apply(mask_phone_number)

    # Encrypt addresses
    data['alamat'] = data['alamat'].astype(str).apply(encrypt_address)

    # Clean and validate the 'umur' column to ensure all values are valid integers
    data['umur'] = pd.to_numeric(data['umur'], errors='coerce')  # Coerce invalid values to NaN
    data['umur'] = data['umur'].fillna(0).astype(int)  # Replace NaN with 0 and convert to integer

    # Save processed data to CSV
    data.to_csv(output_file, index=False)

    # Insert data into PostgreSQL using psycopg2
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    # Define the insert SQL statement
    insert_sql = """
    INSERT INTO masterpembeli_data (id, nama, umur, status, no_hp, alamat)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    # Convert DataFrame rows into a list of tuples for batch insert
    data_tuples = [tuple(x) for x in data[['id', 'nama', 'umur', 'status', 'no_hp', 'alamat']].values]

    # Perform the batch insert
    try:
        execute_batch(cursor, insert_sql, data_tuples)
        conn.commit()  # Commit changes after the batch insert
    except Exception as e:
        conn.rollback()  # Rollback in case of error
        print(f"Error inserting data: {e}")

    # Commit and close the connection
    cursor.close()
    conn.close()

# Define the DAG
default_args = {
    'owner': 'Kelompok2',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'mask_encrypt_data_master_pembeli',
    default_args=default_args,
    description='DAG for masking phone numbers and encrypting addresses',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# PythonOperator to create table if not exists
create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

# PythonOperator to execute data processing
data_processing_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

# Set task dependencies
create_table_task >> data_processing_task