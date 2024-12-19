from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Definisi default_args
default_args = {
    'owner': 'kelompok2',
    'start_date': datetime(2024, 12, 15),  # Ganti sesuai kebutuhan
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definisi DAG
with DAG(
    'goat_sales_analysis',
    default_args=default_args,
    description='DAG untuk analisis penjualan kambing',
    schedule_interval= timedelta(days= 1),  # Menjalankan secara manual atau atur jadwal sesuai kebutuhan
) as dag:

    # Task 1: Analisis Pola Penjualan
    analysis_task_1 = BashOperator(
        task_id='analysis_task_1',
        bash_command='spark-submit /home/hadoop/airflow/dags/analyze_goat.py',  # Ganti dengan path yang benar
    )

    # Task 2: Analisis Tren Penjualan per Bulan Tahun 2024
    analysis_task_2 = BashOperator(
        task_id='analysis_task_2',
        bash_command='spark-submit /home/hadoop/airflow/dags/analyze_goat.py',  # Ganti dengan path yang benar
    )

    # Task 3: Analisis Rata-rata Harga Kambing
    analysis_task_3 = BashOperator(
        task_id='analysis_task_3',
        bash_command='spark-submit /home/hadoop/airflow/dags/analyze_goat.py',  # Ganti dengan path yang benar
    )

    # Menentukan urutan task
    analysis_task_1 >> analysis_task_2 >> analysis_task_3