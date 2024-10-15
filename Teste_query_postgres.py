from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import logging

# Conexão com o PostgreSQL
postgres_conn_id = 'postgres_default'

default_args = {
    'owner': 'matheus',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='teste_postgres',
    start_date=datetime(2024, 9, 14),
    max_active_runs=1,
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=['teste', 'postgres']
)
def postgres_dag():
    def export_postgres_to_txt():
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM bloqueados")

        # Cria um arquivo CSV e escreve o resultado da consulta
        with open("/opt/airflow/dags/test_file_matheus.txt", "w", newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([i[0] for i in cursor.description])  # Cabeçalhos
            csv_writer.writerows(cursor.fetchall())  # Dados

        cursor.close()
        conn.close()
        logging.info("Saved teste_postgres.txt")

    task1 = PythonOperator(
        task_id='export_postgres_to_txt',
        python_callable=export_postgres_to_txt
    )

dag = postgres_dag()