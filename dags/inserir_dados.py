from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import glob
import pandas as pd
import psycopg2

# Argumentos padrão
default_args = {
    'owner': 'Cristina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

# DAG
dag = DAG(
    'inserir_dados_postgres',
    description='Essa DAG leva os dados de preços de combustíveis da ANP para o Postgres',
    default_args=default_args,
    catchup=False,
    schedule_interval='10 0 * * 6',  # Todo sábado às 00:10
    start_date=datetime(2024, 1, 1),
)

# Sensor: espera por arquivo Excel
def check_for_excel_file():
    path = Variable.get("path_file")
    files = glob.glob(os.path.join(path, "*.xlsx"))
    return len(files) > 0

file_sensor_task = PythonSensor(
    task_id='file_sensor_task',
    python_callable=check_for_excel_file,
    poke_interval=10,
    timeout=600,
    mode='poke',
    dag=dag
)

# Criação da tabela
create_table_sql = """ 
CREATE TABLE IF NOT EXISTS preco_combustivel (
    data_inicial DATE, 
    data_final DATE, 
    estado TEXT, 
    municipio TEXT, 
    produto TEXT,
    numero_de_postos_pesquisados INT,
    unidade_de_medida TEXT,
    preco_medio_revenda REAL, 
    desvio_padrao_revenda REAL,
    preco_minimo_revenda REAL,
    preco_maximo_revenda REAL,
    coef_de_variacao_revenda REAL,
    data_processamento TIMESTAMP,
    CONSTRAINT preco_combustivel_pk PRIMARY KEY (data_inicial, estado, municipio, produto)  -- Chave composta
);
"""


create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='postgres',
    sql=create_table_sql,
    dag=dag
)

# Processa o Excel e insere os dados
def process_and_insert(**kwargs):
    path = Variable.get("path_file")
    files = glob.glob(os.path.join(path, "*.xlsx"))

    if not files:
        raise FileNotFoundError("Nenhum arquivo Excel encontrado.")

    latest_file = max(files, key=os.path.getctime)
    df = pd.read_excel(latest_file, engine='openpyxl')

    print(f"Lendo arquivo: {latest_file}")
    print(df.head())

    # Renomear colunas
    df.columns = [
        col.replace(" ", "_").replace("É", "E").replace("Í", "I").replace("Ó", "O")
           .replace("Á", "A").replace("Ç", "C").replace("Ã", "A").replace("Ú", "U")
           .replace("Ê", "E").replace("Â", "A").upper()
        for col in df.columns
    ]

    # Conexão com o banco
    conn = BaseHook.get_connection("postgres")
    conn_string = f"host='{conn.host}' dbname='{conn.schema}' user='{conn.login}' password='{conn.password}'"
    with psycopg2.connect(conn_string) as pg_conn:
        cursor = pg_conn.cursor()

        for _, row in df.iterrows():
            values = tuple(row[col] for col in [
                'DATA_INICIAL', 'DATA_FINAL', 'ESTADO', 'MUNICIPIO', 'PRODUTO',
                'NUMERO_DE_POSTOS_PESQUISADOS', 'UNIDADE_DE_MEDIDA',
                'PRECO_MEDIO_REVENDA', 'DESVIO_PADRAO_REVENDA', 'PRECO_MINIMO_REVENDA',
                'PRECO_MAXIMO_REVENDA', 'COEF_DE_VARIACAO_REVENDA'
            ])

            insert_query = """
              INSERT INTO preco_combustivel (
              data_inicial, data_final, estado, municipio, produto,
              numero_de_postos_pesquisados, unidade_de_medida,
              preco_medio_revenda, desvio_padrao_revenda, preco_minimo_revenda,
              preco_maximo_revenda, coef_de_variacao_revenda,
              data_processamento
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (data_inicial, estado, municipio, produto) DO NOTHING
"""

            cursor.execute(insert_query, values)

        pg_conn.commit()

    os.remove(latest_file)
    print(f"Arquivo deletado: {latest_file}")

process_and_insert_task = PythonOperator(
    task_id='process_and_insert_task',
    python_callable=process_and_insert,
    dag=dag
)

# Encadeamento das tarefas
file_sensor_task >> create_table_task >> process_and_insert_task
