
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from airflow.sensors.python import PythonSensor
from airflow.models import Variable
import glob
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Argumentos padrão
default_args = {
    'owner': 'Cristina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

# Definindo a DAG
dag = DAG(
    'levar_dados',
    description='Essa DAG levar dados os preços de combustíveis da ANP para Postgres',
    default_args=default_args,
    catchup=False,
    schedule_interval='10 0 * * 6',  # todo sábado às 00:10
    start_date=datetime(2024, 1, 1),
) 

# Task 1: sensor que espera qualquer .xlsx na pasta
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

# Task 2: lê o Excel e envia dados via XCom
def process_excel(**kwargs):
        path = Variable.get("path_file")
        files = glob.glob(os.path.join(path, "*.xlsx"))

        if not files:
            raise FileNotFoundError("Nenhum arquivo Excel encontrado.")

        latest_file = max(files, key=os.path.getctime)
        df = pd.read_excel(latest_file)

        print(f"Lendo: {latest_file}")
        print(df.head())

        primeira_linha = df.iloc[0].to_dict()

        ti = kwargs['ti']
        for k, v in primeira_linha.items():
            ti.xcom_push(key=k, value=v)

        os.remove(latest_file)
        print(f"Arquivo deletado: {latest_file}")

get_data_task = PythonOperator(
        task_id='get_data_task',
        python_callable=process_excel,
        provide_context=True,
        dag=dag
    )

    
    # Criação da tabela
create_table_dl = """ CREATE TABLE IF NOT EXISTS preco_combustivel (
    preco_combustivel_id SERIAL PRIMARY KEY,
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
    data_processamento TIMESTAMP)
"""
create_table_task = PostgresOperator(task_id='create_table_task',
                                     postgres_conn_id='postgres',
                                     sql=create_table_dl,
                                     dag=dag
                                     )



insert_sql = """ 
INSERT INTO preco_combustivel (
    data_inicial, 
    data_final, 
    estado, 
    municipio, 
    produto,
    numero_de_postos_pesquisados,
    unidade_de_medida,
    preco_medio_revenda, 
    desvio_padrao_revenda,
    preco_minimo_revenda,
    preco_maximo_revenda,
    coef_de_variacao_revenda,
    data_processamento
)
VALUES (
    '{{ ti.xcom_pull(key="DATA INICIAL") }}',
    '{{ ti.xcom_pull(key="DATA FINAL") }}',
    '{{ ti.xcom_pull(key="ESTADO") }}',
    '{{ ti.xcom_pull(key="MUNICÍPIO") }}',
    '{{ ti.xcom_pull(key="PRODUTO") }}',
    '{{ ti.xcom_pull(key="NÚMERO DE POSTOS PESQUISADOS") }}',
    '{{ ti.xcom_pull(key="UNIDADE DE MEDIDA") }}',
    '{{ ti.xcom_pull(key="PREÇO MÉDIO REVENDA") }}',
    '{{ ti.xcom_pull(key="DESVIO PADRÃO REVENDA") }}',
    '{{ ti.xcom_pull(key="PREÇO MÍNIMO REVENDA") }}',
    '{{ ti.xcom_pull(key="PREÇO MÁXIMO REVENDA") }}',
    '{{ ti.xcom_pull(key="COEF DE VARIAÇÃO REVENDA") }}',
    CURRENT_TIMESTAMP
);
"""

insert_data_task = PostgresOperator(task_id='insert_data_task',
                                    postgres_conn_id ='postgres',
                                    sql=insert_sql,
                                    dag=dag)
    
    
    # Encadeamento
file_sensor_task >> get_data_task >> create_table_task >> insert_data_task
