
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
    )

    
    # Criação da tabela
create_table_dl = """ create table if not exist preco_combustivel(
PRECO_COMBUSTIVEL_ID INT IDENTITY (1,1) PRIMARY KEY,
DATA INICIAL date, 
DATA FINAL date, 
ESTADO text, 
MUNICÍPIO text, 
PRODUTO text,
NÚMERO DE POSTOS PESQUISADOS int,
UNIDADE DE MEDIDA text,
PREÇO MÉDIO REVENDA real, 
DESVIO PADRÃO REVENDA real,
PREÇO MÍNIMO REVENDA real,
PREÇO MÁXIMO REVENDA real,
COEF DE VARIAÇÃO REVENDA real,
DATA_PROCESSAMENTO timestamp)
"""
create_table_task = PostgresOperator(task_id='create_table_task',
                                     postgres_conn_id='postgres',
                                     sql=create_table_dl
                                     dag=dag,
                                     )


    
    
    
    # Encadeamento
    #file_sensor_task >> get_data_task
