from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import glob
import pandas as pd

# Argumentos padrão
default_args = {
    'owner': 'Cristina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

# Definindo a DAG
dag = DAG(
    'inser_dados',
    description='Essa DAG leva os dados de preços de combustíveis da ANP para o Postgres',
    default_args=default_args,
    catchup=False,
    schedule_interval='10 0 * * 6',  # Todo sábado às 00:10
    start_date=datetime(2024, 1, 1),
)

# Task 1: Sensor que espera qualquer .xlsx na pasta
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

# Task 2: Lê o Excel e envia dados via XCom
def process_excel(**kwargs):
    path = Variable.get("path_file")
    files = glob.glob(os.path.join(path, "*.xlsx"))

    if not files:
        raise FileNotFoundError("Nenhum arquivo Excel encontrado.")

    latest_file = max(files, key=os.path.getctime)
    df = pd.read_excel(latest_file, engine='openpyxl')

    print(f"Lendo: {latest_file}")
    print(df.head())

    primeira_linha = df.iloc[0].to_dict()

    # Renomeia as chaves para evitar espaços e acentos
    nova_linha = {
        k.replace(" ", "_").replace("É", "E").replace("Í", "I").replace("Ó", "O")
         .replace("Á", "A").replace("Ç", "C").replace("Ã", "A").replace("Ú", "U")
         .replace("Ê", "E").replace("Â", "A").upper(): v
        for k, v in primeira_linha.items()
    }

    ti = kwargs['ti']
    for k, v in nova_linha.items():
        ti.xcom_push(key=k, value=v)

    os.remove(latest_file)
    print(f"Arquivo deletado: {latest_file}")

get_data_task = PythonOperator(
    task_id='get_data_task',
    python_callable=process_excel,
    dag=dag
)

# Criação da tabela
create_table_dl = """ 
CREATE TABLE IF NOT EXISTS preco_combustivel (
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
    data_processamento TIMESTAMP
);
"""

create_table_task = PostgresOperator(
    task_id='create_table_task',
    postgres_conn_id='postgres',
    sql=create_table_dl,
    dag=dag
)

# Inserção dos dados
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
    '{{ ti.xcom_pull(key="DATA_INICIAL") }}',
    '{{ ti.xcom_pull(key="DATA_FINAL") }}',
    '{{ ti.xcom_pull(key="ESTADO") }}',
    '{{ ti.xcom_pull(key="MUNICIPIO") }}',
    '{{ ti.xcom_pull(key="PRODUTO") }}',
    '{{ ti.xcom_pull(key="NUMERO_DE_POSTOS_PESQUISADOS") }}',
    '{{ ti.xcom_pull(key="UNIDADE_DE_MEDIDA") }}',
    '{{ ti.xcom_pull(key="PRECO_MEDIO_REVENDA") }}',
    '{{ ti.xcom_pull(key="DESVIO_PADRAO_REVENDA") }}',
    '{{ ti.xcom_pull(key="PRECO_MINIMO_REVENDA") }}',
    '{{ ti.xcom_pull(key="PRECO_MAXIMO_REVENDA") }}',
    '{{ ti.xcom_pull(key="COEF_DE_VARIACAO_REVENDA") }}',
    CURRENT_TIMESTAMP
);
"""

insert_data_task = PostgresOperator(
    task_id='insert_data_task',
    postgres_conn_id='postgres',
    sql=insert_sql,
    dag=dag
)

# Encadeamento das tarefas
file_sensor_task >> get_data_task >> create_table_task >> insert_data_task
