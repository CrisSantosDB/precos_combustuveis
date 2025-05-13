from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
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
    'download_arquivo',
    description='Essa DAG baixa semanalmente os preços de combustíveis da ANP',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 0 * * 6',  # Executa todo sábado à meia-noite
    start_date=datetime(2024, 1, 1)  # Evita execução retroativa
)

# Função para baixar o arquivo
def download_excel(**context):
    today = datetime.today().date()
    max_attempts = 3

    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)

    for i in range(max_attempts):
        end_date = last_saturday - timedelta(weeks=i)
        start_date = end_date - timedelta(days=6)

        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        url = f"https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/arquivos-lpc/2025/resumo_semanal_lpc_{start_str}_{end_str}.xlsx"
        file_name = f"/usr/local/airflow/data/resumo_semanal_lpc_{start_str}_{end_str}.xlsx"

        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        response = requests.get(url)

        if response.status_code == 200:
            with open(file_name, 'wb') as f:
                f.write(response.content)
            print(f"✅ Arquivo baixado: {file_name}")
            # Retorna o caminho via XCom para a próxima task
            context['ti'].xcom_push(key='caminho_arquivo', value=file_name)
            return
        else:
            print(f"❌ Falha ao baixar o arquivo {url}, tentando a semana anterior...")

    print("⚠️ Nenhum arquivo encontrado nas últimas semanas.")
    context['ti'].xcom_push(key='caminho_arquivo', value=None)

# Função para limpar e salvar a aba "CAPITAIS"
def limpa_tabela(ti):
    caminho_arquivo = ti.xcom_pull(task_ids='download_task', key='caminho_arquivo')
    if caminho_arquivo:
        try:
            # Abre o arquivo Excel e lê a aba 'CAPITAIS'
            df = pd.read_excel(caminho_arquivo, sheet_name='CAPITAIS', skiprows=9, engine='openpyxl')
            print("📊 Primeiras linhas da planilha tratada:")
            print(df.head())

            # Converte as colunas de data para formato de string sem hora
            for column in df.select_dtypes(include=['datetime']):
                df[column] = df[column].dt.strftime('%Y-%m-%d')

            # Salva somente a aba 'CAPITAIS' no mesmo arquivo
            with pd.ExcelWriter(caminho_arquivo, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='CAPITAIS', index=False)

            print(f"✅ Planilha 'CAPITAIS' salva com sucesso em: {caminho_arquivo}")
        except Exception as e:
            print(f"❌ Erro ao processar o arquivo: {e}")
    else:
        print("⚠️ Nenhum arquivo para processar.")

# Task de download
download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_excel,
    provide_context=True,
    dag=dag
)

# Task de leitura e limpeza
limpa_tabela_task = PythonOperator(
    task_id='limpa_tabela_task',
    python_callable=limpa_tabela,
    dag=dag
)

# Dependência entre tasks
download_task >> limpa_tabela_task
