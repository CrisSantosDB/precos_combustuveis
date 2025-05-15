<p align="center">
  <img src="https://raw.githubusercontent.com/CrisSantosDB/precos_combustuveis/main/projeto_preco_combustivel.png" width="800"/>
</p>






# Análise de Preços de Combustíveis - Pipeline Automatizado

Este é um projeto pessoal de Engenharia de Dados que automatiza a coleta, tratamento e armazenamento dos dados semanais de preços de combustíveis publicados pela **Agência Nacional do Petróleo (ANP)**.

---

## Motivação

Os preços dos combustíveis no Brasil sofrem variações constantes, impactando o bolso do consumidor e a logística das empresas. Este projeto tem como objetivo construir um pipeline de dados automatizado para facilitar a análise dessas variações e gerar insights relevantes a partir de dados públicos oficiais.



---

## Tecnologias usadas

- **Apache Airflow (com Astro CLI):** Orquestração do pipeline de dados para automação do processo semanal  
- **Python:** Tratamento e limpeza dos dados brutos  
- **PostgreSQL:** Banco de dados relacional para armazenar os dados tratados  
- **Power BI:** Visualização simples para apresentar insights 

---

## Como funciona o pipeline

1. Dag dowload_arquivo.py para **Download automático** da planilha semanal publicada pela ANP  e  **Tratamento dos dados** 
2. Dag inserir_dados.py **Cria tabela e carga dos dados limpos** no banco de dados PostgreSQL
3. Agendamento via DAG no Apache Airflow

---




## 📊 Análise dos Preços de Revenda

Ao analisar os preços médios de revenda nas últimas quatro semanas, cheguei a alguns insights interessantes a partir dos dados tratados:

### 🔎 Principais descobertas:

- 📈 **GLP (gás de cozinha)** apresenta um **desvio padrão de R$ 713,48** e um **coeficiente de variação de 646,30%**.  
  Isso indica que ele oscila proporcionalmente muito mais do que os demais combustíveis (gasolina, etanol e diesel).

- 🗺️ **Roraima, Amazonas e Tocantins** estão entre os estados com os **maiores preços de revenda**, destacando diferenças regionais significativas no custo dos combustíveis.

### 🔍 Visualização no Power BI

*As visualizações abaixo foram geradas para ilustrar os dados processados pelo pipeline:*

<p align="center">
  <img src="https://raw.githubusercontent.com/CrisSantosDB/precos_combustuveis/main/visualizacao.png" width="1000"/>
</p>

---









