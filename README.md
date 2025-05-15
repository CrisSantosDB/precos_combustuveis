<div style="display: flex; justify-content: center; align-items: center; height: 100vh;">
  <h1 style="font-size: 48px;">⛽ Automatizando os dados da ANP com Airflow</h1>
</div>



<p align="center">
  <img src="https://github.com/CrisSantosDB/precos_combustuveis/blob/main/projeto_airflow.png?raw=true" width="500"/>
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

1. **Download automático** da planilha semanal publicada pela ANP  
2. **Tratamento dos dados** com scripts em Python, focando inicialmente na aba de capitais  
3. **Carga dos dados limpos** no banco de dados PostgreSQL  
4. (Opcional) Visualização no Power BI para análise e apresentação dos resultados

---

## Estrutura do projeto





