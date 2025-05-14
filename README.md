
# ⛽ Automatizando os dados da ANP com Airflow

<p align="center">
  <img src="https://github.com/CrisSantosDB/precos_combustuveis/blob/main/projeto_airflow.png?raw=true" width="500"/>
</p>


Toda semana, a ANP publica uma planilha com os preços dos combustíveis. Eu criei esse projeto pra automatizar esse processo usando o Apache Airflow com Astro CLI.

A ideia é simples:

- Faço o download automático do arquivo
- Trato os dados com Python
- E carrego tudo no banco de dados PostgreSQL

📌 Comecei focando só na aba de **capitais**, que já traz uma boa visão geral. As outras abas ainda não estão no pipeline, mas posso adicionar depois conforme a necessidade.

Esse projeto me ajudou a aprender mais sobre **orquestração de pipelines** e como trabalhar com **dados reais direto da fonte**.




