# 1. Escolha do DuckDB como Storage Local

Date: 2026-04-26

## Contexto
O projeto precisa de um repositório para os dados locais após a extração da APAC, facilitando consultas analíticas rápidas sem depender da carga do SQLite padrão do Airflow ou de um banco externo pesado.

## Decisão
Foi escolhido o **DuckDB** devido ao seu excelente desempenho analítico (OLAP), facilidade de integração nativa com Python e Pandas, e por ser serverless (baseado em arquivo único `pepluvi.duckdb`).

