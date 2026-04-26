# Runbook Operacional - PEPluvi

## Visão Geral
Este documento descreve como operar, monitorar e solucionar problemas comuns do pipeline do PEPluvi.

## Pipeline Diario
O pipeline roda diariamente às 06:00 BRT pelo Airflow.

## Falha no Scraping (Selenium)
- **Problema:** Mudança na interface do site da APAC.
- **Ação:** Inspecionar `scraper.log` na pasta de logs do Airflow ou local. Testar rodar `make run-extract` localmente observando a tela (`headless=False`).

## Lock no DuckDB
- **Problema:** Múltiplos processos acessando `pepluvi.duckdb`.
- **Ação:** Certifique-se que nenhuma ferramenta como DBeaver está com a conexão aberta bloqueando a escrita.
