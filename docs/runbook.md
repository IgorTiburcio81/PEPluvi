# Runbook Operacional - PEPluvi

## Visão Geral
Este documento descreve como operar, monitorar e solucionar problemas comuns do pipeline do PEPluvi. O pipeline utiliza uma arquitetura de Medallion simplificada com DuckDB.

## Pipeline Diário
O pipeline é orquestrado via Airflow (Astro CLI) e roda diariamente às **06:00 UTC**.
- **DAG:** `pipeline_pepluvi`
- **Fluxo:** `limpa_parquet` → `scraping` → `validacao` → `ingestao_duckdb`

## Procedimentos Comuns

### 1. Falha no Scraping (Selenium)
- **Sintoma:** Logs do Airflow mostram erros de `TimeoutException` ou `NoSuchElementException`.
- **Causa Comum:** Mudança na interface do site da APAC ou instabilidade no servidor deles.
- **Diagnóstico:**
    - Verifique `include/pipeline/extract/scraper.log`.
    - Tente rodar localmente: `python include/pipeline/extract/scraping_apac.py` (pode desativar o modo `headless` no script para ver o navegador).
- **Ação:** Atualizar seletores CSS/XPath no script de scraping se houver mudança no site.

### 2. Lock ou Erro de Permissão no DuckDB
- **Sintoma:** `IOException: IO Error: Cannot open file ... Permission denied`.
- **Causa:** Conflito de permissão entre o usuário local e o usuário do container Airflow (UID 50000).
- **Ação:** Execute `chmod 666 include/data/pepluvi.duckdb` e `chmod 777 include/data/`.
- **Bloqueio:** Certifique-se que o banco não está aberto em modo leitura/escrita por outra ferramenta (DBeaver, Python REPL) simultaneamente.

### 3. Recuperação de Dados (Backfill)
- **Problema:** Um ano específico está sem dados ou com dados errados no DuckDB.
- **Ação:**
    1. Delete o arquivo parquet correspondente em `include/data/raw/`.
    2. No script de scraping, você pode forçar a coleta de um ano alterando o loop `range` ou apenas deletando o arquivo e rodando a DAG (ela recriará o arquivo se ele não existir).
    3. Para re-ingerir manualmente: `python include/pipeline/load/ingest_duckdb.py <ANO>`.

### 4. Ingestão Atômica (Segurança)
O script `ingest_duckdb.py` agora possui lógica atômica:
- Ele **só deleta** os dados do ano no DuckDB se o arquivo Parquet for lido com sucesso e contiver registros.
- Isso evita que falhas no scraping apaguem os dados históricos que já estavam no banco.

## Monitoramento de Qualidade
- O script `valid_data.py` roda após o scraping para garantir que o conteúdo do arquivo Parquet corresponde ao ano esperado no nome do arquivo. Se falhar, a DAG para antes da ingestão.
