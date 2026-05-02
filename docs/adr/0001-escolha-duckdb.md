# 1. Escolha do DuckDB como Storage Local

Date: 2026-04-26

## Contexto
O projeto precisa de um repositório para os dados locais após a extração da APAC, facilitando consultas analíticas rápidas sem depender da carga do SQLite padrão do Airflow ou de um banco externo pesado. Precisamos de uma solução que suporte processamento vetorial para lidar com milhões de registros de precipitação de forma eficiente em hardware modesto.

## Decisão
Foi escolhido o **DuckDB** devido ao seu excelente desempenho analítico (OLAP), facilidade de integração nativa com Python e Pandas, e por ser serverless (baseado em arquivo único `pepluvi.duckdb`).

## Consequências
- **Prós:**
    - Velocidade extrema em agregações temporais (ex: média de chuva por década).
    - Zero configuração de infraestrutura (apenas um arquivo local).
    - Integração direta com Parquet e CSV sem necessidade de buffers intermediários complexos.
    - Suporte a SQL padrão e extensões geoespaciais (futuro).
- **Contras:**
    - Permite apenas uma conexão de escrita por vez (limitação de banco baseado em arquivo).
    - Requer gerenciamento manual de concorrência se múltiplas DAGs tentarem escrever simultaneamente.
