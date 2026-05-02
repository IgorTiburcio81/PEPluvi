# 2. Migração para Apache Parquet como Formato Raw

Date: 2026-05-01

## Contexto
Originalmente, o projeto salvava os dados extraídos da APAC em arquivos CSV. Com o crescimento do volume de dados (milhões de linhas) e a necessidade de re-ingestões frequentes, o formato CSV apresentou limitações de performance e falta de metadados de tipos (strings vs floats).

## Decisão
Migrar o armazenamento da camada **Raw** (`include/data/raw/`) de CSV para **Apache Parquet**.

## Racional
1. **Compressão:** Arquivos Parquet ocupam significativamente menos espaço em disco que CSVs.
2. **Tipagem Nativa:** O Parquet preserva os tipos de dados (float64 para precipitação, int64 para IDs), eliminando a necessidade de casting manual pesado durante a leitura no Pandas/DuckDB.
3. **Performance de Leitura:** O DuckDB e o Pandas conseguem ler arquivos Parquet de forma muito mais rápida que CSVs devido ao armazenamento colunar.
4. **Esquema Bronze:** Aproveitamos a migração para organizar o DuckDB sob o schema `bronze`, seguindo as melhores práticas de arquitetura de medalhão.

## Consequências
- **Prós:**
    - Redução no tempo de ingestão no DuckDB.
    - Scripts de carga (`ingest_duckdb.py`) tornaram-se mais simples e menos propensos a erros de parsing.
    - Menor consumo de I/O no disco.
- **Contras:**
    - Arquivos não são mais legíveis em editores de texto simples (requer ferramentas compatíveis com Parquet).
    - Adição da dependência `pyarrow` ao projeto.
