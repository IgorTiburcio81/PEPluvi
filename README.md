# 🌧️ PEPluvi

> Pipeline de dados pluviométricos de Pernambuco — histórico (APAC) e tempo real (CEMADEN).  
> Fonte: [APAC — Agência Pernambucana de Águas e Clima](https://www.apac.pe.gov.br)

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.11+-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)
![Airflow](https://img.shields.io/badge/Airflow-orquestração-017CEE)

---

## Sobre o projeto

O **PEPluvi** coleta dados de precipitação dos pluviômetros de Pernambuco, integrando dados históricos desde 1961 e monitoramento em tempo real (atualizado a cada 15 minutos).

O pipeline utiliza Selenium para scraping histórico e Requests para consumo de APIs, validando a integridade dos arquivos Parquet e disponibilizando-os via DuckDB em uma arquitetura de medalhão.

---

## O que já funciona

| Etapa | Fluxo | Descrição |
|---|---|---|
| **Scraping Histórico** | `scraping_apac.py` | Coleta automatizada (Selenium) por mesorregião/ano. Salva em **Parquet**. |
| **API Real-time** | `dados_15min_apac.py` | Coleta dados CEMADEN a cada 15 min. Salva em **Parquet (Hive)**. |
| **Ingestão IBGE** | `ingest_muni_ibge.py` | Carga de metadados geográficos dos municípios. |
| **Ingestão Bronze** | `ingest_duckdb.py` | Carga física dos dados históricos no DuckDB. |
| **View Bronze** | `update_bronze_view` | Cria **VIEW dinâmica** para dados CEMADEN (sem duplicação). |
| **Orquestração** | Airflow | DAGs para carga diária (APAC) e a cada 15 min (CEMADEN). |

---

## Arquitetura do pipeline

### 1. Dados Históricos (APAC)
```
Airflow DAG (diária)
│
├─ 1. scraping           → Salva Parquet por ano/mesorregião
├─ 2. validacao          → Verifica integridade dos arquivos
└─ 3. ingestao_duckdb    → Carga atômica na tabela bronze.monitoramento_pluviometrico
```

### 2. Dados Real-time (CEMADEN)
```
Airflow DAG (15 em 15 min)
│
├─ 1. extrair_salvar_raw → Salva Parquet na Raw com partição Hive (ano/mes/dia)
└─ 2. atualizar_view     → Atualiza VIEW bronze.apac_15min_bronze (Zero Copy)
```

---

## Camada de dados (Medallion)

| Camada | Localização | Formato | Descrição |
|---|---|---|---|
| **Raw (APAC)** | `include/data/raw/*.parquet` | Parquet | Arquivos brutos por ano/região. |
| **Raw (API)** | `include/data/raw/api_cemaden/` | Parquet | Particionamento Hive: `ano=Y/mes=M/dia=D/`. |
| **Bronze (Hist)** | `bronze.monitoramento_pluviometrico` | DuckDB Table | Dados históricos carregados fisicamente. |
| **Bronze (15min)**| `bronze.apac_15min_bronze` | DuckDB **View** | View dinâmica sobre os arquivos Parquet da Raw. |

---

## Setup

### Pré-requisitos

- Python 3.11+
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (para Airflow local)
- Docker
- Google Chrome (para o Selenium no scraping histórico)

### Instalação

```bash
# Clone e entre no projeto
git clone https://github.com/IgorTiburcio81/PEPluvi.git
cd PEPluvi

# Setup do ambiente
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Subindo o Airflow (Astro CLI)

```bash
# Inicia os containers Docker do Airflow
astro dev start

# A UI do Airflow estará disponível em http://localhost:8080
# Login padrão: admin / admin
```

---

## Como usar

### Consultando a Camada Bronze (DuckDB)
O DuckDB permite consultar os Parquets da Raw e as tabelas da Bronze de forma unificada:

```sql
-- Consultar dados de 15 minutos (View dinâmica)
SELECT * FROM bronze.apac_15min_bronze LIMIT 10;

-- Consultar dados históricos
SELECT * FROM bronze.monitoramento_pluviometrico WHERE ano = 2024;
```

### Execução manual (sem Airflow)

```bash
# 1. Coletar dados da APAC (salva Parquets em include/data/raw/)
make extract

# 2. Validar os Parquets
python include/pipeline/extract/valid_data.py

# 3. Ingerir no DuckDB (carga completa)
make load

# 3b. Ingerir apenas um ano específico (carga incremental)
python include/pipeline/load/ingest_duckdb.py 2026
```

> A carga histórica completa (1961 → hoje, todas as mesorregiões) leva várias horas. O scraper salva um Parquet por ano/mesorregião em `include/data/raw/`, então se cair, basta rodar de novo — os já coletados são pulados automaticamente.

### Execução orquestrada (Airflow)

Após subir o Airflow com `astro dev start`, a DAG `pipeline_pepluvi` roda automaticamente todos os dias às **06h UTC**, executando a carga incremental do ano corrente.

O banco é criado/atualizado em `include/data/pepluvi.duckdb` no schema `bronze`.

---

## Estrutura do repositório

```
PEPluvi/
├── dags/
│   ├── pipeline_pepluvi.py       # DAG Airflow (carga incremental diária)
│   └── pipeline_15min_apac.py    # DAG Real-time (15 min)
├── docs/                         # ADRs e Runbook
├── include/
│   ├── config/
│   │   └── settings.py           # constantes de caminho e URL
│   ├── data/                     # NÃO versionado (.gitignore)
│   │   ├── raw/                  # Parquets brutos por mesorregião/ano e api_cemaden
│   │   └── pepluvi.duckdb        # banco OLAP local (schema: bronze)
│   └── pipeline/
│       ├── extract/
│       │   ├── scraping_apac.py  # scraper Selenium → salva Parquet
│       │   ├── dados_15min_apac.py # API CEMADEN → salva Parquet Hive
│       │   ├── ingest_muni_ibge.py # API IBGE → DuckDB
│       │   └── valid_data.py     # validação dos arquivos
│       └── load/
│           └── ingest_duckdb.py  # ETL Parquet → DuckDB bronze
├── transform/                    # modelagem dbt (Silver → Gold)
├── Makefile                      # atalhos de execução
├── pyproject.toml                # dependências e linting (Ruff)
├── Dockerfile                    # imagem customizada (Chrome p/ Selenium)
├── airflow_settings.yaml         # configuração local do Airflow
├── packages.txt                  # pacotes apt do container Astro
├── requirements.txt              # dependências Python (inclui pyarrow)
├── .gitignore
└── README.md
```

---

## Próximos passos

- **Transformação (dbt)** — Modelagem em camadas Bronze → Silver → Gold com testes de qualidade
- **Análises Gold** — Comparativo ano a ano, média histórica, tendência de longo prazo, ranking de eventos extremos
- **Dashboards (Metabase)** — Visualizações interativas com mapas e séries temporais

---

## Referências

- [APAC — Monitoramento Pluviométrico](http://old.apac.pe.gov.br/meteorologia/monitoramento-pluvio.php)
- [DuckDB - Parquet & Hive Partitioning](https://duckdb.org/docs/data/parquet/hive_partitioning)
- [Apache Parquet](https://parquet.apache.org/)
- [Selenium](https://www.selenium.dev/documentation/)
- [Astronomer (Astro CLI)](https://www.astronomer.io/docs/astro/cli/overview)
- [Apache Airflow](https://airflow.apache.org/docs/)

---

*Projeto: PEPluvi — Igor Tiburcio · Iniciado em abril de 2026*
