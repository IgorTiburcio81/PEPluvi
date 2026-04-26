# 🌧️ PEPluvi

> Pipeline de dados pluviométricos de Pernambuco — histórico desde 1961.  
> Fonte: [APAC — Agência Pernambucana de Águas e Clima](https://www.apac.pe.gov.br)

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.11+-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)
![Airflow](https://img.shields.io/badge/Airflow-orquestração-017CEE)

---

## Sobre o projeto

O **PEPluvi** coleta dados de precipitação dos **352 pluviômetros** da APAC, cobrindo todas as **5 mesorregiões** de Pernambuco, desde 1961 até hoje.

O pipeline faz scraping do site da APAC via Selenium, valida a integridade dos CSVs, ingere os dados em um banco DuckDB local e é **orquestrado diariamente pelo Apache Airflow** (via Astro CLI).

---

## O que já funciona

| Etapa | Script | Descrição |
|---|---|---|
| **Scraping** | `include/pipeline/extract/scraping_apac.py` | Coleta automatizada do site da APAC via Selenium, por mesorregião e ano |
| **Validação** | `include/pipeline/extract/valid_data.py` | Verifica se o ano no nome do CSV bate com o conteúdo interno |
| **Ingestão** | `include/pipeline/load/ingest_duckdb.py` | Lê os CSVs, faz unpivot dia→linha e carrega no DuckDB |
| **Orquestração** | `dags/pipeline_pepluvi.py` | DAG Airflow carga incremental diária (D-1) às 06h UTC |

---

## Arquitetura do pipeline

```
Airflow DAG (diária, 06h UTC)
│
├─ 1. limpa_csv          → Remove CSVs do ano corrente
├─ 2. scraping            → Coleta dados atualizados da APAC
├─ 3. validacao           → Valida integridade dos CSVs baixados
├─ 4. limpeza_duckdb      → Remove registros do ano corrente no DuckDB
└─ 5. ingestao_duckdb     → Re-ingere os dados limpos no DuckDB
```

---

## Setup

### Pré-requisitos

- Python 3.11+
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (para rodar o Airflow localmente)
- Docker (necessário pelo Astro CLI)
- Google Chrome (para o Selenium no scraping)

### Instalação

```bash
# Clone o repositório
git clone https://github.com/IgorTiburcio81/PEPluvi.git
cd PEPluvi

# Crie o ambiente virtual e instale as dependências
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

### Execução manual (sem Airflow)

```bash
# 1. Coletar dados da APAC
make extract

# 2. Validar os CSVs
python include/pipeline/extract/valid_data.py

# 3. Ingerir no DuckDB (carga completa)
make load

# 3b. Ingerir apenas um ano específico (carga incremental)
python include/pipeline/load/ingest_duckdb.py 2026
```

> ⚠️ A carga histórica completa (1961 → hoje, todas as mesorregiões) leva várias horas. O scraper salva um CSV por ano/mesorregião em `include/data/raw/`, então se cair, basta rodar de novo, os já coletados são pulados.

### Execução orquestrada (Airflow)

Após subir o Airflow com `astro dev start`, a DAG `pipeline_pepluvi` roda automaticamente todos os dias às **06h UTC**, executando a carga incremental do ano corrente.

O banco é criado/atualizado em `include/data/pepluvi.duckdb`.

---

## Estrutura do repositório

```
PEPluvi/
├── dags/
│   └── pipeline_pepluvi.py       # DAG Airflow (carga incremental diária)
├── docs/                         # ADRs e Runbook
├── include/
│   ├── config/                   # constantes (settings.py)
│   ├── data/                     # ⚠️ NÃO versionado (.gitignore)
│   │   ├── raw/                  # CSVs brutos por mesorregião/ano
│   │   └── pepluvi.duckdb        # banco OLAP local
│   └── pipeline/
│       ├── extract/
│       │   ├── scraping_apac.py  # scraper Selenium
│       │   └── valid_data.py     # validação dos CSVs
│       └── load/
│           └── ingest_duckdb.py  # ETL CSVs → DuckDB
├── transform/                    # modelagem dbt (Silver → Gold)
├── Makefile                      # atalhos de execução
├── pyproject.toml                # dependências e linting (Ruff)
├── Dockerfile                    # imagem customizada (Chrome p/ Selenium)
├── airflow_settings.yaml         # configuração local do Airflow
├── packages.txt                  # pacotes apt do container Astro
├── .dockerignore
├── .gitignore
├── requirements.txt
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
- [DuckDB](https://duckdb.org)
- [Selenium](https://www.selenium.dev/documentation/)
- [Astronomer (Astro CLI)](https://www.astronomer.io/docs/astro/cli/overview)
- [Apache Airflow](https://airflow.apache.org/docs/)

---

*Projeto: PEPluvi — Igor Tiburcio · Iniciado em abril de 2026*
