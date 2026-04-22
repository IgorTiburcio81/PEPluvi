# 🌧️ PEPluvi

> Pipeline de dados pluviométricos de Pernambuco — histórico desde 1961.  
> Fonte: [APAC — Agência Pernambucana de Águas e Clima](https://www.apac.pe.gov.br)

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.11+-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)

---

## Sobre o projeto

O **PEPluvi** coleta dados de precipitação dos **352 pluviômetros** da APAC, cobrindo todas as **5 mesorregiões** de Pernambuco, desde 1961 até hoje.

O pipeline atual faz scraping do site da APAC via Selenium, valida a integridade dos CSVs e ingere os dados em um banco DuckDB local para análise.

---

## O que já funciona

| Etapa | Script | Descrição |
|---|---|---|
| **Scraping** | `scraping/scraping_apac.py` | Coleta automatizada do site da APAC via Selenium, por mesorregião e ano |
| **Validação** | `scraping/valid_data.py` | Verifica se o ano no nome do CSV bate com o conteúdo interno |
| **Ingestão** | `ingestion/ingest_duckdb.py` | Lê os CSVs, faz unpivot dia→linha e carrega no DuckDB |

---

## Setup

```bash
# Clone o repositório
git clone https://github.com/IgorTiburcio81/PEPluvi.git
cd PEPluvi

# Crie o ambiente virtual e instale as dependências
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Como usar

### 1. Coletar dados da APAC

```bash
python scraping/scraping_apac.py
```

> ⚠️ A carga histórica completa (1961 → hoje, todas as mesorregiões) leva várias horas. O scraper salva um CSV por ano/mesorregião em `data/raw/`, então se cair, basta rodar de novo — os já coletados são pulados.

### 2. Validar os CSVs

```bash
python scraping/valid_data.py
```

### 3. Ingerir no DuckDB

```bash
python ingestion/ingest_duckdb.py
```

O banco é criado em `data/pepluvi.duckdb`.

---

## Estrutura do repositório

```
PEPluvi/
├── scraping/
│   ├── scraping_apac.py        # scraper Selenium
│   ├── valid_data.py           # validação dos CSVs
│   └── scraper.log             # log de execução
├── ingestion/
│   └── ingest_duckdb.py        # ETL CSVs → DuckDB
├── data/                       # ⚠️ NÃO versionado (.gitignore)
│   ├── raw/                    # CSVs brutos por mesorregião/ano
│   └── pepluvi.duckdb          # banco OLAP local
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Próximos passos

- **Transformação (dbt)** — Modelagem em camadas Bronze → Silver → Gold com testes de qualidade
- **Orquestração (Airflow)** — DAGs para carga incremental a cada 6h
- **Análises Gold** — Comparativo ano a ano, média histórica, tendência de longo prazo, ranking de eventos extremos
- **Dashboards (Metabase)** — Visualizações interativas com mapas e séries temporais

---

## Referências

- [APAC — Monitoramento Pluviométrico](http://old.apac.pe.gov.br/meteorologia/monitoramento-pluvio.php)
- [DuckDB](https://duckdb.org)
- [Selenium](https://www.selenium.dev/documentation/)

---

*Projeto: PEPluvi — Igor Tiburcio · Iniciado em abril de 2026*
