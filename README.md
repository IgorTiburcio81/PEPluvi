# 🌧️ PEPluvi

> Pipeline de dados pluviométricos de Pernambuco com análise histórica desde 1960.  
> Stack: Scrapy · Airflow · DuckDB · dbt · Metabase  
> Fonte: [APAC — Agência Pernambucana de Águas e Clima](https://www.apac.pe.gov.br)

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.11+-blue)
![Scrapy](https://img.shields.io/badge/Scrapy-scraping-green)
![dbt](https://img.shields.io/badge/dbt-Core-orange)
![DuckDB](https://img.shields.io/badge/DuckDB-OLAP-yellow)
![Metabase](https://img.shields.io/badge/Metabase-self--hosted-509EE3)

---

## 📋 Índice

- [Sobre o projeto](#sobre-o-projeto)
- [Arquitetura](#arquitetura)
- [Stack tecnológica](#stack-tecnológica)
- [Setup](#setup)
- [Como usar](#como-usar)
- [Dashboards](#dashboards)
- [Progresso do projeto](#progresso-do-projeto)

---

## Sobre o projeto

O **PEPluvi** é um pipeline de dados de ponta a ponta que coleta, transforma e visualiza dados de precipitação pluviométrica de Pernambuco a partir dos **352 pluviômetros** operados pela APAC, cobrindo **185 municípios** e todas as **7 mesorregiões** do estado.

O diferencial analítico do projeto está nas camadas Gold: comparação com o ano anterior, cálculo de médias históricas por período, regressão linear de tendência de longo prazo e ranking de eventos extremos — tudo em SQL puro, sem dependências externas de modelagem.

**O que o projeto entrega:**
- Histórico completo de precipitação desde 1960 por posto pluviométrico
- Carga incremental a cada 6h via Airflow
- Análise comparativa ano a ano e versus média histórica
- Detecção de tendência de longo prazo (aridização vs aumento de chuvas)
- Ranking de eventos extremos históricos por município e mesorregião
- Dashboards interativos com exportação CSV nativa via Metabase

---

## Arquitetura

```
APAC — apac.pe.gov.br
(tabelas HTML, sem download)
         │
         ▼
┌──────────────────────────────────┐
│  INGESTÃO — Scrapy + Airflow     │
│  • Carga histórica: 1960 → hoje  │
│  • Incremental a cada 6h         │
│  • Todos os postos e mesorregiões│
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  BRONZE — DuckDB                 │
│  • Dados brutos da APAC          │
│  • Schema raw preservado         │
│  • Particionado por ano/mês      │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  SILVER — dbt + dbt tests        │
│  • Limpeza e padronização        │
│  • Validação de faixas (mm)      │
│  • Enriquecimento geográfico     │
│    (posto → município → mesorr.) │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  GOLD — dbt                      │
│  • Acumulados diários/mensais    │
│  • Comparação ano anterior       │
│  • Média histórica por período   │
│  • Tendência de série temporal   │
│  • Ranking de eventos extremos   │
└──────────────┬───────────────────┘
               │
               ▼
┌──────────────────────────────────┐
│  METABASE                        │
│  • Dashboard: visão geral        │
│  • Mapa de calor de chuvas       │
│  • Comparativo histórico         │
│  • Análise por mesorregião       │
└──────────────────────────────────┘
```

---

## Stack tecnológica

| Camada | Ferramenta | Justificativa |
|---|---|---|
| Scraping | Scrapy | Robusto para scraping tabular, retry nativo, middleware de delay |
| Orquestração | Apache Airflow | DAGs agendadas, controle de dependências, logs de execução |
| Armazenamento | DuckDB | OLAP local, zero configuração, perfeito para séries temporais |
| Transformação | dbt Core | Modelos versionados, testes nativos, documentação automática |
| Qualidade | dbt tests + dbt-utils | Faixas de valores, nulidade, unicidade por posto + data |
| Visualização | Metabase (self-hosted) | Mapas, séries temporais, filtros interativos, open source |
| Seeds | dbt seeds | Tabelas de referência: postos, municípios, mesorregiões |

---

## Setup

### Pré-requisitos

- Python 3.11+
- Docker (para o Metabase)
- Git

### Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/pepluvi.git
cd pepluvi

# 2. Crie o ambiente virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Configure as variáveis de ambiente
cp .env.example .env
# Edite o .env com seus valores
```

### Variáveis de ambiente (`.env`)

```env
DUCKDB_PATH=data/pepluvi.duckdb
APAC_BASE_URL=http://old.apac.pe.gov.br/meteorologia/monitoramento-pluvio.php
SCRAPING_DELAY=2.0
```

---

## Como usar

### Carga histórica (primeira execução)

```bash
# Via Scrapy diretamente
scrapy crawl apac_historico

# Via Airflow (recomendado — controla progresso e dependências)
airflow dags trigger dag_historico_apac
```

> ⚠️ A carga histórica completa (1960 → hoje) leva aproximadamente 3 horas devido ao delay respeitoso de 2s entre requisições. O progresso é salvo no DuckDB a cada mês processado — se o scraper cair, retoma de onde parou.

### Carga incremental

```bash
# Execução manual
scrapy crawl apac_incremental

# Agendamento automático via Airflow (a cada 6 horas)
airflow dags unpause dag_incremental_apac
```

### Transformações dbt

```bash
cd dbt_pepluvi

# Carregar seeds de referência
dbt seed

# Rodar todos os modelos
dbt build

# Rodar por camada
dbt run --select silver
dbt run --select gold

# Rodar os testes
dbt test

# Ver a documentação
dbt docs generate && dbt docs serve
```

### Metabase

```bash
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

Acesse `http://localhost:3000` e configure a conexão com o arquivo `data/pepluvi.duckdb`.

---

## Dashboards

| Dashboard | Descrição |
|---|---|
| 🌧️ Visão Geral | Acumulado do mês, variação vs ano anterior e vs média histórica, mapa de calor por município |
| 📈 Comparativo Histórico | Série histórica por mesorregião, ranking de anos mais chuvosos, linha de tendência |
| 📅 Médias e Sazonalidade | Climatologia mensal, heatmap mês × mesorregião, comparativo entre regiões |
| ⚡ Eventos Extremos | Top 50 eventos históricos, mapa de postos com eventos intensos, frequência ao longo dos anos |

> 🔗 Links para os dashboards públicos serão adicionados após o deploy.

---

## Progresso do projeto

### ⚙️ Ambiente

- [ ] Repositório criado e configurado no GitHub
- [ ] Ambiente virtual Python configurado
- [ ] Dependências instaladas (`requirements.txt`)
- [ ] Arquivo `.env` configurado
- [ ] `.gitignore` com `data/`, `.env`, `.venv/`, `__pycache__/`

---

### 🔍 Investigação da fonte (APAC)

- [ ] Estrutura do formulário de monitoramento mapeada (GET ou POST)
- [ ] Parâmetros do formulário documentados
- [ ] Requisição manual validada com `curl` ou `requests`
- [ ] Códigos de todas as mesorregiões listados
- [ ] Lista de todos os postos pluviométricos obtida
- [ ] Verificação de bloqueio por User-Agent ou cookies
- [ ] Schema real da tabela HTML documentado
- [ ] `seeds/postos_apac.csv` criado com todos os postos e metadados

---

### 🕷️ Ingestão — Scrapy (Camada Bronze)

- [x] `settings.py` configurado com delays e headers respeitosos
- [x] `items.py` com o schema de campos
- [x] `apac_historico.py` implementado:
  - [x] Geração de janelas mensais (1960 → hoje)
  - [x] Parsing da tabela HTML
  - [x] Explosão das colunas de dias em linhas
  - [x] Tratamento de valores especiais (T, vazio, null)
- [x] `pipelines.py` com upsert no DuckDB por `(posto_codigo, data)`
- [ ] Teste do spider em janela pequena validado no DuckDB
- [ ] Carga histórica completa executada e monitorada
- [x] `apac_incremental.py` implementado com lógica de cursor
- [ ] Incremental testado manualmente antes de conectar no Airflow

---

### 🔄 Orquestração — Airflow

- [ ] Airflow instalado e banco inicializado (`airflow db init`)
- [ ] `dag_historico_apac.py` criada com as tasks de scraping + dbt
- [ ] `dag_incremental_apac.py` agendada a cada 6h (`0 */6 * * *`)
- [ ] DAG histórica executada com sucesso via Airflow UI
- [ ] DAG incremental testada via trigger manual
- [ ] DAG incremental ativada e monitorada na primeira execução automática
- [ ] Alertas de falha configurados

---

### 🥈 Transformação Silver (dbt)

- [ ] Projeto dbt inicializado (`dbt init dbt_pepluvi`)
- [ ] `profiles.yml` configurado para DuckDB local
- [ ] Seeds criados e carregados (`dbt seed`):
  - [ ] `postos_apac.csv` (código, nome, município, lat/long, altitude, ativo)
  - [ ] `municipios_pe.csv` (município, IBGE, mesorregião, microrregião, lat/long)
  - [ ] `mesorregioes_pe.csv` (nome, código, área km², descrição)
- [ ] Modelo `stg_chuvas_raw.sql` (staging Bronze)
- [ ] Modelo `dim_postos.sql` (join postos + municípios + mesorregiões)
- [ ] Modelo `dim_chuvas.sql`:
  - [ ] Cast de tipos e filtros
  - [ ] Deduplicação por `(posto_codigo, data)`
  - [ ] Flags `has_data` e `is_trace`
  - [ ] Campos derivados: `ano`, `mes`, `trimestre`, `estacao`, `is_periodo_chuvoso`
- [ ] `schema.yml` com testes de qualidade (unique_combination, accepted_range, not_null)
- [ ] `dbt-utils` instalado e adicionado ao `packages.yml`
- [ ] `dbt test` passando com 0 falhas
- [ ] `dbt docs` gerado e revisado
- [ ] Validação por amostragem:
  - [ ] ~352 postos distintos em `dim_postos`
  - [ ] ~185 municípios distintos em `dim_postos`
  - [ ] `MIN(data)` = 1960 e `MAX(data)` = hoje em `dim_chuvas`

---

### 🥇 Agregações Gold (dbt)

- [ ] Modelo `agg_chuvas_diarias.sql` (acumulados diários e mensais por posto)
- [ ] Modelo `agg_chuvas_mensais.sql` (total, dias com chuva, máximo diário por mês)
- [ ] Modelo `comparativo_ano_anterior.sql` ⭐ (variação mm e % vs mesmo mês ano anterior)
- [ ] Modelo `media_historica_periodo.sql` ⭐ (climatologia mensal com percentis 25–75)
- [ ] Modelo `tendencia_serie_temporal.sql` ⭐ (regressão linear em SQL puro por mesorregião)
- [ ] Modelo `ranking_eventos_extremos.sql` ⭐ (eventos acima do percentil 95 ou > 100mm/dia)
- [ ] `schema.yml` para modelos Gold com testes básicos
- [ ] `dbt build` completo (Silver + Gold) sem erros
- [ ] Validação por amostragem:
  - [ ] Comparativo: variações fazem sentido para anos conhecidos
  - [ ] Média histórica: cruzada com boletins da APAC
  - [ ] Tendência: slope matematicamente coerente com a série
  - [ ] Ranking: chuvas de 2022 em PE aparecem entre os maiores eventos

---

### 📊 Metabase (Dashboards)

- [ ] Metabase rodando via Docker
- [ ] Conexão com `pepluvi.duckdb` configurada
- [ ] Dashboard 1 — Visão Geral criado e validado com dados recentes
- [ ] Dashboard 2 — Comparativo Histórico criado e testado desde 1960
- [ ] Dashboard 3 — Médias e Sazonalidade criado e verificado
- [ ] Dashboard 4 — Eventos Extremos criado (conferir 2022 no topo)
- [ ] Exportação CSV configurada em todas as tabelas
- [ ] Dashboards publicados como públicos (sem login)
- [ ] Testado em dispositivo móvel
- [ ] Screenshots capturados para README e LinkedIn

---

### 🚀 Publicação

- [ ] README completo com screenshots dos dashboards
- [ ] `ARCHITECTURE.md` com decisões técnicas (Scrapy vs Playwright, DuckDB vs PostgreSQL, regressão em SQL)
- [ ] Links para dashboards públicos adicionados ao README
- [ ] Post no LinkedIn publicado
- [ ] Projeto adicionado ao portfólio

---

## Estrutura do repositório

```
pepluvi/
├── scraping/
│   ├── spiders/
│   │   ├── apac_historico.py       # histórico completo (1960 → hoje)
│   │   └── apac_incremental.py     # dados das últimas 6h
│   ├── middlewares.py              # delay, retry, headers
│   ├── pipelines.py                # upsert no DuckDB
│   ├── items.py                    # schema do item scraped
│   └── settings.py                 # configurações Scrapy
├── airflow/
│   └── dags/
│       ├── dag_historico_apac.py
│       └── dag_incremental_apac.py
├── dbt_pepluvi/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── seeds/
│   │   ├── postos_apac.csv
│   │   ├── municipios_pe.csv
│   │   └── mesorregioes_pe.csv
│   └── models/
│       ├── bronze/
│       ├── silver/
│       └── gold/
├── data/                           # não versionado
│   └── pepluvi.duckdb
├── .env.example
├── requirements.txt
└── README.md
```

---

## Referências

| Recurso | Link |
|---|---|
| APAC — Monitoramento Pluviométrico | http://old.apac.pe.gov.br/meteorologia/monitoramento-pluvio.php |
| APAC — Site principal | https://www.apac.pe.gov.br |
| Scrapy | https://docs.scrapy.org |
| scrapy-playwright (se necessário) | https://github.com/scrapy-plugins/scrapy-playwright |
| dbt-duckdb | https://github.com/duckdb/dbt-duckdb |
| dbt-utils | https://hub.getdbt.com/dbt-labs/dbt_utils/latest/ |
| Apache Airflow | https://airflow.apache.org/docs/ |
| Metabase Docker | https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker |
| Municípios de PE (IBGE) | https://www.ibge.gov.br/cidades-e-estados/pe.html |

---

*Projeto: PEPluvi — Igor Tiburcio · Iniciado em abril de 2026*
