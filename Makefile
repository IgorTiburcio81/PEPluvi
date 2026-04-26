.PHONY: help install lint extract load
.DEFAULT_GOAL := help

help:
	@echo "Comandos disponíveis:"
	@echo "  install      - Instala as dependências de desenvolvimento"
	@echo "  lint         - Roda ruff para linting"
	@echo "  extract      - Roda o scraper localmente"
	@echo "  load         - Roda a ingestão localmente"

install:
	pip install -r requirements.txt
	pip install ruff pre-commit
	pre-commit install

lint:
	ruff check .

extract:
	python pipeline/extract/scraping_apac.py

load:
	python pipeline/load/ingest_duckdb.py
