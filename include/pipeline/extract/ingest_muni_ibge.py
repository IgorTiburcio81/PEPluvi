import json
import sys
import os
import urllib.request
import duckdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.settings import URL_API_IBGE, DB_PATH

TARGET_TABLE = "bronze.ibge_municipios_pe"


import gzip

def fetch_municipios(url: str) -> list[dict]:
    try:
        request = urllib.request.Request(url)
        request.add_header('Accept-Encoding', 'gzip')
        with urllib.request.urlopen(request) as response:
            if response.status != 200:
                print(f"Erro HTTP {response.status}", file=sys.stderr)
                sys.exit(1)
            
            raw_data = response.read()
            # Se a resposta vier compactada (GZIP), descompacta antes de decodificar
            if response.info().get('Content-Encoding') == 'gzip' or raw_data.startswith(b'\x1f\x8b'):
                raw_data = gzip.decompress(raw_data)
                
            return json.loads(raw_data.decode("utf-8"))
    except Exception as e:
        print(f"Falha ao acessar a API: {e}", file=sys.stderr)
        sys.exit(1)


def parse_municipios(data: list[dict]) -> list[dict]:
    municipios = []
    for mun in data:
        # A API retorna: id, nome, microrregiao { nome, mesorregiao { nome } }
        codigo_ibge = mun["id"]
        nome = mun["nome"]
        microrregiao = mun["microrregiao"]["nome"]
        mesorregiao = mun["microrregiao"]["mesorregiao"]["nome"]
        municipios.append({
            "codigo_ibge": codigo_ibge,
            "municipio": nome,
            "mesorregiao": mesorregiao,
            "microrregiao": microrregiao
        })
    return municipios


def load_to_duckdb(municipios: list[dict], db_path: str, table_name: str) -> None:
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} (
                codigo_ibge INTEGER,
                municipio VARCHAR,
                mesorregiao VARCHAR,
                microrregiao VARCHAR
            )
        """)

        valores = [
            (mun["codigo_ibge"], mun["municipio"], mun["mesorregiao"], mun["microrregiao"])
            for mun in municipios
        ]

        conn.executemany(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?)", valores)
        conn.close()

        print(f"Dados inseridos com sucesso em {table_name}.")
        print(f"Total de municípios carregados: {len(municipios)}")

    except Exception as e:
        print(f"Erro ao carregar no DuckDB: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    print("Buscando municípios de Pernambuco na API do IBGE...")
    data = fetch_municipios(URL_API_IBGE)
    municipios = parse_municipios(data)
    load_to_duckdb(municipios, DB_PATH, TARGET_TABLE)


if __name__ == "__main__":
    main()