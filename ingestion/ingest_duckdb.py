import re
import pandas as pd
from pathlib import Path
import duckdb
from datetime import datetime
import numpy as np

# Configurações
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = PROJECT_ROOT / "data" / "raw"

DB_PATH = str(PROJECT_ROOT / "data" / "pepluvi.duckdb")

def tratar_mes_ano(mes_ano_str):
    meses = {
        'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
        'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12
    }
    try:
        partes = str(mes_ano_str).lower().split('/')
        return meses.get(partes[0]), int(partes[1])
    except:
        return None, None

def executar_etl():
    # Conecta (ou cria, se não existir) o arquivo do banco de dados
    conn = duckdb.connect(DB_PATH)
    
    arquivos = list(INPUT_DIR.glob("*.csv"))
    
    if not arquivos:
        print(f"Nenhum arquivo CSV encontrado em {INPUT_DIR}")
        return

    print(f"Processando {len(arquivos)} arquivos para o DuckDB...")

    tabela_criada = False

    for file_path in arquivos:
        try:
            # Extrai o ano esperado do nome do arquivo
            ano_match = re.search(r'_(\d{4})\.csv$', file_path.name)
            ano_esperado = int(ano_match.group(1)) if ano_match else None

            # Tenta ler com header na primeira linha
            df_raw = pd.read_csv(file_path, header=0)
            df_raw.columns = [str(c).strip() for c in df_raw.columns]

            # Se as colunas esperadas não estão na linha 0, tenta header=1
            if 'Código' not in df_raw.columns and 'Posto' not in df_raw.columns:
                df_raw = pd.read_csv(file_path, header=1)
                df_raw.columns = [str(c).strip() for c in df_raw.columns]

            # VALIDAÇÃO: Rejeita CSVs que não têm as colunas obrigatórias
            colunas_obrigatorias = ['Código', 'Posto', 'Mês/Ano']
            if not all(col in df_raw.columns for col in colunas_obrigatorias):
                print(f"  [SKIP] {file_path.name}: Colunas obrigatórias ausentes. Arquivo inválido.")
                continue

            # mesorregiao_id
            if 'mesorregiao_id' not in df_raw.columns:
                df_raw.rename(columns={df_raw.columns[-2]: 'mesorregiao_id'}, inplace=True)

            col_dias = [str(i).zfill(2) for i in range(1, 32)]
            col_dias_existentes = [c for c in col_dias if c in df_raw.columns]

            metadados = ['Código', 'Posto', 'Mês/Ano', 'mesorregiao_id']

            df_long = df_raw.melt(
                id_vars=metadados,
                value_vars=col_dias_existentes,
                var_name='dia',
                value_name='chuva'
            )

            # Limpeza de chuva ('-' = sem medição -> NaN, não zero)
            df_long['chuva'] = (df_long['chuva'].astype(str)
                                .str.strip()
                                .replace(['-', '', 'nan', 'None'], pd.NA)
                                .str.replace(',', '.', regex=False))
            df_long['chuva'] = pd.to_numeric(df_long['chuva'], errors='coerce')

            # Função de data robusta
            def formatar_data(row):
                try:
                    partes = str(row['Mês/Ano']).split('/')
                    mes_ext = partes[0].lower()
                    ano_str = partes[1]
                    
                    meses = {
                        'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
                        'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12
                    }
                    
                    return datetime(int(ano_str), meses[mes_ext], int(row['dia']))
                except:
                    return None

            df_long['data'] = df_long.apply(formatar_data, axis=1)
            df_final = df_long.dropna(subset=['data']).copy()

            # VALIDAÇÃO DO ANO
            if ano_esperado is not None:
                df_final = df_final[df_final['data'].dt.year == ano_esperado]
                if df_final.empty:
                    print(f"  [SKIP] {file_path.name}: Nenhum dado corresponde ao ano {ano_esperado}.")
                    continue

            # Renomear para o padrão do Banco
            df_to_db = df_final[['Código', 'Posto', 'data', 'chuva', 'mesorregiao_id']].rename(columns={
                'Código': 'codigo_posto',
                'Posto': 'nome_posto',
                'chuva': 'precipitacao'
            })

            
            # 1. Se a tabela ainda não existe, cria ela usando o DataFrame como molde 
            if not tabela_criada:
                conn.execute("CREATE TABLE IF NOT EXISTS monitoramento_pluviometrico AS SELECT * FROM df_to_db LIMIT 0")
                tabela_criada = True
            
            # 2. Insere os dados.
            conn.execute("INSERT INTO monitoramento_pluviometrico SELECT * FROM df_to_db")

            print(f"  [OK] Ingerido: {file_path.name} ({len(df_to_db)} linhas)")

        except Exception as e:
            print(f"  [ERRO] Falha no arquivo {file_path.name}: {e}")

    conn.close()

if __name__ == "__main__":
    executar_etl()
    print("\n Ingestão no DuckDB concluída com sucesso!")