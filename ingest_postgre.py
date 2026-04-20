import re
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

# Configurações

INPUT_DIR = Path("dados_apac_recife/arquivos_anuais")

DB_URL = "postgresql://postgres:147852369@localhost:5432/apac_clima"

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
    engine = create_engine(DB_URL)
    arquivos = list(INPUT_DIR.glob("*.csv"))
    
    if not arquivos:
        print(f"Nenhum arquivo CSV encontrado em {INPUT_DIR}")
        return

    print(f"Processando {len(arquivos)} arquivos para o PostgreSQL...")

    for file_path in arquivos:
        try:
            # Extrai o ano esperado do nome do arquivo (ex: Mata_Pernambucana_1961.csv -> 1961)
            ano_match = re.search(r'_(\d{4})\.csv$', file_path.name)
            ano_esperado = int(ano_match.group(1)) if ano_match else None

            # Tenta ler com header na primeira linha (formato do scraper corrigido)
            df_raw = pd.read_csv(file_path, header=0)
            df_raw.columns = [str(c).strip() for c in df_raw.columns]

            # Se as colunas esperadas não estão na linha 0, tenta header=1 (formato antigo)
            if 'Código' not in df_raw.columns and 'Posto' not in df_raw.columns:
                df_raw = pd.read_csv(file_path, header=1)
                df_raw.columns = [str(c).strip() for c in df_raw.columns]

            # VALIDAÇÃO: Rejeita CSVs que não têm as colunas obrigatórias (lixo/HTML)
            colunas_obrigatorias = ['Código', 'Posto', 'Mês/Ano']
            if not all(col in df_raw.columns for col in colunas_obrigatorias):
                print(f"  [SKIP] {file_path.name}: Colunas obrigatórias ausentes. Arquivo inválido.")
                continue

            # --- LOGICA PARA ENCONTRAR mesorregiao_id ---
            if 'mesorregiao_id' not in df_raw.columns:
                df_raw.rename(columns={df_raw.columns[-2]: 'mesorregiao_id'}, inplace=True)

            # Colunas de dias: de '01' até '31'
            col_dias = [str(i).zfill(2) for i in range(1, 32)]
            col_dias_existentes = [c for c in col_dias if c in df_raw.columns]

            # Metadados obrigatórios
            metadados = ['Código', 'Posto', 'Mês/Ano', 'mesorregiao_id']

            # UNPIVOT (Melt)
            df_long = df_raw.melt(
                id_vars=metadados,
                value_vars=col_dias_existentes,
                var_name='dia',
                value_name='chuva'
            )

            # Limpeza de chuva — .str.replace encadeado corretamente
            df_long['chuva'] = (df_long['chuva'].astype(str)
                                .str.replace(',', '.', regex=False)
                                .str.replace('-', '0', regex=False))
            df_long['chuva'] = pd.to_numeric(df_long['chuva'], errors='coerce').fillna(0.0)

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

            # VALIDAÇÃO DO ANO: filtra apenas linhas do ano esperado (pelo nome do arquivo)
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

            # Ingestão
            df_to_db.to_sql('monitoramento_pluviometrico', engine, if_exists='append', index=False, chunksize=1000)
            print(f"  [OK] Ingerido: {file_path.name} ({len(df_to_db)} linhas)")

        except Exception as e:
            print(f"  [ERRO] Falha no arquivo {file_path.name}: {e}")

if __name__ == "__main__":
    executar_etl()
    print("\n Ingestão no PostgreSQL concluída com sucesso!")