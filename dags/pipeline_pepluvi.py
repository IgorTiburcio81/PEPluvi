from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'igor.tiburcio',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJETO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with DAG(
    'pipeline_pepluvi',
    default_args=default_args,
    description='DAG para o pipeline PEPluvi (Carga Incremental D-1)',
    schedule='0 6 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['pepluvi'],
) as dag:

    task_limpa_csv = BashOperator(
        task_id='limpa_csv',
        bash_command=f'cd {PROJETO_DIR} && rm -f data/raw/*_2026.csv'
    )

    task_scraping = BashOperator(
        task_id='scraping',
        bash_command=f'cd {PROJETO_DIR} && python pipeline/extract/scraping_apac.py'
    )

    task_validacao = BashOperator(
        task_id='validacao_integridade',
        bash_command=f'cd {PROJETO_DIR} && python pipeline/extract/valid_data.py'
    )

    task_limpeza_duckdb = BashOperator(
        task_id='limpeza_duckdb',
        bash_command=f"""cd {PROJETO_DIR} && python -c "import duckdb; duckdb.connect('data/pepluvi.duckdb').execute('DELETE FROM monitoramento_pluviometrico WHERE EXTRACT(YEAR FROM data) = 2026')" """
    )

    task_ingestao = BashOperator(
        task_id='ingestao_duckdb',
        bash_command=f'cd {PROJETO_DIR} && python pipeline/load/ingest_duckdb.py 2026'
    )

    task_limpa_csv >> task_scraping >> task_validacao >> task_limpeza_duckdb >> task_ingestao