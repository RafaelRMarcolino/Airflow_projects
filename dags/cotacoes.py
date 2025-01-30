from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

import pandas as pd
import requests
import logging

from io import StringIO

# ... (rest of your DAG code)

dag = DAG(
    'fin_cotacoes_bcb_classic',
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'start_date': datetime(2023, 1, 1),
        'catchup': False,  
    },
    tags=["bcb"]
)

####### EXTRACT ###############

def EXTRACT(**kwargs):
    ds_nodash = kwargs["ds_nodash"]
    base_url = "https://www4.bcb.gov.br/Download/fechamento/"
    full_url = base_url + ds_nodash + ".csv"
    logging.warning(full_url)

    try:
        response = requests.get(full_url) 
        if response.status_code == 200: 
            csv_data = response.content.decode('utf-8')
            return csv_data
    except Exception as e:
        logging.error(e)

# Definindo a tarefa de extração fora da função
extract_task = PythonOperator(
    task_id='extract',  # Aqui é onde o erro estava
    python_callable=EXTRACT,
    provide_context=True, 
    dag=dag
)

############# TRANSFORM ###############

def transform(**kwargs):
    cotacoes = kwargs['ti'].xcom_pull(task_id='extract')
    csvStringIO = StringIO(cotacoes)

    column_names = [
        "DT_FECHAMENTO",
        "COD_MOEDA",
        "TIPO_MOEDA",
        "DESC_MOEDA",
        "TAXA_COMPRA",
        "TAXA_VENDA",
        "PARIDADE_COMPRA",
        "PARIDADE_VENDA"
    ]

    data_types = {
        "DT_FECHAMENTO": str,
        "COD_MOEDA": str,
        "TIPO_MOEDA": str,
        "DESC_MOEDA": str,
        "TAXA_COMPRA": float,
        "TAXA_VENDA": float,
        "PARIDADE_COMPRA": float,
        "PARIDADE_VENDA": float
    }

    df = pd.read_csv(
        csvStringIO,
        sep=";",
        decimal=",",
        thousands=".",
        encoding="utf-8",
        header=None,
        names=column_names,
        dtype=data_types
    )

    return df

# Definindo a tarefa de transformação fora da função
transform_task = PythonOperator(  
    task_id='transform',  # Aqui também garantimos que o task_id está presente
    python_callable=transform,
    provide_context=True,
    dag=dag
)

######## CREATE TABLE ###########

create_table_ddl = """
    CREATE TABLE IF NOT EXISTS cotacoes (
        dt_fechamento DATE,
        cod_moeda TEXT,
        tipo_moeda TEXT,
        desc_moeda TEXT,
        taxa_compra REAL,
        taxa_venda REAL,
        paridade_compra REAL,
        paridade_venda REAL,
        data_processamento TIMESTAMP,
        CONSTRAINT table_pk PRIMARY KEY (dt_fechamento, cod_moeda)
    );
"""

# Definindo a tarefa de criação de tabela fora da função
create_table_postgres = PostgresOperator(
    task_id="create_table_postgres",  # task_id está definido corretamente
    postgres_conn_id="postgres_astro",
    sql=create_table_ddl,
    dag=dag
)

#### --- LOAD --- ####

def load(**kwargs):
    ti = kwargs['ti']
    cotacoes_df = ti.xcom_pull(task_ids='transform')

    table_name = "cotacoes"
    postgres_hook = PostgresHook(postgres_conn_id="postgres_astro", schema="astro")

    # Converta o DataFrame para uma lista de tuplas
    rows = list(cotacoes_df.itertuples(index=False))

    # Insira os dados no banco de dados
    postgres_hook.insert_rows(
        table_name,
        rows,
        replace=True,
        replace_index=["DT_FECHAMENTO", "COD_MOEDA"],
        target_fields=[
            "DT_FECHAMENTO", "COD_MOEDA", "TIPO_MOEDA", "DESC_MOEDA",
            "TAXA_COMPRA", "TAXA_VENDA", "PARIDADE_COMPRA", "PARIDADE_VENDA",
            "DATA_PROCESSAMENTO"
        ]
    )

# Definindo a tarefa de carga fora da função
load_task = PythonOperator(
    task_id='load',  # task_id definido corretamente
    python_callable=load,
    provide_context=True,
    dag=dag  # Certifique-se de que 'dag' está definido corretamente
)

# Definindo a ordem de execução das tarefas
extract_task >> transform_task >> create_table_postgres >> load_task
