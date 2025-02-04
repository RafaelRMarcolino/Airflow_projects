from airflow import DAG
from airflow.operators.bash_operator import BashOperator  # Corrigido de BashOparator para BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pendulum 
from os.path import join
import pandas as pd

# Definindo o DAG
with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2022, 8, 22, tz="UTC"),
    schedule_interval='0 0 * * 1',  # Executa toda segunda-feira
) as dag:

    # Tarefa 1: Criar a pasta
    tarefa_1 = BashOperator(
        task_id="cria_pasta", 
        bash_command='mkdir -p "/home/ozzy/Airflow_projects/dados_climaticos/semana={{ ds }}"',  # Usando 'ds' (data de execução do DAG)
    )

    # Função que extrai os dados
    def extrai_dados(data_interval_end):
        city = 'Boston'  # Corrigido o nome da cidade
        key = 'ANZQ5K8QQP8BXZ85F4ZEQ2FPK'

        URL = join(
            'http://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
            f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=day&key={key}&contentType=csv'
        )
        
        # Baixando os dados
        dados = pd.read_csv(URL)

        print('dados')
        print(dados)

        file_path = f"/home/ozzy/Airflow_projects/dados_climaticos/dados/semana={data_interval_end}/"

        # Salvando os dados em diferentes arquivos CSV
        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatura.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    # Tarefa 2: Extrair dados com PythonOperator
    tarefa_2 = PythonOperator(
        task_id='extrai_dados',
        python_callable=extrai_dados,
        op_kwargs={'data_interval_end': '{{ ds }}'}  # Usando 'ds' para a data da execução
    )

    # Definindo a dependência entre as tarefas
    tarefa_1 >> tarefa_2
