import os
import pandas as pd
import requests
from os.path import join
from datetime import datetime, timedelta

# Definindo a data de início e fim
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# Formatando as datas
data_inicio = data_inicio.strftime("%Y-%m-%d")  # Corrigido o formato de data
data_fim = data_fim.strftime("%Y-%m-%d")        # Corrigido o formato de data

# Definindo a cidade e a chave de API
city = 'Boston'
key = "BDBQT8K68GJHUV4ZDWV8JEXQW"

# Construindo a URL com f-string para garantir a interpolação das variáveis
URL = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=json"


response = requests.get(URL)


if response.status_code == 200:
    
    dados = response.json()
    df = pd.json_normalize(dados['days']) 
    print(f"Dados carregados: {df.head()}")

    
    file_path = f'/home/ozzy/Airflow_projects/Airflow_projects/dados_climaticos/dados/semana={data_inicio}/'

    
    if not os.path.exists(file_path):
        print(f"Diretório não existe. Criando diretório: {file_path}")
        os.makedirs(file_path, exist_ok=True)
    else:
        print(f"O diretório já existe: {file_path}")

    
    df.to_csv(file_path + 'dados_brutos.csv', index=False)
    df[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatura.csv', index=False)
    df[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv', index=False)

    print("Arquivos CSV salvos com sucesso.")
else:
    print(f"Erro ao acessar a API: {response.status_code}")
