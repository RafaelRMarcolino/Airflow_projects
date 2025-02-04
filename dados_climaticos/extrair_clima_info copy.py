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


print(URL)
# Fazendo a solicitação GET para obter os dados
response = requests.get(URL)

print('response')
print(response)

# Verificando se a solicitação foi bem-sucedida
if response.status_code == 200:
    # Carregando os dados JSON na variável 'dados'
    dados = response.json()

    # Convertendo os dados para um DataFrame pandas
    df = pd.json_normalize(dados['days'])  # 'days' é onde estão as informações relevantes

    # Definindo o caminho para salvar os dados
    file_path = f'/home/ozzy/Airflow_projects/dados_climaticos/dados/semana={data_inicio}/'

    # Criando o diretório se ele não existir
    os.makedirs(file_path, exist_ok=True)

    # Salvando os dados em arquivos CSV
    df.to_csv(file_path + 'dados_brutos.csv', index=False)
    df[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperatura.csv', index=False)
    df[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv', index=False)

    print("Arquivos CSV salvos com sucesso.")
else:
    print(f"Erro ao acessar a API: {response.status_code}")
