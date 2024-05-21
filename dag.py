from airflow.decorators import dag, task
from datetime import datetime
import requests
import zipfile
import os

# Configuração do drag
@dag(
    dag_id='etl_acidentes',
    start_date=datetime(2024, 5, 20),
    tags=['etl'],
    schedule='@monthly',
)

def etl():
    @task(task_id='download_e_extracao_zip')
    def download_e_extracao_zip(url: str):
        arquivo_zip = 'arquivo.zip'
        nome_pasta = 'dados'
        
        # Download do arquivo ZIP
        response = requests.get(url)
        if response.status_code == 200:
            with open(arquivo_zip, 'wb') as file:
                file.write(response.content)
            print(f"Arquivo ZIP baixado e salvo em: {arquivo_zip}")
        else:
            raise Exception(f"Falha ao baixar arquivo. Código de status: {response.status_code}")

        # Extrai o arquivo ZIP
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(nome_pasta)
            print(f"Arquivo ZIP extraído para: {nome_pasta}")

        # Remove o arquivo ZIP após a extração
        os.remove(arquivo_zip)
        print(f"Arquivo ZIP removido: {arquivo_zip}")

    urls = ['https://drive.google.com/uc?export=download&id=14qBOhrE1gioVtuXgxkCJ9kCA8YtUGXKA', 
            'https://drive.google.com/uc?export=download&id=1-caam_dahYOf2eorq4mez04Om6DD5d_3',
            'https://drive.google.com/uc?export=download&id=1wskEgRC3ame7rncSDQ7qWhKsoKw1lohY']
    
    for url in urls:
        download_e_extracao_zip(url)

etl()
