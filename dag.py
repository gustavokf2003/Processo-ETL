from airflow.decorators import dag, task
from datetime import datetime
import requests
import zipfile
import os
import pandas as pd
import json

# Configuração do drag
@dag(
    dag_id='etl_acidentes',
    start_date=datetime(2024, 5, 20),
    tags=['etl'],
    schedule='@monthly',
    max_active_tasks=16,
    concurrency=16
)

def etl():
    @task(task_id='download_e_extracao_zip')
    def download_e_extracao_zip(url: str, nome_pasta: str = 'dados'):
        arquivo_zip = 'arquivo.zip'
        
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

    @task(task_id='merge_dados')
    def merge_dados(arquivos: list, ano: str):
        # Fazer merge dos DataFrame com mesmo ano
  
        dfs = [pd.read_csv(f"dados/{arquivo}", delimiter=';', encoding='latin1') for arquivo in arquivos]
        df = pd.merge(dfs[0][['id', 'tipo_veiculo', 'marca', 'ano_fabricacao_veiculo']], dfs[1], how='inner', on='id')
        df = df.drop_duplicates()
        df.to_csv(f'dados/resultado_{ano}.csv', index=False)

        for arquivo in arquivos:
            os.remove(f'dados/{arquivo}')
    
    # Leitura do arquivo json com as URLs
    with open('urls.json', 'r') as json_file:
        urls = json.load(json_file)
            
    # Tarefas de download e extração
    download_tasks_2024_causas = download_e_extracao_zip(urls['2024']['causas']['link'])
    download_tasks_2024_ocorrencia = download_e_extracao_zip(urls['2024']['ocorrencia']['link'])

    download_tasks_2023_causas = download_e_extracao_zip(urls['2023']['causas']['link'])
    download_tasks_2023_ocorrencia = download_e_extracao_zip(urls['2023']['ocorrencia']['link'])

    download_tasks_2022_causas = download_e_extracao_zip(urls['2022']['causas']['link'])
    download_tasks_2022_ocorrencia = download_e_extracao_zip(urls['2022']['ocorrencia']['link'])

    # Tarefas de merge 
    merge_dados_task_2024 = merge_dados([urls['2024']['causas']['nome'], urls['2024']['ocorrencia']['nome']], '2024')
    merge_dados_task_2023 = merge_dados([urls['2023']['causas']['nome'], urls['2023']['ocorrencia']['nome']], '2023')
    merge_dados_task_2022 = merge_dados([urls['2022']['causas']['nome'], urls['2022']['ocorrencia']['nome']], '2022')
    
    # Ordem das tarefas
    download_tasks_2024_causas >> merge_dados_task_2024
    download_tasks_2024_ocorrencia >> merge_dados_task_2024

    download_tasks_2023_causas >> merge_dados_task_2023
    download_tasks_2023_ocorrencia >> merge_dados_task_2023

    download_tasks_2022_causas >> merge_dados_task_2022
    download_tasks_2022_ocorrencia >> merge_dados_task_2022

etl()
