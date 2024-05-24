from airflow.decorators import dag, task
from datetime import datetime
import requests
import zipfile
import os
import pandas as pd
import json
import holidays

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
    def download_e_extracao_zip(arquivo: dict, nome_pasta: str = 'dados'):
        nome_arquivo = arquivo['nome'][0:-4]
        url = arquivo['link']
        arquivo_zip = f'{nome_arquivo}.zip'
        
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
        df = pd.merge(dfs[0][['id', 'tipo_veiculo', 'marca', 'ano_fabricacao_veiculo']], dfs[1], how='right', on='id')
        df = df.drop_duplicates(subset='id', keep='first')

        # Tirar colunas que não serão utilizadas
        df = df.drop(columns=['uop', 'regional', 'ignorados', 'ilesos', 'feridos_graves', 'feridos_leves'])

        df.to_csv(f'dados/resultado_{ano}.csv', index=False)

        for arquivo in arquivos:
            os.remove(f'dados/{arquivo}')

    @task(task_id='limpeza')
    def limpeza_dados(arquivo: str):

        df = pd.read_csv(f"dados/{arquivo}", delimiter=',', encoding='utf-8')

        # Verifica se data e hórario estão em formato padrão e se existem.
        try: 
            df['data_inversa'] = pd.to_datetime(df['data_inversa'])
            df['horario'] = pd.to_datetime(df['horario'], format='%H:%M:%S').dt.time

            valores_anos = df['data_inversa'].dt.year.value_counts().keys()
            if list(valores_anos) == [df['data_inversa'].dt.year[0]] and len(valores_anos) == 1:
                print("\nData e horário estão certos!")
            else:
                print("\nExiste um valor do ano que não corresponde a tabela")
        except: 
            print("\nData ou horário possuem erros!")

        # Retira valores de 'dia_semana' invalidos
        df = df[df['dia_semana'].isin(['domingo', 'quarta-feira', 'quinta-feira', 'segunda-feira', 'sexta-feira', 'sábado', 'terça-feira'])]

        # Arruma a distribuição das fase dos dias
        df['fase_dia'] = df.apply(
            lambda row: 'Amanhecer' if 5 <= row['horario'].hour < 7 
            else 'Dia' if 7 <= row['horario'].hour < 12 
            else 'Tarde' if 12 <= row['horario'].hour < 18 
            else 'Noite' if 18 <= row['horario'].hour < 24
            else 'Madrugada',
            axis=1
        )

        ufs = [
            "AC",  # Acre
            "AL",  # Alagoas
            "AM",  # Amazonas
            "AP",  # Amapá
            "BA",  # Bahia
            "CE",  # Ceará
            "DF",  # Distrito Federal
            "ES",  # Espírito Santo
            "GO",  # Goiás
            "MA",  # Maranhão
            "MG",  # Minas Gerais
            "MS",  # Mato Grosso do Sul
            "MT",  # Mato Grosso
            "PA",  # Pará
            "PB",  # Paraíba
            "PE",  # Pernambuco
            "PI",  # Piauí
            "PR",  # Paraná
            "RJ",  # Rio de Janeiro
            "RN",  # Rio Grande do Norte
            "RO",  # Rondônia
            "RR",  # Roraima
            "RS",  # Rio Grande do Sul
            "SC",  # Santa Catarina
            "SE",  # Sergipe
            "SP",  # São Paulo
            "TO"   # Tocantins
        ]

        # Retira valores de UF invalidos
        df = df[df['uf'].isin(ufs)]

        #  Retora valores ausentes em delegacia
        df = df.dropna(subset=['delegacia'])

        # Retira as linhas onde 'mortos' é maior que 'pessoas'
        df = df[df['mortos'] <= df['pessoas']]

        # Retira as linhas onde 'feridos' é maior que 'pessoas'
        df = df[df['feridos'] <= df['pessoas']]

        # Calculando a mediana das colunas 'pessoas' e 'veiculo' ignorando os valores zero
        mediana_pessoas = df[df['pessoas'] > 0]['pessoas'].median()
        mediana_veiculos = df[df['veiculos'] > 0]['veiculos'].median()

        # Substituindo os valores zero pela mediana
        df.loc[df['pessoas'] == 0, 'pessoas'] = mediana_pessoas
        df.loc[df['veiculos'] == 0, 'veiculos'] = mediana_veiculos

        # Retira valores de 'sentido_via' invalidos
        df = df[df['sentido_via'].isin(['Crescente', 'Decrescente', 'Não Informado'])]

        # Retira valores de 'tipo_pista' invalidos
        df = df[df['tipo_pista'].isin(['Dupla', 'Múltipla', 'Simples'])]

        # Retira valores de 'uso_solo' invalidos
        df = df[df['uso_solo'].isin(['Não', 'Sim'])]

        df.to_csv(f'dados/limpos_{arquivo[10:14]}.csv', index=False)

    @task(task_id='transformar_dados')
    def transformar_dados(arquivo: str):

        df = pd.read_csv(f"dados/{arquivo}", delimiter=',', encoding='utf-8')

        # Transformando para datetime
        df['data_inversa'] = pd.to_datetime(df['data_inversa'])
        df['horario'] = pd.to_datetime(df['horario'], format='%H:%M:%S').dt.time

        meses = {
            1: 'Janeiro',
            2: 'Fevereiro',
            3: 'Março',
            4: 'Abril',
            5: 'Maio',
            6: 'Junho',
            7: 'Julho',
            8: 'Agosto',
            9: 'Setembro',
            10: 'Outubro',
            11: 'Novembro',
            12: 'Dezembro'
        }

        # Derivando novas colunas da dimensão tempo
        df['dia'] = df['data_inversa'].dt.day
        df['mes'] = df['data_inversa'].dt.month.map(meses)
        df['ano'] = df['data_inversa'].dt.year
        df['hora'] = df['horario'].apply(lambda x: x.hour)
        df['trimestre'] = df['data_inversa'].dt.quarter
        df['feriado'] = df['data_inversa'].apply(lambda x: x in holidays.Brazil())
        df['dia_util'] = df.apply(lambda row: False if row['dia_semana'] in ['sábado', 'domingo'] else True, axis=1)

        # Derivando novas colunas da dimensão rodovia
        df['uso_solo'] = df['uso_solo'].replace({'Rural': 'Não', 'Urbano': 'Sim'})

        tipos_de_via_lista = ['Aclive', 'Declive', 'Curva', 'Em Obras', 'Viaduto', 'Reta', 'Ponte', 'Rotatória', 'Interseção de Vias', 'Desvio Temporário', 'Retorno Regulamentado', 'Túnel']
        tipos_via = {tipo: [] for tipo in tipos_de_via_lista}

        for _, linha in df.iterrows():
            vias = linha['tracado_via'].split(';')
            for via_certa in tipos_de_via_lista:
                encontrado = False
                for via in vias:
                    if via[0:3] in via_certa:
                        encontrado = True
                        break
                tipos_via[via_certa].append(encontrado)

        for coluna, linhas in tipos_via.items():
            df[f'{coluna}'] = linhas

        # Tirar colunas que não serão utilizadas
        df = df.drop(columns=['data_inversa', 'horario', 'tracado_via'])

        df.to_csv(f'dados/transformados_e_limpos_{arquivo[7:11]}.csv', index=False)

        os.remove(f'dados/limpos_{arquivo[7:11]}.csv')
                  
    @task(task_id='unir_dados')
    def unir_dados(arquivos: list):
        # Unir os dados limpos e transformados
        dfs = [pd.read_csv(f"dados/{arquivo}", delimiter=',', encoding='utf-8') for arquivo in arquivos]
        df = pd.concat(dfs, axis=0)

        df.to_csv(f'dados/transformados_e_limpos_todos_anos.csv', index=False)

        for arquivo in arquivos:
            os.remove(f'dados/{arquivo}')

    @task(task_id='criar_dimensões')
    def criar_dimensoes(arquivo: str):
        df = pd.read_csv(arquivo, delimiter=',', encoding='utf-8')

        # Criar dimensão tempo
        dim_tempo = df[['hora', 'dia', 'mes', 'ano', 'trimestre', 'feriado', 'dia_util', 'dia_semana']].drop_duplicates().reset_index(drop=True)
        dim_tempo['id_tempo'] = dim_tempo.index + 1

        # Criar dimensão rodovia
        dim_rodovia = df[['br', 'km', 'sentido_via', 'uso_solo', 'tipo_pista', 'Aclive', 'Declive', 'Curva', 'Em Obras', 'Viaduto', 'Reta', 'Ponte', 'Rotatória', 'Interseção de Vias', 'Desvio Temporário', 'Retorno Regulamentado', 'Túnel']].drop_duplicates().reset_index(drop=True)
        dim_rodovia['id_rodovia'] = dim_rodovia.index + 1

        # Criar dimensão local
        dim_local = df[['uf', 'municipio', 'delegacia', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)
        dim_local['id_local'] = dim_local.index + 1

        # Criar tabela fato
        fato_acidente = df[['pessoas', 'veiculos', 'feridos', 'mortos']].reset_index(drop=True)
        fato_acidente = df.merge(dim_tempo[['id_tempo','hora', 'dia', 'mes', 'ano', 'trimestre', 'feriado', 'dia_util', 'dia_semana']], 
                                 on=['hora', 'dia', 'mes', 'ano', 'trimestre', 'feriado', 'dia_util', 'dia_semana'],
                                 how='left')
        
        fato_acidente = fato_acidente.merge(dim_rodovia[['id_rodovia','br', 'km', 'sentido_via', 'uso_solo', 'tipo_pista', 'Aclive', 'Declive', 'Curva', 'Em Obras', 'Viaduto', 'Reta', 'Ponte', 'Rotatória', 'Interseção de Vias', 'Desvio Temporário', 'Retorno Regulamentado', 'Túnel']],
                                            on=['br', 'km', 'sentido_via', 'uso_solo', 'tipo_pista', 'Aclive', 'Declive', 'Curva', 'Em Obras', 'Viaduto', 'Reta', 'Ponte', 'Rotatória', 'Interseção de Vias', 'Desvio Temporário', 'Retorno Regulamentado', 'Túnel'],
                                            how='left')
        
        fato_acidente = fato_acidente.merge(dim_local[['id_local', 'uf', 'municipio', 'delegacia', 'latitude', 'longitude']],
                                            on=['uf', 'municipio', 'delegacia', 'latitude', 'longitude'],
                                            how='left')
        
        fato_acidentes = fato_acidente[['id_tempo', 'id_rodovia', 'id_local', 'pessoas', 'veiculos', 'feridos', 'mortos']]
        fato_acidentes = fato_acidentes.rename(columns={'pessoas': 'pessoas_envolvidas', 'veiculos': 'veiculos_envolvidos', 'mortos': 'obitos'})

        nome_colunas = {
            'Aclive': 'aclive',
            'Declive': 'declive',
            'Curva': 'curva',
            'Em Obras': 'em_obras',
            'Viaduto': 'viaduto',
            'Reta': 'reta',
            'Ponte': 'ponte',
            'Rotatória': 'rotatoria',
            'Interseção de Vias': 'intersecao_vias',
            'Desvio Temporário': 'desvio_temporario',
            'Retorno Regulamentado': 'retorno_regulamentado',
            'Túnel': 'tunel'
        }
        dim_rodovia = dim_rodovia.rename(columns={'br': 'rodovia', 'km': 'posicao_rodovia'})
        dim_rodovia = dim_rodovia.rename(columns=nome_colunas)

        fato_acidentes.to_csv('dados/fato_acidentes.csv', index=False)
        dim_tempo.to_csv('dados/dim_tempo.csv', index=False)
        dim_rodovia.to_csv('dados/dim_rodovia.csv', index=False)
        dim_local.to_csv('dados/dim_local.csv', index=False)


    # Leitura do arquivo json com as URLs
    with open('urls.json', 'r') as json_file:
        urls = json.load(json_file)
            
    # Tarefas de download e extração
    download_tasks_2024_causas = download_e_extracao_zip(urls['2024']['causas'])
    download_tasks_2024_ocorrencia = download_e_extracao_zip(urls['2024']['ocorrencia'])

    download_tasks_2023_causas = download_e_extracao_zip(urls['2023']['causas'])
    download_tasks_2023_ocorrencia = download_e_extracao_zip(urls['2023']['ocorrencia'])

    download_tasks_2022_causas = download_e_extracao_zip(urls['2022']['causas'])
    download_tasks_2022_ocorrencia = download_e_extracao_zip(urls['2022']['ocorrencia'])

    # Tarefas de merge 
    merge_dados_task_2024 = merge_dados([urls['2024']['causas']['nome'], urls['2024']['ocorrencia']['nome']], '2024')
    merge_dados_task_2023 = merge_dados([urls['2023']['causas']['nome'], urls['2023']['ocorrencia']['nome']], '2023')
    merge_dados_task_2022 = merge_dados([urls['2022']['causas']['nome'], urls['2022']['ocorrencia']['nome']], '2022')

    # Tarefas de limpeza
    limpeza_dados_task_2024 = limpeza_dados("resultado_2024.csv")
    limpeza_dados_task_2023 = limpeza_dados("resultado_2023.csv")
    limpeza_dados_task_2022 = limpeza_dados("resultado_2022.csv")

    # Tarefas de transformação 
    transformar_dados_task_2024 = transformar_dados("limpos_2024.csv")
    transformar_dados_task_2023 = transformar_dados("limpos_2023.csv")
    transformar_dados_task_2022 = transformar_dados("limpos_2022.csv")

    # Tarefa de unir dados
    unir_dados_task = unir_dados(["transformados_e_limpos_2024.csv", 
                                  "transformados_e_limpos_2023.csv", 
                                  "transformados_e_limpos_2022.csv"])
    
    # Tarefa de criar dimensões
    criar_dimensoes_task = criar_dimensoes('dados/transformados_e_limpos_todos_anos.csv')
    
    # Ordem das tarefas
    download_tasks_2024_causas >> merge_dados_task_2024
    download_tasks_2024_ocorrencia >> merge_dados_task_2024

    download_tasks_2023_causas >> merge_dados_task_2023
    download_tasks_2023_ocorrencia >> merge_dados_task_2023

    download_tasks_2022_causas >> merge_dados_task_2022
    download_tasks_2022_ocorrencia >> merge_dados_task_2022

    merge_dados_task_2024 >> limpeza_dados_task_2024
    merge_dados_task_2023 >> limpeza_dados_task_2023
    merge_dados_task_2022 >> limpeza_dados_task_2022

    limpeza_dados_task_2024 >> transformar_dados_task_2024
    limpeza_dados_task_2023 >> transformar_dados_task_2023
    limpeza_dados_task_2022 >> transformar_dados_task_2022

    transformar_dados_task_2024 >> unir_dados_task
    transformar_dados_task_2023 >> unir_dados_task
    transformar_dados_task_2022 >> unir_dados_task

    unir_dados_task >> criar_dimensoes_task

etl()
