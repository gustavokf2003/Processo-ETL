from airflow.decorators import dag, task
from datetime import datetime
import requests
import zipfile
import os
import pandas as pd
import json
import holidays
import psycopg2

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
        else:
            raise Exception(f"Falha ao baixar arquivo. Código de status: {response.status_code}")

        # Extrai o arquivo ZIP
        with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(nome_pasta)

        # Remove o arquivo ZIP após a extração
        os.remove(arquivo_zip)

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
            raise Exception("Data ou horário não estão em formato padrão ou não existem!")
        

        # Substitui os valores null de 'ano_fabricacao_veiculo' pela mediana
        mediana_ano = df[df['ano_fabricacao_veiculo'] != 0]['ano_fabricacao_veiculo'].median()
        df.loc[df['ano_fabricacao_veiculo'].isnull(), 'ano_fabricacao_veiculo'] = mediana_ano
        df.loc[df['ano_fabricacao_veiculo'] == 0, 'ano_fabricacao_veiculo'] = mediana_ano

        # Valores nulos em 'br' e 'km' são substituídos por -1
        br_km = ['br', 'km']
        for coluna in br_km:
            df.loc[df[coluna].isnull(), coluna] = -1

        # Substitui os valores null por "não informado"
        for coluna in df.columns:
            df.loc[df[coluna].isnull(), coluna] = 'não informado'

        valores_esperados = {'dia_semana': ['domingo', 'quarta-feira', 'quinta-feira', 'segunda-feira', 'sexta-feira', 'sábado', 'terça-feira'],
                             'uf': ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO'],
                             'sentido_via': ['Crescente', 'Decrescente', 'Não Informado'],
                             'tipo_pista': ['Dupla', 'Múltipla', 'Simples'],
                             'uso_solo': ['Não', 'Sim'],
                             'classificacao_acidente': ['Com Vítimas Fatais', 'Com Vítimas Feridas', 'Sem Vítimas'],
                             'condicao_metereologica': ['Chuva', 'Céu Claro', 'Garoa/Chuvisco', 'Granizo', 'Ignorado', 'Neve', 'Nevoeiro/Neblina', 'Nublado', 'Sol', 'Vento']
                             }
        
        # Substitui valores invalidos por "não informado"
        for coluna, valores in valores_esperados.items():
            df.loc[~df[coluna].isin(valores), coluna] = 'não informado'

        # Arruma a distribuição das fase dos dias
        df['fase_dia'] = df.apply(
            lambda row: 'Amanhecer' if 5 <= row['horario'].hour < 7 
            else 'Dia' if 7 <= row['horario'].hour < 12 
            else 'Tarde' if 12 <= row['horario'].hour < 18 
            else 'Noite' if 18 <= row['horario'].hour < 24
            else 'Madrugada',
            axis=1
        )

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
        dim_tempo = df[['hora', 'dia', 'mes', 'ano', 'trimestre', 'feriado', 'dia_util', 'dia_semana', 'fase_dia']].drop_duplicates().reset_index(drop=True)
        dim_tempo['id_tempo'] = dim_tempo.index + 1

        # Criar dimensão rodovia
        dim_rodovia = df[['br', 'km', 'sentido_via', 'uso_solo', 'tipo_pista', 'Aclive', 'Declive', 'Curva', 'Em Obras', 'Viaduto', 'Reta', 'Ponte', 'Rotatória', 'Interseção de Vias', 'Desvio Temporário', 'Retorno Regulamentado', 'Túnel']].drop_duplicates().reset_index(drop=True)
        dim_rodovia['id_rodovia'] = dim_rodovia.index + 1

        # Criar dimensão local
        dim_local = df[['uf', 'municipio', 'delegacia', 'latitude', 'longitude']].drop_duplicates().reset_index(drop=True)
        dim_local['id_local'] = dim_local.index + 1

        # Criar dimensão descritivo 
        dim_descritivo = df[['causa_acidente', 'tipo_acidente', 'classificacao_acidente', 'condicao_metereologica']].drop_duplicates().reset_index(drop=True)
        dim_descritivo['id_descritivo'] = dim_descritivo.index + 1

        # Criar dimensão veiculo protagonista
        dim_veiculo = df[['tipo_veiculo', 'marca', 'ano_fabricacao_veiculo']].drop_duplicates().reset_index(drop=True)
        dim_veiculo['id_veiculo'] = dim_veiculo.index + 1

        # Criar tabela fato
        fato_acidente = df[['pessoas', 'veiculos', 'feridos', 'mortos']].reset_index(drop=True)
        fato_acidente = df.merge(dim_tempo[['id_tempo','hora', 'dia', 'mes', 'ano', 'trimestre', 'feriado', 'dia_util', 'dia_semana', 'fase_dia']], 
                                 on=['hora', 'dia', 'mes', 'ano', 'trimestre', 'feriado', 'dia_util', 'dia_semana', 'fase_dia'],
                                 how='left')
        
        fato_acidente = fato_acidente.merge(dim_rodovia[['id_rodovia','br', 'km', 'sentido_via', 'uso_solo', 'tipo_pista', 'Aclive', 'Declive', 'Curva', 'Em Obras', 'Viaduto', 'Reta', 'Ponte', 'Rotatória', 'Interseção de Vias', 'Desvio Temporário', 'Retorno Regulamentado', 'Túnel']],
                                            on=['br', 'km', 'sentido_via', 'uso_solo', 'tipo_pista', 'Aclive', 'Declive', 'Curva', 'Em Obras', 'Viaduto', 'Reta', 'Ponte', 'Rotatória', 'Interseção de Vias', 'Desvio Temporário', 'Retorno Regulamentado', 'Túnel'],
                                            how='left')
        
        fato_acidente = fato_acidente.merge(dim_local[['id_local', 'uf', 'municipio', 'delegacia', 'latitude', 'longitude']],
                                            on=['uf', 'municipio', 'delegacia', 'latitude', 'longitude'],
                                            how='left')
        
        fato_acidente = fato_acidente.merge(dim_descritivo[['id_descritivo', 'causa_acidente', 'tipo_acidente', 'classificacao_acidente', 'condicao_metereologica']],
                                            on=['causa_acidente', 'tipo_acidente', 'classificacao_acidente', 'condicao_metereologica'],
                                            how='left')
        
        fato_acidente = fato_acidente.merge(dim_veiculo[['id_veiculo', 'tipo_veiculo', 'marca', 'ano_fabricacao_veiculo']],
                                            on=['tipo_veiculo', 'marca', 'ano_fabricacao_veiculo'],
                                            how='left')
        
        fato_acidentes = fato_acidente[['id_veiculo','id_descritivo','id_tempo', 'id_rodovia', 'id_local', 'pessoas', 'veiculos', 'feridos', 'mortos']]
        fato_acidentes = fato_acidentes.rename(columns={'pessoas': 'pessoas_envolvidas', 'veiculos': 'veiculos_envolvidos', 'mortos': 'obitos'})
        fato_acidentes = fato_acidentes.drop_duplicates(subset=['id_descritivo','id_tempo', 'id_rodovia', 'id_local']).reset_index(drop=True)

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
        dim_descritivo.to_csv('dados/dim_descritivo.csv', index=False)
        dim_veiculo.to_csv('dados/dim_veiculo.csv', index=False)

    @task(task_id='carregar_dados')
    def carregar_dados():
        
        # Conexão com o banco de dados
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="airflowdb",
            user="airflowuser",
            password="senha"
        )

        # Criação das tabelas
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Dim_Tempo (
                id_tempo SERIAL PRIMARY KEY,
                hora INT,
                dia INT,
                mes VARCHAR(10),
                ano INT,
                trimestre INT,
                fase_dia VARCHAR(10),
                dia_semana VARCHAR(15),
                feriado BOOLEAN,
                dia_util BOOLEAN    
            );
                        
            CREATE TABLE IF NOT EXISTS Dim_Rodovia (
                id_rodovia SERIAL PRIMARY KEY,
                rodovia FLOAT,
                posicao_rodovia VARCHAR(14),
                sentido_via VARCHAR(15),
                uso_solo VARCHAR(14),
                tipo_pista VARCHAR(14),
                aclive BOOLEAN,
                declive BOOLEAN,
                curva BOOLEAN,
                em_obras BOOLEAN,
                viaduto BOOLEAN,
                reta BOOLEAN,
                ponte BOOLEAN,
                rotatoria BOOLEAN,
                intersecao_vias BOOLEAN,
                desvio_temporario BOOLEAN,
                retorno_regulamentado BOOLEAN,
                tunel BOOLEAN
            );
                        
            CREATE TABLE IF NOT EXISTS Dim_Local (
                id_local SERIAL PRIMARY KEY,
                uf VARCHAR(14),
                municipio VARCHAR(40),
                delegacia VARCHAR(30),
                latitude VARCHAR,
                longitude VARCHAR
            );
                    
            CREATE TABLE IF NOT EXISTS Dim_Descritivo (
                id_descritivo SERIAL PRIMARY KEY,
                causa_acidente VARCHAR(100),
                tipo_acidente VARCHAR(40),
                classificacao_acidente VARCHAR(30),
                condicao_metereologica VARCHAR(30)
            );
                       
            CREATE TABLE IF NOT EXISTS Dim_Veiculo (
                id_veiculo SERIAL PRIMARY KEY,
                tipo_veiculo VARCHAR(40),
                marca VARCHAR(80),
                ano_fabricacao_veiculo INT
            );
                        
            CREATE TABLE IF NOT EXISTS Fato_Acidentes (
                id_descritivo INT,
                id_tempo INT,
                id_rodovia INT,
                id_local INT,
                pessoas_envolvidas INT,
                veiculos_envolvidos INT,
                feridos INT,
                obitos INT,
                PRIMARY KEY (id_descritivo, id_tempo, id_rodovia, id_local),
                FOREIGN KEY (id_descritivo) REFERENCES Dim_Descritivo(id_descritivo),
                FOREIGN KEY (id_tempo) REFERENCES Dim_Tempo(id_tempo),
                FOREIGN KEY (id_rodovia) REFERENCES Dim_Rodovia(id_rodovia),
                FOREIGN KEY (id_local) REFERENCES Dim_Local(id_local)
            );
        """)

        conn.commit()

        # Carregar dados dos arquivos CSV
        dim_tempo = pd.read_csv('dados/dim_tempo.csv')
        dim_rodovia = pd.read_csv('dados/dim_rodovia.csv')
        dim_local = pd.read_csv('dados/dim_local.csv')
        dim_descritivo = pd.read_csv('dados/dim_descritivo.csv')
        dim_veiculo = pd.read_csv('dados/dim_veiculo.csv')
        fato_acidentes = pd.read_csv('dados/fato_acidentes.csv')

        # Inserção dos dados
        for _, row in dim_tempo.iterrows():
            cursor.execute("""
                INSERT INTO Dim_Tempo (id_tempo, hora, dia, mes, ano, trimestre, fase_dia, dia_semana, feriado, dia_util)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_tempo) DO NOTHING
            """, (row['id_tempo'], row['hora'], row['dia'], row['mes'], row['ano'], row['trimestre'], row['fase_dia'], row['dia_semana'], row['feriado'], row['dia_util']))

        for _, row in dim_rodovia.iterrows():
            cursor.execute("""
                INSERT INTO Dim_Rodovia (id_rodovia, rodovia, posicao_rodovia, sentido_via, uso_solo, tipo_pista, aclive, declive, curva, em_obras, viaduto, reta, ponte, rotatoria, intersecao_vias, desvio_temporario, retorno_regulamentado, tunel)
                VALUES (%s, %s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_rodovia) DO NOTHING
            """, (row['id_rodovia'], row['rodovia'], row['posicao_rodovia'], row['sentido_via'], row['uso_solo'], row['tipo_pista'], row['aclive'], row['declive'], row['curva'], row['em_obras'], 
                row['viaduto'], row['reta'], row['ponte'], row['rotatoria'], row['intersecao_vias'], row['desvio_temporario'], row['retorno_regulamentado'], row['tunel']))

        for _, row in dim_local.iterrows():
            cursor.execute("""
                INSERT INTO Dim_Local (id_local, uf, municipio, delegacia, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_local) DO NOTHING
            """, (row['id_local'], row['uf'], row['municipio'], row['delegacia'], row['latitude'], row['longitude']))

        for _, row in dim_descritivo.iterrows():
            cursor.execute("""
                INSERT INTO Dim_Descritivo (id_descritivo, causa_acidente, tipo_acidente, classificacao_acidente, condicao_metereologica)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id_descritivo) DO NOTHING
            """, (row['id_descritivo'], row['causa_acidente'], row['tipo_acidente'], row['classificacao_acidente'], row['condicao_metereologica']))

        for _, row in dim_veiculo.iterrows():
            cursor.execute("""
                INSERT INTO Dim_Veiculo (id_veiculo, tipo_veiculo, marca, ano_fabricacao_veiculo)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_veiculo) DO NOTHING
            """, (row['id_veiculo'], row['tipo_veiculo'], row['marca'], row['ano_fabricacao_veiculo']))

        for _, row in fato_acidentes.iterrows():
            cursor.execute("""
                INSERT INTO Fato_Acidentes (id_descritivo, id_tempo, id_rodovia, id_local, pessoas_envolvidas, veiculos_envolvidos, feridos, obitos)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id_descritivo, id_tempo, id_rodovia, id_local) DO NOTHING
            """, (int(row['id_descritivo']), int(row['id_tempo']), int(row['id_rodovia']), int(row['id_local']), int(row['pessoas_envolvidas']), int(row['veiculos_envolvidos']), int(row['feridos']), int(row['obitos'])))

        conn.commit()
        cursor.close()
        conn.close()


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

    download_tasks_2021_causas = download_e_extracao_zip(urls['2021']['causas'])
    download_tasks_2021_ocorrencia = download_e_extracao_zip(urls['2021']['ocorrencia'])

    download_tasks_2020_causas = download_e_extracao_zip(urls['2020']['causas'])
    download_tasks_2020_ocorrencia = download_e_extracao_zip(urls['2020']['ocorrencia'])

    # Tarefas de merge 
    merge_dados_task_2024 = merge_dados([urls['2024']['causas']['nome'], urls['2024']['ocorrencia']['nome']], '2024')
    merge_dados_task_2023 = merge_dados([urls['2023']['causas']['nome'], urls['2023']['ocorrencia']['nome']], '2023')
    merge_dados_task_2022 = merge_dados([urls['2022']['causas']['nome'], urls['2022']['ocorrencia']['nome']], '2022')
    merge_dados_task_2021 = merge_dados([urls['2021']['causas']['nome'], urls['2021']['ocorrencia']['nome']], '2021')
    merge_dados_task_2020 = merge_dados([urls['2020']['causas']['nome'], urls['2020']['ocorrencia']['nome']], '2020')

    # Tarefas de limpeza
    limpeza_dados_task_2024 = limpeza_dados("resultado_2024.csv")
    limpeza_dados_task_2023 = limpeza_dados("resultado_2023.csv")
    limpeza_dados_task_2022 = limpeza_dados("resultado_2022.csv")
    limpeza_dados_task_2021 = limpeza_dados("resultado_2021.csv")
    limpeza_dados_task_2020 = limpeza_dados("resultado_2020.csv")

    # Tarefas de transformação 
    transformar_dados_task_2024 = transformar_dados("limpos_2024.csv")
    transformar_dados_task_2023 = transformar_dados("limpos_2023.csv")
    transformar_dados_task_2022 = transformar_dados("limpos_2022.csv")
    transformar_dados_task_2021 = transformar_dados("limpos_2021.csv")
    transformar_dados_task_2020 = transformar_dados("limpos_2020.csv")

    # Tarefa de unir dados
    unir_dados_task = unir_dados(["transformados_e_limpos_2024.csv", 
                                  "transformados_e_limpos_2023.csv", 
                                  "transformados_e_limpos_2022.csv",
                                  "transformados_e_limpos_2021.csv",
                                  "transformados_e_limpos_2020.csv"])
    
    # Tarefa de criar dimensões
    criar_dimensoes_task = criar_dimensoes('dados/transformados_e_limpos_todos_anos.csv')

    # Tarefa de carregar dados
    carregar_dados_task = carregar_dados()
    
    # Ordem das tarefas
    download_tasks_2024_causas >> merge_dados_task_2024
    download_tasks_2024_ocorrencia >> merge_dados_task_2024

    download_tasks_2023_causas >> merge_dados_task_2023
    download_tasks_2023_ocorrencia >> merge_dados_task_2023

    download_tasks_2022_causas >> merge_dados_task_2022
    download_tasks_2022_ocorrencia >> merge_dados_task_2022

    download_tasks_2021_causas >> merge_dados_task_2021
    download_tasks_2021_ocorrencia >> merge_dados_task_2021

    download_tasks_2020_causas >> merge_dados_task_2020
    download_tasks_2020_ocorrencia >> merge_dados_task_2020

    merge_dados_task_2024 >> limpeza_dados_task_2024
    merge_dados_task_2023 >> limpeza_dados_task_2023
    merge_dados_task_2022 >> limpeza_dados_task_2022
    merge_dados_task_2021 >> limpeza_dados_task_2021
    merge_dados_task_2020 >> limpeza_dados_task_2020

    limpeza_dados_task_2024 >> transformar_dados_task_2024
    limpeza_dados_task_2023 >> transformar_dados_task_2023
    limpeza_dados_task_2022 >> transformar_dados_task_2022
    limpeza_dados_task_2021 >> transformar_dados_task_2021
    limpeza_dados_task_2020 >> transformar_dados_task_2020

    transformar_dados_task_2024 >> unir_dados_task
    transformar_dados_task_2023 >> unir_dados_task
    transformar_dados_task_2022 >> unir_dados_task
    transformar_dados_task_2021 >> unir_dados_task
    transformar_dados_task_2020 >> unir_dados_task

    unir_dados_task >> criar_dimensoes_task

    criar_dimensoes_task >> carregar_dados_task

etl()
