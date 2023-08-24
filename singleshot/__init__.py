import datetime
import logging
from api_in import produto_plano_de_contas
import os
import requests
import pyodbc

import azure.functions as func

obj3 = "produto_plano_de_contas"
url_base = os.getenv('DB_UR')
cont_pg = 0
url_tg = f"{url_base}/{obj3}?cursor={cont_pg}"

def verificar_API_and_save(dados, filename):
    with open(filename, 'w', encoding='utf-8') as file:
        if dados is not None:
            if len(dados) > 0:
                for estrutura in dados:
                    file.write(str(estrutura) + '\n')  # Escreve cada estrutura no arquivo
            else:
                file.write("Nenhum dado encontrado.\n")
        else:
            file.write("Falha ao acessar a API.\n")
            
def visualizar_API(dados):    
    if dados is not None:
        if len(dados) > 0:
            for estrutura in dados:
                print(estrutura)
        else:
            print("Nenhum dado encontrado.")
    else:
        print("Falha ao acessar a API.")

def insert_into_database(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username}Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')

    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    #remover os prefixos
    _id_value = data['_id'].replace('id', '')
    planos_de_custos_list_custom_produto_plano_de_custo_value = data['planos_de_custos_list_custom_produto_plano_de_custo'].replace('planos_de_custos_list_custom_produto_plano_de_custo', '')
    id_produto_centro_decustos_value = data['id_produto_centro_decustos'].replace('id_produto_centro_decustos', '')
    id_empresa_custom_empresa_value = data['id_empresa_custom_empresa'].replace('id_empresa_custom_empresa', '')
    visivel_boolean_value = data['visivel_boolean'].replace('visivel_boolean', '')
    unificador_text_value = data['unificador_text'].replace('unificador_text', '')
    tipo_plano_de_contas_option_tiposubreceita_value = data['tipo_plano_de_contas_option_tiposubreceita'].replace('tipo_plano_de_contas_option_tiposubreceita', '')
    plano_de_contas_custom_subreceita_value = data['plano_de_contas_custom_subreceita'].replace('plano_de_contas_custom_subreceita', '')
    porcentagem_number_value = data['porcentagem_number'].replace('porcentagem_number', '')
    ativo_boolean_value = data['ativo_boolean'].replace('ativo_boolean', '')
    id_empresa_text_value = data['id_empresa_text'].replace('id_empresa_text', '')
    Created_By_value= data['[Created By]'].replace('[Created By]', '')
    Created_Date_value = data['[Created Date]'].replace('[Created Date]', '')
    Modified_Date_value = data['[Modified Date]'].replace('[Modified Date]', '')
    

    for estrutura in data:
        query = """
            INSERT INTO PRODUTO_CONTAS (
                [Modified Date], [Created Date], [Created By], id_empresa_text,
                ativo_boolean, porcentagem_number, plano_de_contas_custom_subreceita,
                tipo_plano_de_contas_option_tiposubreceita, unificador_text, visivel_boolean,
                id_empresa_custom_empresa, id_produto_centro_decustos,
                planos_de_custos_list_custom_produto_plano_de_custo, _id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    values = (
        Modified_Date_value, Created_Date_value, Created_By_value,
        id_empresa_text_value, ativo_boolean_value, porcentagem_number_value,
        plano_de_contas_custom_subreceita_value, tipo_plano_de_contas_option_tiposubreceita_value,
        unificador_text_value, visivel_boolean_value, id_empresa_custom_empresa_value,
        id_produto_centro_decustos_value,
        planos_de_custos_list_custom_produto_plano_de_custo_value, _id_value
    )

    cursor.execute(query, values)
    conn.commit()
    conn.close()


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
        print("Timer atrasado")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    print("Executado com sucesso")

response = requests.get(url_tg)
response.encoding = 'utf-8'  # Definir a codificação como UTF-8

dados = produto_plano_de_contas(url_tg)

filename = 'dados_salvos.txt'
verificar_API_and_save(dados, filename)

insert_into_database(dados)

logging.info=("Script finalizado")
