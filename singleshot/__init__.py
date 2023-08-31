from datetime import datetime 
import logging
from api_in import produto_plano_de_contas, ContasPagar, ContasReceber, ContasPagar_5pg, ContasReceber_5pg, produto_plano_de_contas_5pg
import os
import requests
import pyodbc

import azure.functions as func

obj3 = "produto_plano_de_contas"
obj2 = "contas a pagar"
obj1 = "contas a receber"

url_base = os.getenv('DB_UR')
cont_pg = 0
url_tg = f"{url_base}/{obj3}?cursor={cont_pg}" 
url_tg_contas_a_pagar = f"{url_base}/{obj2}?cursor={cont_pg}" 
url_tg_contas_a_receber = f"{url_base}/{obj1}?cursor={cont_pg}"

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

def insert_into_databaseFULL_obj3(data):# Full dados obj3 produto_plano_de_contas
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()
    
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
    for item in data:
        try:
            _id_value = item['_id'].replace('id', '')
            planos_de_custos_list_custom_produto_plano_de_custo_value = item['planos_de_custos_list_custom_produto_plano_de_custo'].replace('planos_de_custos_list_custom_produto_plano_de_custo', '')
            id_produto_centro_decustos_value = item['id_produto_centro_decustos'].replace('id_produto_centro_decustos', '')
            id_empresa_custom_empresa_value = item['id_empresa_custom_empresa'].replace('id_empresa_custom_empresa', '')
            visivel_boolean_value = item['visivel_boolean'].replace('visivel_boolean', '')
            unificador_text_value = item['unificador_text'].replace('unificador_text', '')
            tipo_plano_de_contas_option_tiposubreceita_value = item['tipo_plano_de_contas_option_tiposubreceita'].replace('tipo_plano_de_contas_option_tiposubreceita', '')
            plano_de_contas_custom_subreceita_value = item['plano_de_contas_custom_subreceita'].replace('plano_de_contas_custom_subreceita', '')
            porcentagem_number_value = item['porcentagem_number'].replace('porcentagem_number', '')
            ativo_boolean_value = item['ativo_boolean'].replace('ativo_boolean', '')
            id_empresa_text_value = item['id_empresa_text'].replace('id_empresa_text', '')
            Created_By_value= item['Created By'].replace('Created By', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')
            
            created_date_obj = datetime.strptime(Created_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')
            modified_date_obj = datetime.strptime(Modified_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')
            
            formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            porcentagem_number_value = float(item['porcentagem_number']) if item['porcentagem_number'] else None

        except KeyError:
            _id_value = ''
            planos_de_custos_list_custom_produto_plano_de_custo_value = ''
            id_produto_centro_decustos_value = ''
            id_empresa_custom_empresa_value = ''
            visivel_boolean_value = ''
            unificador_text_value = ''
            tipo_plano_de_contas_option_tiposubreceita_value = ''
            plano_de_contas_custom_subreceita_value = ''
            porcentagem_number_value = ''
            ativo_boolean_value = ''
            id_empresa_text_value = ''
            Created_By_value = ''
            Created_Date_value = ''
            Modified_Date_value = ''
            
        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,
            id_empresa_text_value, ativo_boolean_value, porcentagem_number_value,
            plano_de_contas_custom_subreceita_value, tipo_plano_de_contas_option_tiposubreceita_value,
            unificador_text_value, visivel_boolean_value, id_empresa_custom_empresa_value,
            id_produto_centro_decustos_value,
            planos_de_custos_list_custom_produto_plano_de_custo_value, _id_value
        )
        
        cursor.execute(query, values)
        conn.commit()
    
    conn.close()

def insert_into_databaseFULL_obj2(data):  # Full dados obj2 contas a pagar
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    query = """
        INSERT INTO CONTAS_A_PAGAR (
            [Modified Date], [Created Date], [Created By], compet_ncia_date, id_empresa_text,
            pago_boolean, repeti__es_number, valor_number, vencimento_date, forma_de_pagamento_text, apagado_boolean, entrada_boolean,
            cliente_custom_cliente1, parcela_number, pedido_de_venda_custom_pedido_de_venda, valor_inicial_number, ativo_boolean,
            id_cash_text, agrupado_boolean, parcela_name_text, mes_number, ano_number,
            produto_plano_de_contas_list_custom_produto_plano_de_contas, empresa1_custom_empresa, migrado_boolean, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        _id_value = ''
        migrado_boolean_value = ''
        empresa1_custom_empresa_value = ''
        produto_plano_de_contas_list_custom_produto_plano_de_contas_value = ''
        ano_number_value = ''
        mes_number_value = ''
        parcela_name_text_value = ''
        agrupado_boolean_value = ''
        id_cash_text_value = ''
        ativo_boolean_value = ''
        valor_inicial_number_value = ''
        pedido_de_vendas_custom_pedido_de_venda_value = ''
        parcela_number_value = ''
        cliente_custom_cliente1_value = ''
        entrada_boolean_value = ''
        apagado_boolean_value = ''
        forma_de_pagamento_text_value = ''
        vencimento_date_value = ''
        valor_number_value = ''
        repeti__es_number_value = ''
        pago_boolean_value = ''
        id_empresa_text_value = ''
        compet_ncia_date_value = ''
        Created_By_value = ''
        Created_Date_value = ''
        Modified_Date_value = ''

        try:
            _id_value = item['_id'].replace('id', '')
            migrado_boolean_value = item['migrado_boolean'].replace('migrado_boolean', '')
            empresa1_custom_empresa_value = item['empresa1_custom_empresa'].replace('empresa1_custom_empresa', '')
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value = item['produto_plano_de_contas_list_custom_produto_plano_de_contas'].replace('produto_plano_de_contas_list_custom_produto_plano_de_contas', '')
            ano_number_value = item['ano_number'].replace('ano_number', '')
            mes_number_value = item['mes_number'].replace('mes_number', '')
            parcela_name_text_value = item['parcela_name_text'].replace('parcela_name_text', '')
            agrupado_boolean_value = item['agrupado_boolean'].replace('agrupado_boolean', '')
            id_cash_text_value = item['id_cash_text'].replace('id_cash_text', '')
            ativo_boolean_value = item['ativo_boolean'].replace('ativo_boolean', '')
            valor_inicial_number_value = item['valor_inicial_number'].replace('valor_inicial_number', '')
            pedido_de_vendas_custom_pedido_de_venda_value = item['pedido_de_venda_custom_pedido_de_venda'].replace('pedido_de_venda_custom_pedido_de_venda', '')
            parcela_number_value = item['parcela_number'].replace('parcela_number', '')
            cliente_custom_cliente1_value = item['cliente_custom_cliente1'].replace('cliente_custom_cliente1', '')
            entrada_boolean_value = item['entrada_boolean'].replace('entrada_boolean', '')
            apagado_boolean_value = item['apagado_boolean'].replace('apagado_boolean', '')
            forma_de_pagamento_text_value = item['forma_de_pagamento_text'].replace('forma_de_pagamento_text', '')
            vencimento_date_value = item['vencimento_date'].replace('vencimento_date', '')
            valor_number_value = item['valor_number'].replace('valor_number', '')
            repeti__es_number_value = item['repeti__es_number'].replace('repeti__es_number', '')
            pago_boolean_value = item['pago_boolean'].replace('pago_boolean', '')
            id_empresa_text_value = item['id_empresa_text'].replace('id_empresa_text', '')
            compet_ncia_date_value = item['compet_ncia_date'].replace('compet_ncia_date', '')
            Created_By_value = item['Created By'].replace('Created By', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')

            created_date_obj = datetime.strptime(Created_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')
            modified_date_obj = datetime.strptime(Modified_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')

            formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')

        except KeyError:
            pass

        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,compet_ncia_date_value,
            id_empresa_text_value,pago_boolean_value,repeti__es_number_value,valor_number_value,vencimento_date_value,forma_de_pagamento_text_value,
            apagado_boolean_value,entrada_boolean_value,cliente_custom_cliente1_value,parcela_number_value,
            pedido_de_vendas_custom_pedido_de_venda_value,valor_inicial_number_value,ativo_boolean_value,id_cash_text_value,
            agrupado_boolean_value,parcela_name_text_value,mes_number_value,ano_number_value,
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value,empresa1_custom_empresa_value,migrado_boolean_value, _id_value
        )

        cursor.execute(query, values)
        conn.commit()

    conn.close()

def insert_into_databaseFULL_obj1(data):# Full dados obj1 contas a receber
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    query = """
        INSERT INTO CONTAS_A_RECEBER (
            [Modified Date], [Created Date], [Created By], compet_ncia_date, id_empresa_text,
            pago_boolean, repeti__es_number, valor_number, vencimento_date, forma_de_pagamento_text, banco_text, data_do_pagamento_date,
            apagado_boolean, entrada_boolean, cliente_custom_cliente1, parcela_number,
            pedido_de_venda_custom_pedido_de_venda, plano_de_contas2_custom_subreceita, valor_inicial_number, ativo_boolean,
            id_cash_text, agrupado_boolean, parcela_name_text, mes_number, ano_number,
            produto_plano_de_contas_list_custom_produto_plano_de_contas, empresa1_custom_empresa, migrado_boolean, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        _id_value = ''
        migrado_boolean_value = ''
        empresa1_custom_empresa_value = ''
        produto_plano_de_contas_list_custom_produto_plano_de_contas_value = ''
        ano_number_value = ''
        mes_number_value = ''
        parcela_name_text_value = ''
        agrupado_boolean_value = ''
        id_cash_text_value = ''
        ativo_boolean_value = ''
        valor_inicial_number_value = ''
        plano_de_contas2_custom_subreceita_value = ''
        pedido_de_vendas_custom_pedido_de_venda_value = ''
        parcela_number_value = ''
        cliente_custom_cliente1_value = ''
        entrada_boolean_value = ''
        apagado_boolean_value = ''
        data_do_pagamento_date_value = ''
        banco_text_value = ''
        forma_de_pagamento_text_value = ''
        vencimento_date_value = ''
        valor_number_value = ''
        repeti__es_number_value = ''
        pago_boolean_value = ''
        id_empresa_text_value = ''
        compet_ncia_date_value = ''
        Created_By_value = ''
        Created_Date_value = ''
        Modified_Date_value = ''

        try:
            _id_value = item['_id'].replace('id', '')
            migrado_boolean_value = item['migrado_boolean'].replace('migrado_boolean', '')
            empresa1_custom_empresa_value = item['empresa1_custom_empresa'].replace('empresa1_custom_empresa', '')
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value = item['produto_plano_de_contas_list_custom_produto_plano_de_contas'].replace('produto_plano_de_contas_list_custom_produto_plano_de_contas', '')
            ano_number_value = item['ano_number'].replace('ano_number', '')
            mes_number_value = item['mes_number'].replace('mes_number', '')
            parcela_name_text_value = item['parcela_name_text'].replace('parcela_name_text', '')
            agrupado_boolean_value = item['agrupado_boolean'].replace('agrupado_boolean', '')
            id_cash_text_value = item['id_cash_text'].replace('id_cash_text', '')
            ativo_boolean_value = item['ativo_boolean'].replace('ativo_boolean', '')
            valor_inicial_number_value = item['valor_inicial_number'].replace('valor_inicial_number', '')
            plano_de_contas2_custom_subreceita_value = item['plano_de_contas2_custom_subreceita'].replace('plano_de_contas2_custom_subreceita','')
            pedido_de_vendas_custom_pedido_de_venda_value = item['pedido_de_venda_custom_pedido_de_venda'].replace('pedido_de_venda_custom_pedido_de_venda', '')
            parcela_number_value = item['parcela_number'].replace('parcela_number', '')
            cliente_custom_cliente1_value = item['cliente_custom_cliente1'].replace('cliente_custom_cliente1', '')
            entrada_boolean_value = item['entrada_boolean'].replace('entrada_boolean', '')
            apagado_boolean_value = item['apagado_boolean'].replace('apagado_boolean', '')
            data_do_pagamento_date_value = item['data_do_pagamento_date'].replace('data_do_pagamento_date','')
            banco_text_value = item['banco_text'].replace('banco_text','')
            forma_de_pagamento_text_value = item['forma_de_pagamento_text'].replace('forma_de_pagamento_text', '')
            vencimento_date_value = item['vencimento_date'].replace('vencimento_date', '')
            valor_number_value = item['valor_number'].replace('valor_number', '')
            repeti__es_number_value = item['repeti__es_number'].replace('repeti__es_number', '')
            pago_boolean_value = item['pago_boolean'].replace('pago_boolean', '')
            id_empresa_text_value = item['id_empresa_text'].replace('id_empresa_text', '')
            compet_ncia_date_value = item['compet_ncia_date'].replace('compet_ncia_date', '')
            Created_By_value = item['Created By'].replace('Created By', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')

            created_date_obj = datetime.strptime(Created_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')
            modified_date_obj = datetime.strptime(Modified_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')

            formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')

        except KeyError:
            pass

        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,compet_ncia_date_value,
            id_empresa_text_value,pago_boolean_value,repeti__es_number_value,valor_number_value,vencimento_date_value,forma_de_pagamento_text_value,
            banco_text_value, data_do_pagamento_date_value, apagado_boolean_value,entrada_boolean_value,cliente_custom_cliente1_value,parcela_number_value,
            pedido_de_vendas_custom_pedido_de_venda_value, plano_de_contas2_custom_subreceita_value, valor_inicial_number_value,ativo_boolean_value,id_cash_text_value,
            agrupado_boolean_value,parcela_name_text_value,mes_number_value,ano_number_value,
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value,empresa1_custom_empresa_value,migrado_boolean_value, _id_value
        )

        cursor.execute(query, values)
        conn.commit()
 
    conn.close()

def att_bd_azureo_bj3(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    query = """
        INSERT INTO PRODUTO_CONTAS (
            [Modified Date], [Created Date], [Created By], id_empresa_text,
            ativo_boolean, porcentagem_number, plano_de_contas_custom_subreceita,
            tipo_plano_de_contas_option_tiposubreceita, unificador_text, visivel_boolean,
            id_empresa_custom_empresa, id_produto_centro_decustos,
            planos_de_custos_list_custom_produto_plano_de_custo, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            [Modified Date] = VALUES([Modified Date]),
            [Created Date] = VALUES([Created Date]),
            [Created By] = VALUES([Created By]),
            id_empresa_text = VALUES(id_empresa_text),
            ativo_boolean = VALUES(ativo_boolean),
            porcentagem_number = VALUES(porcentagem_number),
            plano_de_contas_custom_subreceita = VALUES(plano_de_contas_custom_subreceita),
            tipo_plano_de_contas_option_tiposubreceita = VALUES(tipo_plano_de_contas_option_tiposubreceita),
            unificador_text = VALUES(unificador_text),
            visivel_boolean = VALUES(visivel_boolean),
            id_empresa_custom_empresa = VALUES(id_empresa_custom_empresa),
            id_produto_centro_decustos = VALUES(id_produto_centro_decustos),
            planos_de_custos_list_custom_produto_plano_de_custo = VALUES(planos_de_custos_list_custom_produto_plano_de_custo);
    """
    
    for item in data:
        try:
            _id_value = item['_id'].replace('id', '')
            planos_de_custos_list_custom_produto_plano_de_custo_value = item['planos_de_custos_list_custom_produto_plano_de_custo'].replace('planos_de_custos_list_custom_produto_plano_de_custo', '')
            id_produto_centro_decustos_value = item['id_produto_centro_decustos'].replace('id_produto_centro_decustos', '')
            id_empresa_custom_empresa_value = item['id_empresa_custom_empresa'].replace('id_empresa_custom_empresa', '')
            visivel_boolean_value = item['visivel_boolean'].replace('visivel_boolean', '')
            unificador_text_value = item['unificador_text'].replace('unificador_text', '')
            tipo_plano_de_contas_option_tiposubreceita_value = item['tipo_plano_de_contas_option_tiposubreceita'].replace('tipo_plano_de_contas_option_tiposubreceita', '')
            plano_de_contas_custom_subreceita_value = item['plano_de_contas_custom_subreceita'].replace('plano_de_contas_custom_subreceita', '')
            porcentagem_number_value = item['porcentagem_number'].replace('porcentagem_number', '')
            ativo_boolean_value = item['ativo_boolean'].replace('ativo_boolean', '')
            id_empresa_text_value = item['id_empresa_text'].replace('id_empresa_text', '')
            Created_By_value= item['Created By'].replace('Created By', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')
            
            created_date_obj = datetime.strptime(Created_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')
            modified_date_obj = datetime.strptime(Modified_Date_value, '%Y-%m-%dT%H:%M:%S.%fZ')
            
            formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            porcentagem_number_value = float(item['porcentagem_number']) if item['porcentagem_number'] else None

        except KeyError:
            _id_value = ''
            planos_de_custos_list_custom_produto_plano_de_custo_value = ''
            id_produto_centro_decustos_value = ''
            id_empresa_custom_empresa_value = ''
            visivel_boolean_value = ''
            unificador_text_value = ''
            tipo_plano_de_contas_option_tiposubreceita_value = ''
            plano_de_contas_custom_subreceita_value = ''
            porcentagem_number_value = ''
            ativo_boolean_value = ''
            id_empresa_text_value = ''
            Created_By_value = ''
            Created_Date_value = ''
            Modified_Date_value = ''
            
        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,
            id_empresa_text_value, ativo_boolean_value, porcentagem_number_value,
            plano_de_contas_custom_subreceita_value, tipo_plano_de_contas_option_tiposubreceita_value,
            unificador_text_value, visivel_boolean_value, id_empresa_custom_empresa_value,
            id_produto_centro_decustos_value,
            planos_de_custos_list_custom_produto_plano_de_custo_value, _id_value
        ) 
        
        cursor.execute(query, values)
        conn.commit()
    conn.close()

def att_bd_azureo_bj2():
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    


def att_bd_azureo_bj1():
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()




def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
        print("Timer atrasado")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    print("Executado com sucesso")

response = requests.get(url_tg)
response = requests.get(url_tg_contas_a_pagar)
response = requests.get(url_tg_contas_a_receber)
response.encoding = 'utf-8'  # Definir a codificação como UTF-8


###___*** Exemplo de chamada das funções  ***___###

#Chamadas da pasta API_IN.py (BUSCA OS DADOS DO BUBBLE E SALVA NO DICIONARIO) # FULL DADOS

dados = produto_plano_de_contas(url_tg)
#dados_contas_a_pagar = ContasPagar(url_tg_contas_a_pagar)
#dados_contas_a_receber = ContasReceber(url_tg_contas_a_receber)


#Chamada da pasta API_IN.py (COMPARA AS PRIMEIRAS 5 PAGINAS = 500 ITENS DA API DO BUBBLE)

#dados = produto_plano_de_contas_5pg(url_tg)
#dados_contas_a_pagar = ContasPagar_5pg(url_tg_contas_a_pagar)
#dados_contas_a_receber = ContasReceber_5pg(url_tg_contas_a_receber)


#SALVA TODOS OS DADOS DO BANCO DE DADOS (BACKUP COMPLETO)

insert_into_databaseFULL_obj3(dados)
#insert_into_databaseFULL_obj2(dados_contas_a_pagar)
#insert_into_databaseFULL_obj1(dados_contas_a_receber)


#filename = 'dados_salvos.txt'
#verificar_API_and_save(dados, filename)


logging.info=("Script finalizado")
print("Script finalizado")
logging.basicConfig(level=logging.INFO, filename="RelatorioLogs.log", format="%(asctime)s - %(levelname)s - %(message)s")