from datetime import datetime, timezone
import logging
from api_in import ContasPagar_5pg, ContasReceber_5pg,movimentacao_financeira_5pg,centro_de_custos_5pg, sub_planodecontas_5pg,produto_centro_de_custos_5pg,produto_plano_de_contas_5pg
import os
import requests
import pyodbc
import azure.functions as func
from dateutil import parser
import json

obj3 = "produto_plano_de_contas"
obj2 = "contas a pagar"
obj1 = "contas a receber"
obj4 = "movimentacao_financeira"
obj5 = "Centro_de_custos"
obj6 = "SubPlanodecontas"
obj7 = "produto_centro_de_custo"

url_base = os.getenv('DB_UR')
cont_pg = 0
url_tg_plano_contas = f"{url_base}/{obj3}?cursor={cont_pg}" 
url_tg_contas_a_pagar = f"{url_base}/{obj2}?cursor={cont_pg}" 
url_tg_contas_a_receber = f"{url_base}/{obj1}?cursor={cont_pg}"
url_movimentacao_financeira = f"{url_base}/{obj4}?cursor={cont_pg}"
url_Centro_de_custos = f"{url_base}/{obj5}?cursor={cont_pg}"
url_SubPlanodecontas = f"{url_base}/{obj6}?cursor={cont_pg}"
url_produto_centro_de_custo = f"{url_base}/{obj7}?cursor={cont_pg}"

logging.basicConfig(level=logging.INFO, filename="RelatorioLogs.log", format="%(asctime)s - %(levelname)s - %(message)s")

#_____VERIFICAÇÕES API/DATATIME_____#
def parse_custom_datetime(datetime_str):
    try:
        date_obj = parser.parse(datetime_str)
        return date_obj
    except ValueError:
        print(f"Erro ao converter a data/hora: {datetime_str}")
        return None
    
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


#_____BUSCA POR MAIS DE 50 MIL DADOS DA API_____#
def overflowdata_produto_contas(url_tg):
    cont_pg = 0
    estruturas = []
    url_tg = f"{url_base}/{obj3}?cursor={cont_pg}&sort_field=Created%20Date&descending=false"
    while True:
        response = requests.get(url_tg)
        response.encoding = 'utf-8'  # Definir a codificação como UTF-8
        try:
            data = response.json()
            if "response" in data and "results" in data["response"]:
                results = data["response"]["results"]
                for item in results:
                    estrutura = {
                        "Modified Date": str(item.get("Modified Date", "")),
                        "Created Date": str(item.get("Created Date", "")),
                        "Created By": str(item.get("Created By", "")),
                        "id_empresa_text": str(item.get("id_empresa_text", "")),
                        "ativo_boolean": str(item.get("ativo_boolean", "")),
                        "porcentagem_number": str(item.get("porcentagem_number", "")),
                        "plano_de_contas_custom_subreceita": str(item.get("plano_de_contas_custom_subreceita", "")),
                        "tipo_plano_de_contas_option_tiposubreceita": str(item.get("tipo_plano_de_contas_option_tiposubreceita", "")),
                        "unificador_text": str(item.get("unificador_text", "")),
                        "visivel_boolean": str(item.get("visivel_boolean", "")),
                        "id_empresa_custom_empresa": str(item.get("id_empresa_custom_empresa", "")),
                        "id_produto_centro_decustos": str(item.get("id_produto_centro_decustos", "")),
                        "planos_de_custos_list_custom_produto_plano_de_custo": str(item.get("planos_de_custos_list_custom_produto_plano_de_custo", "")),
                        "_id": str(item.get("_id", ""))
                    }
                    estruturas.append(estrutura) 

                remaining = data.get("response", {}).get("remaining", 0)
                
                if remaining == 0:
                    print(estrutura)
                    insert_into_databaseFULL_obj3(estruturas)
                    print("Script finalizado PRODUTO PLANO DE CONTAS")
                    break
                if len(estruturas)==5000:
                    insert_into_databaseFULL_obj3(estruturas)
                    estruturas.clear()
                    print("Inseriu no banco de dados")
                
                cont_pg += 100
                print(f"Paginas faltantes: /{remaining}/ - Contador /{cont_pg}" )
                print(len(estruturas))
                url_tg = f"{url_base}/{obj3}?cursor={cont_pg}&sort_field=Created%20Date&descending=false"
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da API")
            return []

    return []

#_____INSERIR NO BANCO AZURE SQL_____#
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
            
            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)
            
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
            produto_plano_de_contas_list_custom_produto_plano_de_contas, empresa_custom_empresa, migrado_boolean, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        _id_value = ''
        migrado_boolean_value = ''
        empresa_custom_empresa_value = ''
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
            empresa_custom_empresa_value = item['empresa_custom_empresa'].replace('empresa_custom_empresa', '')
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

            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None

        except KeyError:
            pass

        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,compet_ncia_date_value,
            id_empresa_text_value,pago_boolean_value,repeti__es_number_value,valor_number_value,vencimento_date_value,forma_de_pagamento_text_value,
            apagado_boolean_value,entrada_boolean_value,cliente_custom_cliente1_value,parcela_number_value,
            pedido_de_vendas_custom_pedido_de_venda_value,valor_inicial_number_value,ativo_boolean_value,id_cash_text_value,
            agrupado_boolean_value,parcela_name_text_value,mes_number_value,ano_number_value,
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value,empresa_custom_empresa_value,migrado_boolean_value, _id_value
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
            produto_plano_de_contas_list_custom_produto_plano_de_contas, empresa_custom_empresa, migrado_boolean, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        _id_value = ''
        migrado_boolean_value = ''
        empresa_custom_empresa_value = ''
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
        formatted_modified_date = None
        formatted_created_date = None

        try:
            _id_value = item['_id'].replace('id', '')
            migrado_boolean_value = item['migrado_boolean'].replace('migrado_boolean', '')
            empresa_custom_empresa_value = item['empresa1_custom_empresa'].replace('empresa1_custom_empresa', '')
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

            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None

        except KeyError:
            pass

        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,compet_ncia_date_value,
            id_empresa_text_value,pago_boolean_value,repeti__es_number_value,valor_number_value,vencimento_date_value,forma_de_pagamento_text_value,
            banco_text_value, data_do_pagamento_date_value, apagado_boolean_value,entrada_boolean_value,cliente_custom_cliente1_value,parcela_number_value,
            pedido_de_vendas_custom_pedido_de_venda_value, plano_de_contas2_custom_subreceita_value, valor_inicial_number_value,ativo_boolean_value,id_cash_text_value,
            agrupado_boolean_value,parcela_name_text_value,mes_number_value,ano_number_value,
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value,empresa_custom_empresa_value,migrado_boolean_value, _id_value
        )

        cursor.execute(query, values)
        conn.commit()
 
    conn.close()

def inser_into_database_obj4(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    query = """
        INSERT INTO MOVIMENTACAO_FINANCEIRA (
            [Modified Date], [Created Date], [Created By], apagado_boolean, banco_custom_bancos, cliente_custom_cliente1,
            conciliado_boolean, contas_a_receber_custom_contas_a_receber, data_date, id_empresa_text,
            tipo_option_tipo_de_conta, valor_number, descricao_text, plano_de_contas_custom_subreceita,
            acrescimo_number, decrescimo_number, mes_number, ano_number, empresa_custom_empresa, migrado_boolean, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        Modified_Date_value = ''
        Created_Date_value = ''
        Created_By_value = ''
        apagado_boolean_value = ''
        banco_custom_bancos_value = ''
        cliente_custom_cliente1_value =''
        conciliado_boolean_value = ''
        contas_a_receber_custom_contas_a_receber_value = ''
        data_date_value = ''
        id_empresa_text_value = ''
        tipo_option_tipo_de_conta_value = ''
        valor_number_value = ''
        descricao_text_value = ''
        plano_de_contas_custom_subreceita_value = ''
        acrescimo_number_value = 0
        decrescimo_number_value = 0
        mes_number_value = ''
        ano_number_value = ''
        empresa_custom_empresa_value = ''
        migrado_boolean_value = ''
        _id_value = ''
        formatted_created_date = None
        formatted_data_date = None
        formatted_modified_date = None
        
        try:
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            Created_By_value = item['Created By'].replace('Created By', '')
            apagado_boolean_value =item['apagado_boolean'].replace('apagado_boolean', '')
            banco_custom_bancos_value = item['banco_custom_bancos'].replace('banco_custom_bancos', '')
            cliente_custom_cliente1_value =item['cliente_custom_cliente1'].replace('cliente_custom_cliente1', '')
            conciliado_boolean_value = item['conciliado_boolean'].replace('conciliado_boolean', '')
            contas_a_receber_custom_contas_a_receber_value = item['contas_a_receber_custom_contas_a_receber'].replace('contas_a_receber_custom_contas_a_receber', '')
            data_date_value = item['data_date'].replace('data_date', '')
            id_empresa_text_value = item['id_empresa_text'].replace('id_empresa_text', '')
            tipo_option_tipo_de_conta_value = item['tipo_option_tipo_de_conta'].replace('tipo_option_tipo_de_conta', '')
            valor_number_value = float(item['valor_number']) if item['valor_number'] else None
            descricao_text_value = item['descricao_text'].replace('descricao_text', '')
            plano_de_contas_custom_subreceita_value = item['plano_de_contas_custom_subreceita'].replace('plano_de_contas_custom_subreceita', '')
            acrescimo_number_value = item['acrescimo_number'] 
            decrescimo_number_value = item['decrescimo_number']
            mes_number_value = int(item['mes_number'])
            ano_number_value = int(item['ano_number'])
            empresa_custom_empresa_value = item['empresa_custom_empresa'].replace('empresa_custom_empresa', '')
            migrado_boolean_value = item['migrado_boolean'].replace('migrado_boolean', '')
            _id_value = item['_id'].replace('_id', '')

            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)
            data_date_obj = parse_custom_datetime(data_date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None
                
            if data_date_obj is not None:
                formatted_data_date = data_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_data_date = None
                
            if acrescimo_number_value is not None:
                acrescimo_number_value = float(acrescimo_number_value)
            if decrescimo_number_value is not None:
                decrescimo_number_value = float(decrescimo_number_value)

        except KeyError as e:
            print(f"Erro ao inserir dados: {e}")
            print(f"Dados problemáticos: {item}")
            pass

        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,apagado_boolean_value, banco_custom_bancos_value,cliente_custom_cliente1_value,
            conciliado_boolean_value,contas_a_receber_custom_contas_a_receber_value, formatted_data_date,id_empresa_text_value,tipo_option_tipo_de_conta_value,valor_number_value,
            descricao_text_value,plano_de_contas_custom_subreceita_value,acrescimo_number_value,decrescimo_number_value,
            mes_number_value,ano_number_value,empresa_custom_empresa_value,migrado_boolean_value, _id_value
        )

        cursor.execute(query, values)
        conn.commit()
 
    conn.close()

def inser_into_database_obj5(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    query = """
    INSERT INTO CENTRO_DE_CUSTOS(
        [Created By],[Created Date],id_empresa_custom_empresa,id_empresa_text,
        [Modified Date], nome_text, _id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        Created_By_value = ''
        Created_Date_value = None
        id_empresa_custom_empresa_value = ''
        id_empresa_text_value = ''
        Modified_Date_value = None
        nome_text_value = ''
        _id_value = ''
        try:
            Created_By_value = item['Created By'].replace('Created By', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            id_empresa_custom_empresa_value = item['id_empresa_custom_empresa'].replace('id_empresa_custom_empresa', '')
            id_empresa_text_value = item['id_empresa_text'].replace('id_empresa_text', '')
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')
            nome_text_value = item['nome_text'].replace('nome_text', '')
            _id_value = item['_id'].replace('id', '')
            print(f"Valor de id_empresa_text: {id_empresa_text_value}")
            print(f"ID: : {_id_value}")
            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None

        except KeyError:
            pass

        values = (
            Created_By_value,formatted_created_date, id_empresa_custom_empresa_value,id_empresa_text_value,formatted_modified_date,nome_text_value, _id_value
        )

        cursor.execute(query, values)
        conn.commit()
        print(f"Inserido no BANCO DE DADOS{_id_value}")
    print("FINAlIZADO")    
    conn.close()
    
def inser_into_database_obj6(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    query = """
        INSERT INTO SUBPLANODECONTAS (
            [Modified Date], [Created Date], [Created By], id_empresa_text, subreceita_text,apagado_boolean,tiposub_option_tiposubreceita,
            subplanodecontas_text, empresa_custom_empresa, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        Modified_Date_value = None
        Created_Date_value = None
        Created_By_value = None
        id_empresa_text_value = ''
        subreceita_text_value = ''
        apagado_boolean_value = ''
        tiposub_option_tiposubreceita_value = ''
        subplanodecontas_text_value = ''
        empresa_custom_empresa_value = ''
        _id_value = ''
        
        try:
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            Created_By_value = item['Created By'].replace('Created By', '')
            id_empresa_text_value = item['id_empresa_text'].replace('id_empresa_text', '')
            subreceita_text_value = item['subreceita_text'].replace('subreceita_text', '')
            apagado_boolean_value = item['apagado_boolean'].replace('apagado_boolean', '')
            tiposub_option_tiposubreceita_value = item['tiposub_option_tiposubreceita'].replace('tiposub_option_tiposubreceita', '')
            subplanodecontas_text_value = item['subplanodecontas_text'].replace('subplanodecontas_text', '')
            empresa_custom_empresa_value = item['empresa_custom_empresa'].replace('empresa_custom_empresa', '')
            _id_value = item['_id'].replace('id', '')

            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None

        except KeyError:
            pass

        values = (
            formatted_modified_date, formatted_created_date, Created_By_value,id_empresa_text_value,subreceita_text_value,
            apagado_boolean_value,tiposub_option_tiposubreceita_value,subplanodecontas_text_value,empresa_custom_empresa_value, _id_value
        )

        cursor.execute(query, values)
        conn.commit()
 
    conn.close()
   
def inser_into_database_obj7(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    query = """
        INSERT INTO PRODUTO_CENTRO_DE_CUSTO (
            ativo_boolean,[Created By], [Created Date], [Modified Date],  
            plano_de_custo_custom_plano_de_custo,porcentagem_number, _id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        # Inicialização das variáveis
        ativo_boolean_value = ''
        Created_By_value = ''
        Created_Date_value = None
        Modified_Date_value = None
        plano_de_custo_custom_plano_de_custo_value = ''
        porcentagem_number_value = 1
        _id_value = ''
        
        try:
            ativo_boolean_value = item['ativo_boolean'].replace('ativo_boolean', '')
            Created_By_value = item['Created By'].replace('Created By', '')
            Created_Date_value = item['Created Date'].replace('Created Date', '')
            Modified_Date_value = item['Modified Date'].replace('Modified Date', '')
            plano_de_custo_custom_plano_de_custo_value = item['plano_de_custo_custom_plano_de_custo'].replace('plano_de_custo_custom_plano_de_custo', '')
            porcentagem_number_value = item['porcentagem_number']
            _id_value = item['_id'].replace('id', '')
            
            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None
                
        except KeyError as e:
            print(f"Erro ao inserir dados: {e}")
            print(f"Dados problemáticos: {item}")
            pass

        values = (
            ativo_boolean_value,Created_By_value, formatted_created_date, formatted_modified_date,
            plano_de_custo_custom_plano_de_custo_value,porcentagem_number_value,_id_value
        )

        cursor.execute(query, values)
        conn.commit()
 
    conn.close()


#_____INSERIR/ATUALIZAR NO BANCO AZURE SQL_____#

def att_bd_azure_obj2(data):# Att o banco de dados Azure ou adicionar os novos contas a pagar
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    for item in data:
        created_date_obj = None
        modified_date_obj = None 
        formatted_created_date = None
        formatted_modified_date = None
        _id_value = ''
        migrado_boolean_value = ''
        empresa_custom_empresa_value = ''
        produto_plano_de_contas_list_custom_produto_plano_de_contas_value = ''
        ano_number_value = ''
        mes_number_value = ''
        parcela_name_text_value = ''
        agrupado_boolean_value = ''
        id_cash_text_value = ''
        ativo_boolean_value = ''
        valor_inicial_number_value = ''
        pedido_de_venda_custom_pedido_de_venda_value = ''
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
            _id_value = item['_id']
            migrado_boolean_value = item['migrado_boolean']
            empresa_custom_empresa_value = item['empresa_custom_empresa']
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value = item['produto_plano_de_contas_list_custom_produto_plano_de_contas']
            ano_number_value = item['ano_number']
            mes_number_value = item['mes_number']
            parcela_name_text_value = item['parcela_name_text']
            agrupado_boolean_value = item['agrupado_boolean']
            id_cash_text_value = item['id_cash_text']
            ativo_boolean_value = item['ativo_boolean']
            valor_inicial_number_value = item['valor_inicial_number']
            pedido_de_venda_custom_pedido_de_venda_value = item['pedido_de_venda_custom_pedido_de_venda']
            parcela_number_value = item['parcela_number']
            cliente_custom_cliente1_value = item['cliente_custom_cliente1']
            entrada_boolean_value = item['entrada_boolean']
            apagado_boolean_value = item['apagado_boolean']
            forma_de_pagamento_text_value = item['forma_de_pagamento_text']
            vencimento_date_value = item['vencimento_date']
            valor_number_value = item['valor_number']
            repeti__es_number_value = item['repeti__es_number']
            pago_boolean_value = item['pago_boolean']
            id_empresa_text_value = item['id_empresa_text']
            compet_ncia_date_value = item['compet_ncia_date']
            Created_By_value = item['Created By']
            Created_Date_value = item['Created Date']
            Modified_Date_value = item['Modified Date']
            print(f"_id: {_id_value}")

            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute("SELECT COUNT(*) FROM CONTAS_A_PAGAR WHERE _id=?", (_id_value,))
            row_count = cursor.fetchone()[0]
            print(f"Row count para {_id_value}: {row_count}")
            values = (formatted_modified_date, formatted_created_date, Created_By_value, compet_ncia_date_value,
                  id_empresa_text_value, pago_boolean_value, repeti__es_number_value, valor_number_value,
                  vencimento_date_value, forma_de_pagamento_text_value, apagado_boolean_value, entrada_boolean_value,
                  cliente_custom_cliente1_value, parcela_number_value, pedido_de_venda_custom_pedido_de_venda_value,
                  valor_inicial_number_value, ativo_boolean_value, id_cash_text_value, agrupado_boolean_value,
                  parcela_name_text_value, mes_number_value, ano_number_value,
                  produto_plano_de_contas_list_custom_produto_plano_de_contas_value, empresa_custom_empresa_value,
                  migrado_boolean_value, _id_value
            )
            if row_count > 0:
                print('ATUALIZADO no banco de dados')
                cursor.execute("""
                    UPDATE CONTAS_A_PAGAR SET
                    [Modified Date] = ?, [Created Date] = ?, [Created By] = ?,
                    compet_ncia_date = ?, id_empresa_text = ?, pago_boolean = ?,
                    repeti__es_number = ?, valor_number = ?, vencimento_date = ?,
                    forma_de_pagamento_text = ?, apagado_boolean = ?, entrada_boolean = ?,
                    cliente_custom_cliente1 = ?, parcela_number = ?, 
                    pedido_de_venda_custom_pedido_de_venda = ?, valor_inicial_number = ?,
                    ativo_boolean = ?, id_cash_text = ?, agrupado_boolean = ?,
                    parcela_name_text = ?, mes_number = ?, ano_number = ?,
                    produto_plano_de_contas_list_custom_produto_plano_de_contas = ?,
                    empresa_custom_empresa = ?, migrado_boolean = ?
                    WHERE _id = ?
                """, values)
            else:
                print('INSERIDO no banco de dados')
                cursor.execute("""
                    INSERT INTO CONTAS_A_PAGAR (
                        [Modified Date], [Created Date], [Created By], compet_ncia_date, 
                        id_empresa_text, pago_boolean, repeti__es_number, valor_number, 
                        vencimento_date, forma_de_pagamento_text, apagado_boolean, entrada_boolean,
                        cliente_custom_cliente1, parcela_number, pedido_de_venda_custom_pedido_de_venda,
                        valor_inicial_number, ativo_boolean, id_cash_text, agrupado_boolean, 
                        parcela_name_text, mes_number, ano_number, 
                        produto_plano_de_contas_list_custom_produto_plano_de_contas, 
                        empresa_custom_empresa, migrado_boolean, _id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, values)
            conn.commit()
        except Exception as e:
            print(f"Erro: {e}")
            
    conn.close()
    logging.info('Função obj2 Finalizada')

def att_bd_azure_obj1(data): # Att o banco de dados Azure ou adicionar os novos contas a RECEBER
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    for item in data:
        # Inicialização das variáveis
        created_date_obj = None
        modified_date_obj = None 
        formatted_created_date = None
        formatted_modified_date = None
        _id_value = ''
        migrado_boolean_value = ''
        empresa_custom_empresa_value = ''
        produto_plano_de_contas_list_custom_produto_plano_de_contas_value = ''
        ano_number_value = ''
        mes_number_value = ''
        parcela_name_text_value = ''
        agrupado_boolean_value = ''
        id_cash_text_value = ''
        ativo_boolean_value = ''
        valor_inicial_number_value = ''
        plano_de_contas2_custom_subreceita_value = ''
        pedido_de_venda_custom_pedido_de_venda_value = ''
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
            _id_value = item['_id']
            migrado_boolean_value = item['migrado_boolean']
            empresa_custom_empresa_value = item['empresa1_custom_empresa']
            produto_plano_de_contas_list_custom_produto_plano_de_contas_value = item['produto_plano_de_contas_list_custom_produto_plano_de_contas']
            ano_number_value = item['ano_number']
            mes_number_value = item['mes_number']
            parcela_name_text_value = item['parcela_name_text']
            agrupado_boolean_value = item['agrupado_boolean']
            id_cash_text_value = item['id_cash_text']
            ativo_boolean_value = item['ativo_boolean']
            valor_inicial_number_value = item['valor_inicial_number']
            plano_de_contas2_custom_subreceita_value = item['plano_de_contas2_custom_subreceita']
            pedido_de_venda_custom_pedido_de_venda_value = item['pedido_de_venda_custom_pedido_de_venda']
            parcela_number_value = item['parcela_number']
            cliente_custom_cliente1_value = item['cliente_custom_cliente1']
            entrada_boolean_value = item['entrada_boolean']
            apagado_boolean_value = item['apagado_boolean']
            data_do_pagamento_date_value = item['data_do_pagamento_date']
            banco_text_value = item['banco_text']
            forma_de_pagamento_text_value = item['forma_de_pagamento_text']
            vencimento_date_value = item['vencimento_date']
            valor_number_value = item['valor_number']
            repeti__es_number_value = item['repeti__es_number']
            pago_boolean_value = item['pago_boolean']
            id_empresa_text_value = item['id_empresa_text']
            compet_ncia_date_value = item['compet_ncia_date']
            Created_By_value = item['Created By']
            Created_Date_value = item['Created Date']
            Modified_Date_value = item['Modified Date']
            
            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute("SELECT COUNT(*) FROM CONTAS_A_RECEBER WHERE _id=?", (_id_value,))
            row_count = cursor.fetchone()[0]
            print(f"Row count para {_id_value}: {row_count}")
            values= (
                    formatted_modified_date, formatted_created_date, Created_By_value, compet_ncia_date_value,
                    id_empresa_text_value, pago_boolean_value, repeti__es_number_value, valor_number_value,
                    vencimento_date_value, forma_de_pagamento_text_value, banco_text_value, data_do_pagamento_date_value, apagado_boolean_value, entrada_boolean_value,
                    cliente_custom_cliente1_value, parcela_number_value, pedido_de_venda_custom_pedido_de_venda_value,plano_de_contas2_custom_subreceita_value,
                    valor_inicial_number_value, ativo_boolean_value, id_cash_text_value, agrupado_boolean_value,
                    parcela_name_text_value, mes_number_value, ano_number_value,
                   produto_plano_de_contas_list_custom_produto_plano_de_contas_value, empresa_custom_empresa_value,
                   migrado_boolean_value, _id_value
               )
        
            if row_count > 0:
                cursor.execute("""
                    UPDATE CONTAS_A_RECEBER SET 
                    [Modified Date] = ?, [Created Date] = ?, [Created By] = ?, compet_ncia_date = ?, id_empresa_text = ?,
                    pago_boolean = ?, repeti__es_number = ?, valor_number = ?, vencimento_date = ?, forma_de_pagamento_text = ?, banco_text = ?, data_do_pagamento_date = ?,
                    apagado_boolean = ?, entrada_boolean = ?, cliente_custom_cliente1 = ?, parcela_number = ?,
                    pedido_de_venda_custom_pedido_de_venda = ?, plano_de_contas2_custom_subreceita = ?, valor_inicial_number = ?, ativo_boolean = ?,
                    id_cash_text = ?, agrupado_boolean = ?, parcela_name_text = ?, mes_number = ?, ano_number = ?,
                    produto_plano_de_contas_list_custom_produto_plano_de_contas = ?, empresa_custom_empresa = ?, migrado_boolean = ?
                    WHERE _id = ?""",values)
            else: 
                cursor.execute("""
                    INSERT INTO CONTAS_A_RECEBER (
                    [Modified Date], [Created Date], [Created By], compet_ncia_date, id_empresa_text,
                    pago_boolean, repeti__es_number, valor_number, vencimento_date, forma_de_pagamento_text, banco_text, data_do_pagamento_date,
                    apagado_boolean, entrada_boolean, cliente_custom_cliente1, parcela_number,
                    pedido_de_venda_custom_pedido_de_venda, plano_de_contas2_custom_subreceita, valor_inicial_number, ativo_boolean,
                    id_cash_text, agrupado_boolean, parcela_name_text, mes_number, ano_number,
                    produto_plano_de_contas_list_custom_produto_plano_de_contas, empresa_custom_empresa, migrado_boolean, _id
                    )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)     
                    """,values)
            
            conn.commit()
        except Exception as e:
            print(f"Erro: {e}")
    conn.close()
    logging.info('Função obj1 Finalizada')
    
def att_bd_azure_obj3(data): # Percorre toda a API do Bubble (todas as paginas)
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    for item in data:
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
        created_date_obj = None
        modified_date_obj = None 
        formatted_created_date = None
        formatted_modified_date = None
        try:
            _id_value = item['_id']
            planos_de_custos_list_custom_produto_plano_de_custo_value = item['planos_de_custos_list_custom_produto_plano_de_custo']
            id_produto_centro_decustos_value = item['id_produto_centro_decustos']
            id_empresa_custom_empresa_value = item['id_empresa_custom_empresa']
            visivel_boolean_value = item['visivel_boolean']
            unificador_text_value = item['unificador_text']
            tipo_plano_de_contas_option_tiposubreceita_value = item['tipo_plano_de_contas_option_tiposubreceita']
            plano_de_contas_custom_subreceita_value = item['plano_de_contas_custom_subreceita']
            porcentagem_number_value = item['porcentagem_number']
            ativo_boolean_value = item['ativo_boolean']
            id_empresa_text_value = item['id_empresa_text']
            Created_By_value= item['Created By']
            Created_Date_value = item['Created Date']
            Modified_Date_value = item['Modified Date']
            
            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)
            
            formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            porcentagem_number_value = float(item['porcentagem_number']) if item['porcentagem_number'] else None

            
            cursor.execute("SELECT COUNT(*) FROM PRODUTO_CONTAS WHERE _id=?", (_id_value,))
            row_count = cursor.fetchone()[0]
            print(f"Row count para {_id_value}: {row_count}")
            
            values = (
                formatted_modified_date, formatted_created_date, Created_By_value,
                id_empresa_text_value, ativo_boolean_value, porcentagem_number_value,
                plano_de_contas_custom_subreceita_value, tipo_plano_de_contas_option_tiposubreceita_value,
                unificador_text_value, visivel_boolean_value, id_empresa_custom_empresa_value,
                id_produto_centro_decustos_value,
                planos_de_custos_list_custom_produto_plano_de_custo_value, _id_value
            )
            if row_count > 0:
                cursor.execute("""
                    UPDATE PRODUTO_CONTAS SET 
                    [Modified Date] = ?, [Created Date] = ?, [Created By] = ?, id_empresa_text = ?,
                    ativo_boolean = ?, porcentagem_number = ?, plano_de_contas_custom_subreceita = ?,
                    tipo_plano_de_contas_option_tiposubreceita = ?, unificador_text = ?, visivel_boolean = ?,
                    id_empresa_custom_empresa = ?, id_produto_centro_decustos = ?,
                    planos_de_custos_list_custom_produto_plano_de_custo = ?
                    WHERE _id = ?""",values)
            else: 
                cursor.execute("""
                    INSERT INTO PRODUTO_CONTAS (
                    [Modified Date], [Created Date], [Created By], id_empresa_text,
                    ativo_boolean, porcentagem_number, plano_de_contas_custom_subreceita,
                    tipo_plano_de_contas_option_tiposubreceita, unificador_text, visivel_boolean,
                    id_empresa_custom_empresa, id_produto_centro_decustos,
                    planos_de_custos_list_custom_produto_plano_de_custo, _id
                    )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
                    """,values)
            
            conn.commit()
        except Exception as e:
            print(f"Erro: {e}")
    conn.close()
    logging.info('Função obj3 Finalizada')

def att_bd_azure_obj4(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    for item in data:
        # Inicialização das variáveis
        Modified_Date_value = ''
        Created_Date_value = ''
        Created_By_value = ''
        apagado_boolean_value = ''
        banco_custom_bancos_value = ''
        cliente_custom_cliente1_value =''
        conciliado_boolean_value = ''
        contas_a_receber_custom_contas_a_receber_value = ''
        data_date_value = ''
        id_empresa_text_value = ''
        tipo_option_tipo_de_conta_value = ''
        valor_number_value = ''
        descricao_text_value = ''
        plano_de_contas_custom_subreceita_value = ''
        acrescimo_number_value = 0
        decrescimo_number_value = 0
        mes_number_value = ''
        ano_number_value = ''
        empresa_custom_empresa_value = ''
        migrado_boolean_value = ''
        _id_value = ''
        formatted_created_date = None
        formatted_data_date = None
        formatted_modified_date = None
        
        try:
            Modified_Date_value = item['Modified Date']
            Created_Date_value = item['Created Date']
            Created_By_value = item['Created By']
            apagado_boolean_value =item['apagado_boolean']
            banco_custom_bancos_value = item['banco_custom_bancos']
            cliente_custom_cliente1_value =item['cliente_custom_cliente1']
            conciliado_boolean_value = item['conciliado_boolean']
            contas_a_receber_custom_contas_a_receber_value = item['contas_a_receber_custom_contas_a_receber']
            data_date_value = item['data_date']
            id_empresa_text_value = item['id_empresa_text']
            tipo_option_tipo_de_conta_value = item['tipo_option_tipo_de_conta']
            valor_number_value = float(item['valor_number']) if item['valor_number'] else None
            descricao_text_value = item['descricao_text']
            plano_de_contas_custom_subreceita_value = item['plano_de_contas_custom_subreceita']
            acrescimo_number_value = item['acrescimo_number'] 
            decrescimo_number_value = item['decrescimo_number']
            mes_number_value = int(item['mes_number'])
            ano_number_value = int(item['ano_number'])
            empresa_custom_empresa_value = item['empresa_custom_empresa']
            migrado_boolean_value = item['migrado_boolean']
            _id_value = item['_id']

            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)
            data_date_obj = parse_custom_datetime(data_date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None
                
            if data_date_obj is not None:
                formatted_data_date = data_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_data_date = None
                
            if acrescimo_number_value is not None:
                acrescimo_number_value = float(acrescimo_number_value)
            if decrescimo_number_value is not None:
                decrescimo_number_value = float(decrescimo_number_value)
                
            cursor.execute("SELECT COUNT(*) FROM MOVIMENTACAO_FINANCEIRA WHERE _id=?", (_id_value,))
            row_count = cursor.fetchone()[0]
            print(f"Row count para {_id_value}: {row_count}")
            
            values = (
                formatted_modified_date, formatted_created_date, Created_By_value,apagado_boolean_value, banco_custom_bancos_value,cliente_custom_cliente1_value,
                conciliado_boolean_value,contas_a_receber_custom_contas_a_receber_value, formatted_data_date,id_empresa_text_value,tipo_option_tipo_de_conta_value,valor_number_value,
                descricao_text_value,plano_de_contas_custom_subreceita_value,acrescimo_number_value,decrescimo_number_value,
                mes_number_value,ano_number_value,empresa_custom_empresa_value,migrado_boolean_value, _id_value
            )
            
            if row_count > 0:
                cursor.execute("""
                    UPDATE MOVIMENTACAO_FINANCEIRA SET 
                    [Modified Date] = ?, [Created Date] = ?, [Created By] = ?, apagado_boolean = ?, banco_custom_bancos = ?, cliente_custom_cliente1 = ?,
                    conciliado_boolean = ?, contas_a_receber_custom_contas_a_receber = ?, data_date = ?, id_empresa_text = ?,
                    tipo_option_tipo_de_conta = ?, valor_number = ?, descricao_text = ?, plano_de_contas_custom_subreceita = ?,
                    acrescimo_number = ?, decrescimo_number = ?, mes_number = ?, ano_number = ?, empresa_custom_empresa = ?, migrado_boolean = ?
                    WHERE _id = ?""",values)
            else: 
                cursor.execute("""
                    INSERT INTO MOVIMENTACAO_FINANCEIRA (
                    [Modified Date], [Created Date], [Created By], apagado_boolean, banco_custom_bancos, cliente_custom_cliente1,
                    conciliado_boolean, contas_a_receber_custom_contas_a_receber, data_date, id_empresa_text,
                    tipo_option_tipo_de_conta, valor_number, descricao_text, plano_de_contas_custom_subreceita,
                    acrescimo_number, decrescimo_number, mes_number, ano_number, empresa_custom_empresa, migrado_boolean, _id
                    )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)    
                    """,values)
            
            conn.commit()
        except KeyError as e:
            print(f"Erro ao inserir dados: {e}")
            print(f"Dados problemáticos: {item}")
            pass
 
    conn.close()
 
def att_bd_azure_obj5(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    for item in data:
        # Inicialização das variáveis
        Created_By_value = ''
        Created_Date_value = None
        id_empresa_custom_empresa_value = ''
        id_empresa_text_value = ''
        Modified_Date_value = None
        nome_text_value = ''
        _id_value = ''
        try:
            Created_By_value = item['Created By']
            Created_Date_value = item['Created Date']
            id_empresa_custom_empresa_value = item['id_empresa_custom_empresa']
            id_empresa_text_value = item['id_empresa_text']
            Modified_Date_value = item['Modified Date']
            nome_text_value = item['nome_text']
            _id_value = item['_id']
           
            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None

            cursor.execute("SELECT COUNT(*) FROM CENTRO_DE_CUSTOS WHERE _id=?", (_id_value,))
            row_count = cursor.fetchone()[0]
            print(f"Row count para {_id_value}: {row_count}")
            values = (
                Created_By_value,formatted_created_date, id_empresa_custom_empresa_value,id_empresa_text_value,
                formatted_modified_date,nome_text_value, _id_value
            )
            
            if row_count > 0:
                cursor.execute("""
                    UPDATE CENTRO_DE_CUSTOS SET 
                    [Created By] = ?,[Created Date] = ?,id_empresa_custom_empresa = ?,id_empresa_text = ?,
                    [Modified Date] = ?, nome_text = ?
                    WHERE _id = ?""",values)
            else: 
                cursor.execute("""
                    INSERT INTO CENTRO_DE_CUSTOS (
                    [Created By],[Created Date],id_empresa_custom_empresa,id_empresa_text,
                    [Modified Date], nome_text, _id
                    )VALUES (?, ?, ?, ?, ?, ?, ?)     
                    """,values)
            
            conn.commit()
        except KeyError:
            pass
          
    conn.close()
    
def att_bd_azure_obj6(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    for item in data:
        # Inicialização das variáveis
        Modified_Date_value = None
        Created_Date_value = None
        Created_By_value = None
        id_empresa_text_value = ''
        subreceita_text_value = ''
        apagado_boolean_value = ''
        tiposub_option_tiposubreceita_value = ''
        subplanodecontas_text_value = ''
        empresa_custom_empresa_value = ''
        _id_value = ''
        
        try:
            Modified_Date_value = item['Modified Date']
            Created_Date_value = item['Created Date']
            Created_By_value = item['Created By']
            id_empresa_text_value = item['id_empresa_text']
            subreceita_text_value = item['subreceita_text']
            apagado_boolean_value = item['apagado_boolean']
            tiposub_option_tiposubreceita_value = item['tiposub_option_tiposubreceita']
            subplanodecontas_text_value = item['subplanodecontas_text']
            empresa_custom_empresa_value = item['empresa_custom_empresa']
            _id_value = item['_id']

            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None

            cursor.execute("SELECT COUNT(*) FROM SUBPLANODECONTAS WHERE _id=?", (_id_value,))
            row_count = cursor.fetchone()[0]
            print(f"Row count para {_id_value}: {row_count}")
            values = (
                formatted_modified_date, formatted_created_date, Created_By_value,id_empresa_text_value,subreceita_text_value,
                apagado_boolean_value,tiposub_option_tiposubreceita_value,subplanodecontas_text_value,empresa_custom_empresa_value, _id_value
            )
            
            if row_count > 0:
                cursor.execute("""
                    UPDATE SUBPLANODECONTAS SET 
                    [Modified Date] = ?, [Created Date] = ?, [Created By] = ?, id_empresa_text = ?, subreceita_text = ?,apagado_boolean = ?,
                    tiposub_option_tiposubreceita = ?, subplanodecontas_text = ?, empresa_custom_empresa = ?
                    WHERE _id = ?""",values)
            else: 
                cursor.execute("""
                    INSERT INTO SUBPLANODECONTAS (
                    [Modified Date], [Created Date], [Created By], id_empresa_text, subreceita_text,apagado_boolean,tiposub_option_tiposubreceita,
                    subplanodecontas_text, empresa_custom_empresa, _id
                    )VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)   
                    """,values)
            
            conn.commit()
            
        except KeyError:
            pass
 
    conn.close()
    
def att_bd_azure_obj7(data):
    server = os.getenv('SERVER')
    database = os.getenv('DB_AZ')
    username = os.getenv('NAME')
    password = os.getenv('PASSWORD')
    driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};UID={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
    cursor = conn.cursor()

    for item in data:
        # Inicialização das variáveis
        ativo_boolean_value = ''
        Created_By_value = ''
        Created_Date_value = None
        Modified_Date_value = None
        plano_de_custo_custom_plano_de_custo_value = ''
        porcentagem_number_value = 1
        _id_value = ''
        
        try:
            ativo_boolean_value = item['ativo_boolean']
            Created_By_value = item['Created By']
            Created_Date_value = item['Created Date']
            Modified_Date_value = item['Modified Date']
            plano_de_custo_custom_plano_de_custo_value = item['plano_de_custo_custom_plano_de_custo']
            porcentagem_number_value = item['porcentagem_number']
            _id_value = item['_id']
            
            created_date_obj = parse_custom_datetime(Created_Date_value)
            modified_date_obj = parse_custom_datetime(Modified_Date_value)

            if created_date_obj is not None:
                formatted_created_date = created_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_created_date = None

            if modified_date_obj is not None:
                formatted_modified_date = modified_date_obj.strftime('%Y-%m-%d %H:%M:%S')
            else:
                formatted_modified_date = None
                
            cursor.execute("SELECT COUNT(*) FROM PRODUTO_CENTRO_DE_CUSTO WHERE _id=?", (_id_value,))
            row_count = cursor.fetchone()[0]
            print(f"Row count para {_id_value}: {row_count}")
            values = (
                ativo_boolean_value,Created_By_value, formatted_created_date, formatted_modified_date,
                plano_de_custo_custom_plano_de_custo_value,porcentagem_number_value,_id_value
            )
            
            if row_count > 0:
                cursor.execute("""
                    UPDATE PRODUTO_CENTRO_DE_CUSTO SET 
                    ativo_boolean = ?,[Created By] = ?, [Created Date] = ?, [Modified Date] = ?,  
                    plano_de_custo_custom_plano_de_custo = ?,porcentagem_number = ?
                    WHERE _id = ?""",values)
            else: 
                cursor.execute("""
                    INSERT INTO PRODUTO_CENTRO_DE_CUSTO (
                    ativo_boolean,[Created By], [Created Date], [Modified Date],  
                    plano_de_custo_custom_plano_de_custo,porcentagem_number, _id
                    )VALUES (?, ?, ?, ?, ?, ?, ?)   
                    """,values)
            
            conn.commit()
                  
        except KeyError as e:
            print(f"Erro ao inserir dados: {e}")
            print(f"Dados problemáticos: {item}")
            pass
 
    conn.close()


#_____MAIN FUNCTION ACTION_____#
def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.now(timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('O temporizador está atrasado!')
        print("Timer atrasado")

    logging.info('A função de acionamento do temporizador Python foi executada em %s', utc_timestamp)
    print("Executado com sucesso")

response = requests.get(url_tg_plano_contas)
response = requests.get(url_tg_contas_a_pagar)
response = requests.get(url_tg_contas_a_receber)   
response = requests.get(url_movimentacao_financeira)  
response = requests.get(url_Centro_de_custos)  
response = requests.get(url_SubPlanodecontas)  
response = requests.get(url_produto_centro_de_custo)   
response.encoding = 'utf-8'  # Definir a codificação como UTF-8


###___***Exemplo de chamada das funções***___###

#_____Chamada da pasta API_IN.py_____#
#dados = overflowdata_produto_contas(url_tg_plano_contas)
#dados_contas_a_pagar = ContasPagar(url_tg_contas_a_pagar)
#dados_contas_a_receber = ContasReceber(url_tg_contas_a_receber)
#dados_4=Movimentacao_financeira(url_movimentacao_financeira)
#dados_5=Centro_de_custos(url_Centro_de_custos)
#dados_6=SubPlanodecontas(url_SubPlanodecontas)
#dados_7=produto_centro_de_custo(url_produto_centro_de_custo)
dados_att_3 = produto_plano_de_contas_5pg(url_tg_plano_contas)
dados_contas_a_pagar = ContasPagar_5pg(url_tg_contas_a_pagar)
dados_contas_a_receber = ContasReceber_5pg(url_tg_contas_a_receber)
dados_att_4=movimentacao_financeira_5pg(url_movimentacao_financeira)
dados_att_5=centro_de_custos_5pg(url_Centro_de_custos)
dados_att_6=sub_planodecontas_5pg(url_SubPlanodecontas)
dados_att_7=produto_centro_de_custos_5pg(url_produto_centro_de_custo)

#______SALVA TODOS OS DADOS DO BANCO DE DADOS (BACKUP COMPLETO)______# 
#insert_into_databaseFULL_obj3(dados)
#insert_into_databaseFULL_obj2(dados_contas_a_pagar)
#insert_into_databaseFULL_obj1(dados_contas_a_receber)
#inser_into_database_obj5(dados_5)
#inser_into_database_obj6(dados_6)
#inser_into_database_obj7(dados_7)
#inser_into_database_obj4(dados_4)

#______ATUALIZAR O BANCO DE DADOS_____#
att_bd_azure_obj7(dados_att_7)
att_bd_azure_obj6(dados_att_6) 
att_bd_azure_obj5(dados_att_5) 
att_bd_azure_obj4(dados_att_4) 
att_bd_azure_obj3(dados_att_3)
att_bd_azure_obj2(dados_contas_a_pagar)
att_bd_azure_obj1(dados_contas_a_receber)

#_____SALVAR DADOS TXT_____#
#filename = 'dados_salvos.txt'
#verificar_API_and_save(dados, filename)


logging.info("Script finalizado com sucesso")
    