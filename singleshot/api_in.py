 #from flask import Flask, jsonify, request
from dotenv import load_dotenv
import requests
import json
import os

load_dotenv()

obj1 = "Contas a pagar"
obj2 = "Contas a receber"
obj3 = "produto_plano_de_contas"

url_base = os.getenv('DB_UR')
cont_pg = 0
url_produto = f"{url_base}/{obj3}?cursor={cont_pg}"
url_pagar = f"{url_base}/{obj1}?cursor={cont_pg}"
url_receber = f"{url_base}/{obj2}?cursor={cont_pg}"

def produto_plano_de_contas(url_tg): # Percorre toda a API do Bubble (todas as paginas)
    cont_pg = 0
    estruturas = []
    
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
                    break

                cont_pg += 100
                url_tg = f"{url_base}/{obj3}?cursor={cont_pg}"
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da API")
            return []

    return estruturas

def ContasPagar(url_tg): # Percorre toda a API do Bubble (todas as paginas)
    cont_pg = 0
    estruturas = []
    
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
                        "compet_ncia_date": str(item.get("compet_ncia_date", "")),
                        "id_empresa_text": str(item.get("id_empresa_text", "")),
                        "pago_boolean": str(item.get("pago_boolean", "")),
                        "repeti__es_number": str(item.get("repeti__es_number", "")),
                        "valor_number": str(item.get("valor_number", "")),
                        "vencimento_date": str(item.get("vencimento_date", "")),
                        "forma_de_pagamento_text": str(item.get("forma_de_pagamento_text", "")),
                        "apagado_boolean": str(item.get("apagado_boolean", "")),
                        "entrada_boolean": str(item.get("entrada_boolean", "")),
                        "cliente_custom_cliente1": str(item.get("cliente_custom_cliente1", "")),
                        "parcela_number": str(item.get("parcela_number", "")),
                        "pedido_de_venda_custom_pedido_de_venda": str(item.get("pedido_de_venda_custom_pedido_de_venda", "")),
                        "valor_inicial_number": str(item.get("valor_inicial_number", "")),
                        "ativo_boolean": str(item.get("ativo_boolean", "")),
                        "id_cash_text": str(item.get("id_cash_text", "")),
                        "agrupado_boolean": str(item.get("agrupado_boolean", "")),
                        "parcela_name_text": str(item.get("parcela_name_text", "")),
                        "mes_number": str(item.get("mes_number", "")),
                        "ano_number": str(item.get("ano_number", "")),
                        "produto_plano_de_contas_list_custom_produto_plano_de_contas": str(item.get("produto_plano_de_contas_list_custom_produto_plano_de_contas", "")),
                        "empresa1_custom_empresa": str(item.get("empresa1_custom_empresa", "")),
                        "migrado_boolean": str(item.get("migrado_boolean", "")),
                        "_id": str(item.get("_id", ""))
                    }
                    estruturas.append(estrutura) 

                remaining = data.get("response", {}).get("remaining", 0)
                if remaining == 0:
                    break

                cont_pg += 100
                url_tg = f"{url_base}/{obj2}?cursor={cont_pg}"
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da API")
            return []

    return estruturas

def ContasReceber(url_tg): # Percorre toda a API do Bubble (todas as paginas)
    cont_pg = 0
    estruturas = []
    
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
                        "compet_ncia_date": str(item.get("compet_ncia_date", "")),
                        "id_empresa_text": str(item.get("id_empresa_text", "")),
                        "pago_boolean": str(item.get("pago_boolean", "")),
                        "repeti__es_number": str(item.get("repeti__es_number", "")),
                        "valor_number": str(item.get("valor_number", "")),
                        "vencimento_date": str(item.get("vencimento_date", "")),
                        "forma_de_pagamento_text": str(item.get("forma_de_pagamento_text", "")),
                        "banco_text": str(item.get("banco_text", "")),
                        "data_do_pagamento_date": str(item.get("data_do_pagamento_date", "")),
                        "apagado_boolean": str(item.get("apagado_boolean", "")),
                        "entrada_boolean": str(item.get("entrada_boolean", "")),
                        "cliente_custom_cliente1": str(item.get("cliente_custom_cliente1", "")),
                        "parcela_number": str(item.get("parcela_number", "")),
                        "pedido_de_venda_custom_pedido_de_venda": str(item.get("pedido_de_venda_custom_pedido_de_venda", "")),
                        "plano_de_contas2_custom_subreceita": str(item.get("plano_de_contas2_custom_subreceita", "")),
                        "valor_inicial_number": str(item.get("valor_inicial_number", "")),
                        "ativo_boolean": str(item.get("ativo_boolean", "")),
                        "id_cash_text": str(item.get("id_cash_text", "")),
                        "agrupado_boolean": str(item.get("agrupado_boolean", "")),
                        "parcela_name_text": str(item.get("parcela_name_text", "")),
                        "mes_number": str(item.get("mes_number", "")),
                        "ano_number": str(item.get("ano_number", "")),
                        "produto_plano_de_contas_list_custom_produto_plano_de_contas": str(item.get("produto_plano_de_contas_list_custom_produto_plano_de_contas", "")),
                        "empresa1_custom_empresa": str(item.get("empresa1_custom_empresa", "")),
                        "migrado_boolean": str(item.get("migrado_boolean", "")),
                        "_id": str(item.get("_id", ""))
                    }
                    estruturas.append(estrutura) 

                remaining = data.get("response", {}).get("remaining", 0)
                if remaining == 0:
                    break

                cont_pg += 100
                url_tg = f"{url_base}/{obj2}?cursor={cont_pg}"
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da API")
            return []

    return estruturas

def verificar_API_and_save(dados, filename): # Salva a busca da API em um .TXT para verificação
    with open(filename, 'w', encoding='utf-8') as file:
        if dados is not None:
            if len(dados) > 0:
                for estrutura in dados:
                    file.write(str(estrutura) + '\n')  # Escreve cada estrutura no arquivo
            else:
                file.write("Nenhum dado encontrado.\n")
        else:
            file.write("Falha ao acessar a API.\n")

def verificar_API(dados): # imprime na tela a busca da API (Mostra apenas +- 700 itens na tela)
    
    if dados is not None:
        if len(dados) > 0:
            for estrutura in dados:
                print(estrutura)
        else:
            print("Nenhum dado encontrado.")
    else:
        print("Falha ao acessar a API.")
   
def produto_plano_de_contas_5pg(url):# Pecorre apenas a primeira pagina da API Bubble = 500 itens
    estruturas = []
    num_pg=5
    cont_paginas = 0
    cursor = None

    for _ in range(num_pg):
        if cursor:
            url = f"{url}&cursor={cont_paginas}"

        response = requests.get(url)
        response.encoding = 'utf-8'

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

                cursor = data.get("response", {}).get("cursor")
                cont_paginas+=100 
            else:
                break
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da API")
            return []

    return estruturas

def ContasPagar_5pg(url, num_pg=5): # Pecorre apenas a primeira pagina da API Bubble = 500 itens
    estruturas = []
    num_pg=5
    cont_paginas = 0
    cursor = None

    for _ in range(num_pg):
        if cursor:
            url = f"{url}&cursor={cont_paginas}"

        response = requests.get(url)
        response.encoding = 'utf-8'

        try:
            data = response.json()
            if "response" in data and "results" in data["response"]:
                results = data["response"]["results"]
                for item in results:
                    estrutura = {
                        "Modified Date": str(item.get("Modified Date", "")),
                        "Created Date": str(item.get("Created Date", "")),
                        "Created By": str(item.get("Created By", "")),
                        "compet_ncia_date": str(item.get("compet_ncia_date", "")),
                        "id_empresa_text": str(item.get("id_empresa_text", "")),
                        "pago_boolean": str(item.get("pago_boolean", "")),
                        "repeti__es_number": str(item.get("repeti__es_number", "")),
                        "valor_number": str(item.get("valor_number", "")),
                        "vencimento_date": str(item.get("vencimento_date", "")),
                        "forma_de_pagamento_text": str(item.get("forma_de_pagamento_text", "")),
                        "apagado_boolean": str(item.get("apagado_boolean", "")),
                        "entrada_boolean": str(item.get("entrada_boolean", "")),
                        "cliente_custom_cliente1": str(item.get("cliente_custom_cliente1", "")),
                        "parcela_number": str(item.get("parcela_number", "")),
                        "pedido_de_venda_custom_pedido_de_venda": str(item.get("pedido_de_venda_custom_pedido_de_venda", "")),
                        "valor_inicial_number": str(item.get("valor_inicial_number", "")),
                        "ativo_boolean": str(item.get("ativo_boolean", "")),
                        "id_cash_text": str(item.get("id_cash_text", "")),
                        "agrupado_boolean": str(item.get("agrupado_boolean", "")),
                        "parcela_name_text": str(item.get("parcela_name_text", "")),
                        "mes_number": str(item.get("mes_number", "")),
                        "ano_number": str(item.get("ano_number", "")),
                        "produto_plano_de_contas_list_custom_produto_plano_de_contas": str(item.get("produto_plano_de_contas_list_custom_produto_plano_de_contas", "")),
                        "empresa1_custom_empresa": str(item.get("empresa1_custom_empresa", "")),
                        "migrado_boolean": str(item.get("migrado_boolean", "")),
                        "_id": str(item.get("_id", ""))
                    }
                    estruturas.append(estrutura)

                cursor = data.get("response", {}).get("cursor")
                cont_paginas+=100 
            else:
                break
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da API")
            return []

    return estruturas 

def ContasReceber_5pg(url, num_pg=5): # Pecorre apenas a primeira pagina da API Bubble = 500 itens
    estruturas = []
    num_pg=5
    cont_paginas = 0
    cursor = None

    for _ in range(num_pg):
        if cursor:
            url = f"{url}&cursor={cont_paginas}"

        response = requests.get(url)
        response.encoding = 'utf-8'

        try:
            data = response.json()
            if "response" in data and "results" in data["response"]:
                results = data["response"]["results"]
                for item in results:
                    estrutura = {
                        "Modified Date": str(item.get("Modified Date", "")),
                        "Created Date": str(item.get("Created Date", "")),
                        "Created By": str(item.get("Created By", "")),
                        "compet_ncia_date": str(item.get("compet_ncia_date", "")),
                        "id_empresa_text": str(item.get("id_empresa_text", "")),
                        "pago_boolean": str(item.get("pago_boolean", "")),
                        "repeti__es_number": str(item.get("repeti__es_number", "")),
                        "valor_number": str(item.get("valor_number", "")),
                        "vencimento_date": str(item.get("vencimento_date", "")),
                        "forma_de_pagamento_text": str(item.get("forma_de_pagamento_text", "")),
                        "banco_text": str(item.get("banco_text", "")),
                        "data_do_pagamento_date": str(item.get("data_do_pagamento_date", "")),
                        "apagado_boolean": str(item.get("apagado_boolean", "")),
                        "entrada_boolean": str(item.get("entrada_boolean", "")),
                        "cliente_custom_cliente1": str(item.get("cliente_custom_cliente1", "")),
                        "parcela_number": str(item.get("parcela_number", "")),
                        "pedido_de_venda_custom_pedido_de_venda": str(item.get("pedido_de_venda_custom_pedido_de_venda", "")),
                        "plano_de_contas2_custom_subreceita": str(item.get("plano_de_contas2_custom_subreceita", "")),
                        "valor_inicial_number": str(item.get("valor_inicial_number", "")),
                        "ativo_boolean": str(item.get("ativo_boolean", "")),
                        "id_cash_text": str(item.get("id_cash_text", "")),
                        "agrupado_boolean": str(item.get("agrupado_boolean", "")),
                        "parcela_name_text": str(item.get("parcela_name_text", "")),
                        "mes_number": str(item.get("mes_number", "")),
                        "ano_number": str(item.get("ano_number", "")),
                        "produto_plano_de_contas_list_custom_produto_plano_de_contas": str(item.get("produto_plano_de_contas_list_custom_produto_plano_de_contas", "")),
                        "empresa1_custom_empresa": str(item.get("empresa1_custom_empresa", "")),
                        "migrado_boolean": str(item.get("migrado_boolean", "")),
                        "_id": str(item.get("_id", ""))
                    }
                    estruturas.append(estrutura) 

                cursor = data.get("response", {}).get("cursor")
                cont_paginas+=100 
            else:
                break
        except json.JSONDecodeError:
            print("Erro ao decodificar JSON da API")
            return []

    return estruturas 


ContasReceber(url_receber)          
#ContasPagar(url_pagar) 
#produto_plano_de_contas(url_tg)
#verificar_API(dados)
#filename = 'dados_salvos.txt'
#verificar_API_and_save(dados, filename)

#dados1 = produto_plano_de_contas_5pg(url_produto)
#dados2 = ContasPagarReceber_5pg(url_pagar, num_pg=5)
#dados3 = ContasReceber_5pg(url_receber, num_pg=5)

#print(len(dados1))
#print(len(dados2))
#print(len(dados3))

#verificar_API(dados2)







