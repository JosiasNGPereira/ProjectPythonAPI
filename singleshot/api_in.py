from flask import Flask, jsonify, request
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
#url_tg = f"{url_base}/{obj2}?cursor={cont_pg}"
url_tg = f"{url_base}/{obj3}?cursor={cont_pg}"

def produto_plano_de_contas(url_tg):
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


#buscar na api do bubble contas a pagar e receber  
def ContasPagarReceber(url_tg):
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

def verificar_API(dados):
    
    if dados is not None:
        if len(dados) > 0:
            for estrutura in dados:
                print(estrutura)
        else:
            print("Nenhum dado encontrado.")
    else:
        print("Falha ao acessar a API.")
         
#dados = ContasPagarReceber(url_tg) 
dados = produto_plano_de_contas(url_tg)
verificar_API(dados)



print ("TESTE AZURE DEU CERTO")



