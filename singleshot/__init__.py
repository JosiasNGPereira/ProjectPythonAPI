import datetime
import logging
from api_in import produto_plano_de_contas
import os
import requests

import azure.functions as func

obj3 = "produto_plano_de_contas"
url_base = os.getenv('DB_UR')
cont_pg = 0
url_tg = f"{url_base}/{obj3}?cursor={cont_pg}"
def verificar_API(dados):
    
    if dados is not None:
        if len(dados) > 0:
            for estrutura in dados:
                print(estrutura)
        else:
            print("Nenhum dado encontrado.")
    else:
        print("Falha ao acessar a API.")


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
verificar_API(dados)