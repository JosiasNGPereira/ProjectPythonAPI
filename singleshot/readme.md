# TimerTrigger - Python

The `TimerTrigger` makes it incredibly easy to have your functions executed on a schedule. This sample demonstrates a simple use case of calling your function every 5 minutes.

## How it works

For a `TimerTrigger` to work, you provide a schedule in the form of a [cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression)(See the link for full details). A cron expression is a string with 6 separate expressions which represent a given schedule via patterns. The pattern we use to represent every 5 minutes is `0 */5 * * * *`. This, in plain text, means: "When seconds is equal to 0, minutes is divisible by 5, for any hour, day of the month, month, day of the week, or year".

## Learn more

## Configs

Configurado para rodar todo dia as 01:30 a.m programado em python 3.10.09 

## Functions

Consultar o banco de dados da TG fazendo um backup completo do financeiro(contas a pagar/recer e produto plano de contas).
Chamar estas funções apenas quando necessário, para prevenção e manutenção do banco de dados da Azure.
Funções que são chamadas com o TimerTrigger, consultar apenas as 5 primeiras paginas da API do Bubblo consumindo assim menos consumo de WU,
insert/update de novas contas/contas editadas, tudo isso será inserido no banco de dados da Azure.
Apos este ciclo o banco de dados ficara atualizado conforme o banco do Bubble.

## Functions externas 

Passamos o banco de dados da Azure para o PowerBi gerar relátorios 