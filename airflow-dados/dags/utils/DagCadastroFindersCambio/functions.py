import json
from utils.OneGraph.mail import Mail
from utils.DagCadastroFindersCambio.context_script import gera_context_script
import pandas as pd


def recebe_resposta_lambda_hubspot(evento_lambda: str) -> str:
    """Atualiza base de finders de câmbio.

    Args:
        evento_lambda (str): String do evento que identifica qual webhook do hubspot foi acionado.

    Returns:
        evento_lambda (str): String do evento lambda que decide qual dos tratamentos do pipeline será realizado.

    """

    eventos_possiveis = ["fluxo_finder", "fluxo_indicacao"]

    if evento_lambda not in eventos_possiveis:
        raise Exception(
            f"Origem '{evento_lambda}' desconhecida! Verfique de onde veio a chamada!"
        )
    else:
        return evento_lambda


def incrementa_base_cadastro_finder(datetime_arquivo: str, id_arquivo: str) -> None:
    """Atualiza base de finders de câmbio.

    Args:
        datetime_arquivo (str): Datetime de geração do arquivo com os dados brutos do hubspot.
        id_arquivo (str): ID do arquivo com dados brutos do hubspot.
    """
    context_script = gera_context_script()

    df_finders_historico = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/cambio/finders/lista_finders"
    )
    df_novo_finder = pd.read_json(
        f"s3://{context_script['bucket_lake_landing']}/api/hubspot/cambio/finders/cadastro_finder_cambio/dados/cadastro_finder_cambio_{datetime_arquivo}_{id_arquivo}.json",
    )
    dict_novo_finder = json.loads(df_novo_finder["body"][0])
    df_novo_finder = pd.DataFrame([dict_novo_finder])
    df_novo_finder = df_novo_finder.loc[
        :, ["id_hubspot_negocio_finder", "nome_negocio"]
    ]
    df_novo_finder["id_hubspot_negocio_finder"] = df_novo_finder[
        "id_hubspot_negocio_finder"
    ].astype(str)
    df_novo_finder["data_cadastro"] = context_script["datetime_hoje_brasil"]

    df_finders_atualizado = pd.concat([df_finders_historico, df_novo_finder])
    df_finders_atualizado = df_finders_atualizado.drop_duplicates(
        subset=["id_hubspot_negocio_finder", "nome_negocio"], keep="last"
    )

    print(df_finders_atualizado)
    print(
        df_finders_atualizado["id_hubspot_negocio_finder"].to_list(),
        df_finders_atualizado["nome_negocio"].to_list(),
        df_finders_atualizado["data_cadastro"].to_list(),
    )

    df_finders_atualizado.to_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/cambio/finders/lista_finders/lista_finders.parquet"
    )

    print("Base de finders atualizada!")
    _notifica_mesa_cambio("cadastro_finder_cambio")


def incrementa_base_cadastro_indicacao_finder(
    datetime_arquivo: str, id_arquivo: str
) -> None:
    """Atualiza base de clientes indicados por finders de câmbio.

    Args:
        datetime_arquivo (str): Datetime de geração do arquivo com os dados brutos do hubspot.
        id_arquivo (str): ID do arquivo com dados brutos do hubspot.
    """

    context_script = gera_context_script()

    df_indicacao_finders_historico = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/cambio/finders/lista_clientes"
    )
    df_nova_indicacao_finder = pd.read_json(
        f"s3://{context_script['bucket_lake_landing']}/api/hubspot/cambio/finders/cadastro_indicacao_finder_cambio/dados/cadastro_indicacao_finder_cambio_{datetime_arquivo}_{id_arquivo}.json",
    )
    dict_nova_indicacao_finder = json.loads(df_nova_indicacao_finder["body"][0])
    df_nova_indicacao_finder = pd.DataFrame([dict_nova_indicacao_finder])

    df_nova_indicacao_finder = df_nova_indicacao_finder.loc[
        :, ["id_hubspot_negocio_cliente", "nome_negocio"]
    ]
    df_nova_indicacao_finder["id_hubspot_negocio_cliente"] = df_nova_indicacao_finder[
        "id_hubspot_negocio_cliente"
    ].astype(str)
    df_nova_indicacao_finder["data_cadastro"] = context_script["datetime_hoje_brasil"]

    df_indicacao_finders_atualizado = pd.concat(
        [df_indicacao_finders_historico, df_nova_indicacao_finder]
    )
    df_indicacao_finders_atualizado = df_indicacao_finders_atualizado.drop_duplicates(
        subset=["id_hubspot_negocio_cliente", "nome_negocio"], keep="last"
    )

    print(df_indicacao_finders_atualizado)
    print(
        df_indicacao_finders_atualizado["id_hubspot_negocio_cliente"].to_list(),
        df_indicacao_finders_atualizado["nome_negocio"].to_list(),
        df_indicacao_finders_atualizado["data_cadastro"].to_list(),
    )

    df_indicacao_finders_atualizado.to_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/cambio/clientes/lista_clientes/lista_clientes.parquet"
    )

    print("Base de finders atualizada!")
    _notifica_mesa_cambio("cadastro_indicacao_finder_cambio")


def _notifica_mesa_cambio(fluxo: str):
    """Notifica mesma de câmbio de que há um novo finder para ser cadastrado.

    Args:
        fluxo (str): Qual dos fluxos de cadastro se concretizou.
    """

    context_script = gera_context_script()

    if fluxo == "cadastro_finder_cambio":
        titulo = "Novo finder"
    elif fluxo == "cadastro_indicacao_finder_cambio":
        titulo = "Novo cliente indicado por finder"
    else:
        raise Exception(
            "Não temos esse fluxo para cadastro, verifique o que ocorreu (provável alteração nas funções de atualização de base)."
        )

    client_id = context_script["dict_credencias_microsft"]["client_id"]
    client_secret = context_script["dict_credencias_microsft"]["client_secret"]
    tenant_id = context_script["dict_credencias_microsft"]["tenant_id"]
    subject = f"{titulo} ativado"
    recipients = ["cambio@investimentos.one", "gustavo.sousa@investimentos.one"]
    sender_id = context_script["id_microsoft_tecnologia"]

    # ## DESCOMENTE PARA RODAR LOCAL
    # subject = f"[TESTE] {titulo} ativado"
    # recipients = ["gustavo.sousa@investimentos.one"]
    # sender_id = context_script["id_microsoft_tecnologia"]

    mail_microsoft = Mail(
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
        subject=subject,
        recipients=recipients,
        sender_id=sender_id,
    )

    mail_microsoft.paragraph(f"Prezados, {mail_microsoft.Greetings.time_of_day()}.")
    mail_microsoft.line_break()
    mail_microsoft.paragraph(
        "Este é um e-mail automático do fluxo de finders de câmbio."
    )
    if fluxo == "cadastro_finder_cambio":
        link_app_cadastro = context_script["link_app_cadastro_finder"]
        mail_microsoft.paragraph("Há um novo finder ativo disponível para cadastro.")
        mail_microsoft.paragraph(
            f'Realize o cadastro <a href="{link_app_cadastro}">neste link</a>.'
        )
    if fluxo == "cadastro_indicacao_finder_cambio":
        link_app_cadastro = context_script["link_app_cadastro_indicacao_finder"]
        mail_microsoft.paragraph(
            "Há um novo cliente indicado por finder ativo disponível para cadastro."
        )
        mail_microsoft.paragraph(
            f'Realize o cadastro <a href="{link_app_cadastro}">neste link</a>.'
        )
    mail_microsoft.line_break()
    mail_microsoft.paragraph(
        "Quaisquer dúvidas entre em contato com a equipe de tecnologia."
    )
    mail_microsoft.send_mail()
