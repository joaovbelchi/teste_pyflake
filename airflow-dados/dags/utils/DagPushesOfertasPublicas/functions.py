import pandas as pd
from utils.OneGraph.mail import Mail
from utils.usual_data_transformers import formata_numero_para_texto_monetario
from utils.Commons.DagsReservasOfertasPublicasPushesOfertasPublicas.context_script import (
    gera_context_script,
)


def envia_email_mesas_pushes_pendentes() -> None:
    """Envio e-mail de pushes para as mesas.

    Args:
        context_script (dict): Contexto de execução do script.
    """

    context_script = gera_context_script()

    df_emails_pushes = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/btg/onedrive/pushes/base_emails_pushes"
    )

    df_emails_pushes_consolidado = pd.DataFrame(
        {
            "NUM CONTAS": [df_emails_pushes["Conta"].count()],
            "VALOR TOTAL": [
                f"R$ {formata_numero_para_texto_monetario(df_emails_pushes['VALOR'].sum())}"
            ],
        }
    )

    df_emails_pushes = df_emails_pushes.loc[
        df_emails_pushes["STATUS"].str.contains("pendente", case=False),
        [
            "Conta",
            "TIPO DE OPERAÇÃO",
            "DATA LIMITE",
            "TITULAR",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "NOME DA OFERTA",
            "ATIVO",
            "STATUS",
            "VALOR",
        ],
    ]
    df_emails_pushes["ATIVO"] = df_emails_pushes["ATIVO"].fillna("-")
    df_emails_pushes["TITULAR"] = df_emails_pushes["TITULAR"].str.split(" ").str[0]
    df_emails_pushes["VALOR"] = df_emails_pushes["VALOR"].apply(
        lambda x: "R$ " + formata_numero_para_texto_monetario(x)
    )
    df_emails_pushes = df_emails_pushes.rename(
        columns={
            "Conta": "CONTA",
            "Comercial": "COMERCIAL",
            "Operacional RF": "ADVISOR",
            "Operacional RV": "TRADER",
            "TITULAR": "NOME",
        }
    ).sort_values(by="ADVISOR")

    assunto_email = f"Pushes pendentes - {context_script['datetime_hoje_brasil'].hour}h"
    lista_destinatarios = [
        "mesarv@investimentos.one",
        "advisors@investimentos.one",
        "gustavo.sousa@investimentos.one",
    ]
    remetente = context_script["id_microsoft_backoffice"]

    # RODAR LOCAL
    # lista_destinatarios = ["gustavo.sousa@investimentos.one"]
    # remetente = "0489e923-5bfc-4173-9820-649f8e1772d6"

    mail_microsoft = Mail(
        context_script["dict_credencias_microsft"]["client_id"],
        context_script["dict_credencias_microsft"]["client_secret"],
        context_script["dict_credencias_microsft"]["tenant_id"],
        assunto_email,
        lista_destinatarios,
        remetente,
    )
    mail_microsoft.paragraph(f"Prezados(as), {mail_microsoft.Greetings.time_of_day()}.")
    mail_microsoft.paragraph(
        "Este é um email automatizado do backoffice com informações de pushes pendentes dos nossos clientes."
    )
    mail_microsoft.paragraph("Total consolidado dos pushes pendentes:")
    mail_microsoft.table(
        df_emails_pushes_consolidado,
        pgnr_textual_number=["VALOR"],
    )
    mail_microsoft.paragraph("Pushes pendentes por conta:")
    mail_microsoft.table(
        df_emails_pushes,
        pgnr_textual_number=["VALOR"],
    )
    mail_microsoft.paragraph("Quaisquer dúvidas basta responder a este mesmo e-mail!")
    mail_microsoft.table_as_xlsx(
        f"pushes_pendentes_{context_script['datetime_hoje_brasil'].strftime('%Y%m%d')}.xlsx",
        df_emails_pushes,
    )
    mail_microsoft.send_mail()


def envia_email_comerciais_pushes_pendentes() -> None:
    """Envio e-mail de pushes para os comerciais.

    Args:
        context_script (dict): Contexto de execução do script.
    """

    context_script = gera_context_script()

    df_emails_pushes = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/btg/onedrive/pushes/base_emails_pushes"
    )

    df_emails_pushes = df_emails_pushes.loc[
        df_emails_pushes["STATUS"].str.contains("pendente", case=False)
    ]
    df_lista_comerciais = df_emails_pushes.loc[
        :, ["Comercial", "E-mail Comercial"]
    ].drop_duplicates()
    df_emails_pushes = df_emails_pushes.loc[
        :,
        [
            "Conta",
            "TIPO DE OPERAÇÃO",
            "DATA LIMITE",
            "TITULAR",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "NOME DA OFERTA",
            "ATIVO",
            "STATUS",
            "VALOR",
        ],
    ]
    df_emails_pushes["ATIVO"] = df_emails_pushes["ATIVO"].fillna("-")
    df_emails_pushes["TITULAR"] = df_emails_pushes["TITULAR"].str.split(" ").str[0]
    df_emails_pushes["VALOR"] = df_emails_pushes["VALOR"].apply(
        lambda x: "R$ " + formata_numero_para_texto_monetario(x)
    )
    df_emails_pushes = df_emails_pushes.rename(
        columns={
            "Conta": "CONTA",
            "Comercial": "COMERCIAL",
            "Operacional RF": "ADVISOR",
            "Operacional RV": "TRADER",
            "TITULAR": "NOME",
        }
    )

    assunto_email = f"Pushes pendentes - {context_script['datetime_hoje_brasil'].hour}h"

    for comercial in df_lista_comerciais.itertuples():
        nome_comercial = comercial[1]
        email_comercial = comercial[2]
        primeiro_nome_comercial = nome_comercial.split(" ")[0].capitalize()

        df_email_comercial = df_emails_pushes.loc[
            df_emails_pushes["COMERCIAL"] == nome_comercial
        ]

        lista_destinatarios = [email_comercial, "gustavo.sousa@investimentos.one"]
        remetente = context_script["id_microsoft_backoffice"]

        # RODAR LOCAL
        # email_comercial = "gustavo.sousa@investimentos.one"
        # lista_destinatarios = [email_comercial, "gustavo.sousa@investimentos.one"]
        # remetente = "0489e923-5bfc-4173-9820-649f8e1772d6"

        mail_microsoft = Mail(
            context_script["dict_credencias_microsft"]["client_id"],
            context_script["dict_credencias_microsft"]["client_secret"],
            context_script["dict_credencias_microsft"]["tenant_id"],
            assunto_email,
            lista_destinatarios,
            remetente,
        )
        mail_microsoft.paragraph(
            f"Prezado(a) {primeiro_nome_comercial}, {mail_microsoft.Greetings.time_of_day()}."
        )
        mail_microsoft.paragraph(
            "Este é um email automatizado do backoffice com informações de pushes pendentes dos nossos clientes."
        )
        # formatar tabela aqui
        mail_microsoft.table(df_email_comercial, pgnr_textual_number=["VALOR"])
        mail_microsoft.paragraph(
            "Quaisquer dúvidas basta responder a este mesmo e-mail!"
        )
        mail_microsoft.send_mail()
