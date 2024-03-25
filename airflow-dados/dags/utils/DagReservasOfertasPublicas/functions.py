import pandas as pd
from utils.OneGraph.mail import Mail
from utils.usual_data_transformers import formata_numero_para_texto_monetario
from utils.Commons.DagsReservasOfertasPublicasPushesOfertasPublicas.context_script import (
    gera_context_script,
)


def envia_email_mesas_reservas() -> None:
    """Envio e-mail de reservas para as mesas."""
    df_emails_reservas = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/btg/onedrive/reservas/base_emails_reservas"
    )

    context_script = gera_context_script()

    df_emails_reservas = df_emails_reservas.loc[
        :,
        [
            "Conta",
            "TITULAR",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "NOME DA OFERTA",
            "ATIVO",
            "FIM DA RESERVA",
            "VALOR POSSÍVEL",
            "MODALIDADE",
        ],
    ]

    df_emails_reservas_consolidado = pd.DataFrame(
        {
            "NUM CONTAS": [df_emails_reservas["Conta"].count()],
            "VALOR TOTAL": [
                f"R$ {formata_numero_para_texto_monetario(df_emails_reservas['VALOR POSSÍVEL'].sum())}"
            ],
        }
    )

    df_emails_reservas["TITULAR"] = df_emails_reservas["TITULAR"].str.split(" ").str[0]
    df_emails_reservas["VALOR POSSÍVEL"] = df_emails_reservas["VALOR POSSÍVEL"].apply(
        lambda x: "R$ " + formata_numero_para_texto_monetario(x)
    )
    df_emails_reservas["ATIVO"] = df_emails_reservas["ATIVO"].fillna("-")

    df_emails_reservas = df_emails_reservas.rename(
        columns={
            "Conta": "CONTA",
            "Comercial": "COMERCIAL",
            "Operacional RF": "ADVISOR",
            "Operacional RV": "TRADER",
            "TITULAR": "NOME",
        }
    ).sort_values(by="ADVISOR")

    assunto_email = f"Reservas - {context_script['datetime_hoje_brasil'].hour}h"
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
        "Este é um email automatizado do backoffice com informações de reserva dos nossos clientes."
    )
    mail_microsoft.paragraph("Total consolidado das reservas:")
    mail_microsoft.table(
        df_emails_reservas_consolidado,
        pgnr_textual_number=["VALOR"],
    )
    mail_microsoft.paragraph("Reservas por conta:")
    mail_microsoft.table(
        df_emails_reservas,
        pgnr_textual_number=["VALOR"],
    )
    mail_microsoft.paragraph("Quaisquer dúvidas basta responder a este mesmo e-mail!")
    mail_microsoft.table_as_xlsx(
        f"reservas_{context_script['datetime_hoje_brasil'].strftime('%Y%m%d')}.xlsx",
        df_emails_reservas,
    )
    mail_microsoft.send_mail()


def envia_email_reservas_ranking() -> None:
    """Envio e-mail de ranking reservas para as mesas e os comerciais."""

    context_script = gera_context_script()

    df_emails_reservas = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/btg/onedrive/reservas/base_emails_reservas"
    )

    df_emails_reservas = df_emails_reservas.loc[
        :,
        [
            "Conta",
            "TITULAR",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
            "FIM DA RESERVA",
            "VALOR POSSÍVEL",
            "MODALIDADE",
            "Tipo",
            "VALOR DESEJADO",
        ],
    ]

    df_emails_reservas["TITULAR"] = df_emails_reservas["TITULAR"].str.split(" ").str[0]
    df_emails_reservas = df_emails_reservas.rename(
        columns={
            "Conta": "CONTA",
            "Comercial": "COMERCIAL",
            "Operacional RF": "ADVISOR",
            "Operacional RV": "TRADER",
            "TITULAR": "NOME",
        }
    )

    total_reservado = f"R$ {formata_numero_para_texto_monetario(df_emails_reservas['VALOR DESEJADO'].sum())}"

    categorias_comercias = (
        df_emails_reservas["Tipo"]
        .sort_values(ascending=True)
        .drop_duplicates()
        .to_list()
    )
    for categoria in categorias_comercias:
        df_categoria = df_emails_reservas.loc[df_emails_reservas["Tipo"] == categoria]
        df_categoria = (
            df_categoria.groupby("COMERCIAL")
            .agg(CONTAS=("CONTA", "count"), VALOR_RESERVADO=("VALOR DESEJADO", "sum"))
            .sort_values(by="VALOR_RESERVADO", ascending=False)
            .reset_index()
        )
        df_categoria = pd.concat(
            [
                df_categoria,
                pd.DataFrame(
                    {
                        "COMERCIAL": ["TOTAL GERAL"],
                        "CONTAS": [df_categoria["CONTAS"].sum()],
                        "VALOR_RESERVADO": [df_categoria["VALOR_RESERVADO"].sum()],
                    }
                ),
            ]
        )
        df_categoria["VALOR_RESERVADO"] = df_categoria["VALOR_RESERVADO"].apply(
            lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
        )
        df_categoria = df_categoria.rename(
            columns={"VALOR_RESERVADO": "VALOR RESERVADO"}
        )

    df_advisors = (
        df_emails_reservas.groupby("ADVISOR")
        .agg(CONTAS=("CONTA", "count"), VALOR_RESERVADO=("VALOR DESEJADO", "sum"))
        .sort_values(by="VALOR_RESERVADO", ascending=False)
        .reset_index()
    )
    df_advisors = pd.concat(
        [
            df_advisors,
            pd.DataFrame(
                {
                    "ADVISOR": ["TOTAL GERAL"],
                    "CONTAS": [df_advisors["CONTAS"].sum()],
                    "VALOR_RESERVADO": [df_advisors["VALOR_RESERVADO"].sum()],
                }
            ),
        ]
    )
    df_advisors["VALOR_RESERVADO"] = df_advisors["VALOR_RESERVADO"].apply(
        lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
    )
    df_advisors = df_advisors.rename(columns={"VALOR_RESERVADO": "VALOR RESERVADO"})

    df_traders = (
        df_emails_reservas.groupby("TRADER")
        .agg(CONTAS=("CONTA", "count"), VALOR_RESERVADO=("VALOR DESEJADO", "sum"))
        .sort_values(by="VALOR_RESERVADO", ascending=False)
        .reset_index()
    )
    df_traders = pd.concat(
        [
            df_traders,
            pd.DataFrame(
                {
                    "TRADER": ["TOTAL GERAL"],
                    "CONTAS": [df_traders["CONTAS"].sum()],
                    "VALOR_RESERVADO": [df_traders["VALOR_RESERVADO"].sum()],
                }
            ),
        ]
    )
    df_traders["VALOR_RESERVADO"] = df_traders["VALOR_RESERVADO"].apply(
        lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
    )
    df_traders = df_traders.rename(columns={"VALOR_RESERVADO": "VALOR RESERVADO"})

    assunto_email = (
        f"Ranking das reservas - {context_script['datetime_hoje_brasil'].hour}h"
    )
    lista_destinatarios = [
        "cassio@investimentos",
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
        "Este é um email automatizado do backoffice com informações do nosso ranking de reservas."
    )
    mail_microsoft.paragraph(f"Temos <strong>{total_reservado}</strong> reservados.")
    mail_microsoft.title("<strong>Rankings atualizados</strong>")

    categorias_comercias = (
        df_emails_reservas["Tipo"]
        .sort_values(ascending=True)
        .drop_duplicates()
        .to_list()
    )
    for categoria in categorias_comercias:
        df_categoria = df_emails_reservas.loc[df_emails_reservas["Tipo"] == categoria]
        df_categoria = (
            df_categoria.groupby("COMERCIAL")
            .agg(CONTAS=("CONTA", "count"), VALOR_RESERVADO=("VALOR DESEJADO", "sum"))
            .sort_values(by="VALOR_RESERVADO", ascending=False)
            .reset_index()
        )
        df_categoria = pd.concat(
            [
                df_categoria,
                pd.DataFrame(
                    {
                        "COMERCIAL": ["TOTAL GERAL"],
                        "CONTAS": [df_categoria["CONTAS"].sum()],
                        "VALOR_RESERVADO": [df_categoria["VALOR_RESERVADO"].sum()],
                    }
                ),
            ]
        )
        df_categoria["VALOR_RESERVADO"] = df_categoria["VALOR_RESERVADO"].apply(
            lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
        )
        df_categoria = df_categoria.rename(
            columns={"VALOR_RESERVADO": "VALOR RESERVADO"}
        )
        mail_microsoft.paragraph(f"<strong>{categoria}:</strong>")
        mail_microsoft.table(df_categoria)

    mail_microsoft.paragraph("<strong>Advisors</strong>")
    mail_microsoft.table(df_advisors)
    mail_microsoft.paragraph("<strong>Traders</strong>")
    mail_microsoft.table(df_traders)

    mail_microsoft.paragraph("Quaisquer dúvidas basta responder a este mesmo e-mail!")
    mail_microsoft.send_mail()
