import pandas as pd
from utils.OneGraph.mail import Mail
from utils.usual_data_transformers import formata_numero_para_texto_monetario
from deltalake import DeltaTable
from dateutil.relativedelta import relativedelta
from datetime import datetime
import io
from utils.DagMonitoramentoAplicacoesRf.context_script import gera_context_script


def carrega_pl_de_posicao_tratada(dag_data_interval_end: datetime) -> pd.DataFrame:
    """Carrega base de PL dia a dia de cada cliente da ONE, a partir da base de posição.

    Args:
        dag_data_interval_end (datetime): Data de execução do DAG.
    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de PL de cada cliente da ONE.
    """

    print("Carregando PL de cada cliente da ONE...")

    context_script = gera_context_script()

    dt_pl = DeltaTable(
        f"s3://{context_script['bucket_lake_silver']}/btg/api/posicao_tratada"
    )
    datetime_mes_passado_dag_run = dag_data_interval_end - relativedelta(months=1)
    df_pl = dt_pl.to_pandas(
        filters=[
            [
                ("ano_particao", "=", str(dag_data_interval_end.year)),
                ("mes_particao", "=", str(dag_data_interval_end.month)),
            ],
            [
                ("ano_particao", "=", str(datetime_mes_passado_dag_run.year)),
                ("mes_particao", "=", str(datetime_mes_passado_dag_run.month)),
            ],
        ]
    )

    df_pl["account"] = df_pl["account"].astype(str).str.lstrip("0")
    df_pl["date"] = pd.to_datetime(df_pl["date"])

    df_pl = (
        df_pl.loc[
            df_pl["date"]
            == dag_data_interval_end.replace(hour=0, minute=0, second=0, microsecond=0),
            ["date", "account", "gross_value", "timestamp_dagrun"],
        ]
        .rename(columns={"date": "Data", "account": "Conta", "gross_value": "PL"})
        .groupby(["Data", "Conta", "timestamp_dagrun"])
        .sum("PL")
        .reset_index()
    )

    datetime_pl_mais_recente = df_pl["timestamp_dagrun"].max()
    df_pl = df_pl.loc[
        df_pl["timestamp_dagrun"] == datetime_pl_mais_recente,
        ["Data", "Conta", "PL"],
    ]

    print(
        f"A base de PL foi gerada partir de posicao_tratada gerada em {datetime_pl_mais_recente}!"
    )

    return df_pl


def carrega_movimentacao_rf(dag_data_interval_end: datetime) -> pd.DataFrame:
    """Carrega base de movimentação de renda fixa.
    Args:
        dag_data_interval_end (datetime): Data de execução do DAG.

    Returns:
        df_movimentacao_rf (pd.DataFrame): DataFrame com as operações usadas no dashboard de de renda fixa.
    """

    context_script = gera_context_script()

    print("Carregando base de movimentações de renda fixa...")

    df_movimentacao_rf = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/api/btg/s3/movimentacao_historico/"
    )

    df_movimentacao_rf = df_movimentacao_rf.rename(
        columns={
            "account": "Conta",
            "market": "Market",
            "sub_market": "Sub Market",
            "product": "Produto",
            "movimentation": "Movimentação",
            "issuer": "Emissor",
            "stock": "Ativo",
            "purchase_date": "Data",
            "net_value": "Valor Líquido",
        }
    )

    df_movimentacao_rf["Conta"] = df_movimentacao_rf["Conta"].astype(str)
    df_movimentacao_rf["Data"] = pd.to_datetime(df_movimentacao_rf["Data"])
    df_movimentacao_rf["Produto Clean"] = (
        df_movimentacao_rf["Produto"].str.split(" ").str[0]
    )

    df_movimentacao_rf = df_movimentacao_rf.loc[
        df_movimentacao_rf["Data"] == dag_data_interval_end
    ]

    print(
        f"Base de movimentações de renda fixa carregada com sucesso para a data {dag_data_interval_end}"
    )

    return df_movimentacao_rf


def carrega_socios() -> pd.DataFrame:
    """Carrega base de sócios.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de socios.
    """

    context_script = gera_context_script()

    print("Carregando base de sócios...")

    df_socios = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/socios/",
        columns=["Nome", "Nome_bd", "E-mail"],
    )

    df_socios["E-mail"] = df_socios["E-mail"].str.replace(
        "oneinv.com", "investimentos.one"
    )

    return df_socios


def carrega_clientes_assessores() -> pd.DataFrame:
    """Carrega base ClientesAssessores.

    Returns:
        df_clientes_assessores (pd.DataFrame): DataFrame com os assessores mais recentes de cada conta da ONE.
    """

    context_script = gera_context_script()

    print("Carregando base clientes assessores...")

    df_clientes_assessores = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/api/hubspot/clientesassessores"
    )

    df_clientes_assessores["REGISTRO"] = pd.to_datetime(
        df_clientes_assessores["REGISTRO"]
    )
    data_registro_mais_recente = df_clientes_assessores["REGISTRO"].max()

    df_clientes_assessores = df_clientes_assessores.loc[
        df_clientes_assessores["REGISTRO"] == data_registro_mais_recente,
        ["Conta", "Nome", "Comercial", "Operacional RF", "Operacional RV"],
    ]
    df_clientes_assessores["Nome"] = (
        df_clientes_assessores["Nome"].str.split(" ").str[0]
    )

    print(
        f"Base clientes assessores carregadas com 'REGISTRO' = {data_registro_mais_recente}!"
    )

    return df_clientes_assessores


def estabelece_movimentacoes_rf_monitoradas(
    dag_data_interval_end: datetime,
    df_movimentacao_rf: pd.DataFrame,
    df_pl: pd.DataFrame,
    df_clientes_assessores: pd.DataFrame,
) -> pd.DataFrame:
    """Estabele o % PL que cada aplicação representa.

    Args:
        dag_data_interval_end (datetime): Data de execução do DAG.
        df_movimentacao_rf (pd.DataFrame): Operações dos clientes usadas no dashboard de de renda fixa.
        df_pl (pd.DataFrame): PL dos clientes.
        df_clientes_assessores (pd.DataFrame): DataFrame com os assessores mais recentes de cada conta da ONE.

    Returns:
        df_aplicacoes_rf_monitoradas (pd.DataFrame): DataFrame com as operações usadas no dashboard de de renda fixa acrescidas do % PL que cada aplicação representa.
    """

    if (
        dag_data_interval_end.replace(hour=0, minute=0, second=0, microsecond=0)
        not in df_pl["Data"].drop_duplicates().to_list()
    ):
        raise Exception(
            f"Não temos PL para a data {dag_data_interval_end}, verifique o que ocorreu com a base de PL na Gold!"
        )
    else:
        df_pl = df_pl.loc[df_pl["Data"] == dag_data_interval_end]

        df_movimentacoes_rf_monitoradas = pd.merge(
            df_movimentacao_rf, df_pl, on=["Conta", "Data"], how="left"
        )
        df_movimentacoes_rf_monitoradas = pd.merge(
            df_movimentacoes_rf_monitoradas,
            df_clientes_assessores,
            on="Conta",
            how="left",
        )
        df_movimentacoes_rf_monitoradas["Aplicacação (% PL)"] = (
            df_movimentacoes_rf_monitoradas["Valor Líquido"]
            / df_movimentacoes_rf_monitoradas["PL"]
        )

        filtro_fundos = (df_movimentacoes_rf_monitoradas["Market"] == "FUNDOS") & (
            ~df_movimentacoes_rf_monitoradas["Ativo"].str.contains(
                "cdb plus", case=False
            )
        )
        filtro_rf = (df_movimentacoes_rf_monitoradas["Market"] == "RENDA FIXA") & (
            df_movimentacoes_rf_monitoradas["Produto Clean"].isin(
                ["CRI", "CRA", "DEBÊNTURE", "LCI", "LCA", "LF", "LFSN", "LFSC", "COE"]
            )
        )

        filtro_tipo_movimentacao = (
            ~df_movimentacoes_rf_monitoradas["Movimentação"].str.contains(
                "vencimento", case=False
            )
        ) & (df_movimentacoes_rf_monitoradas["Movimentação"] != "JUROS")

        filtro_final = ((filtro_fundos) | (filtro_rf)) & (filtro_tipo_movimentacao)
        df_movimentacoes_rf_monitoradas = df_movimentacoes_rf_monitoradas.loc[
            filtro_final
        ]

        return df_movimentacoes_rf_monitoradas


def gera_base_movimentacoes_relevantes_rf_drive() -> None:
    """Salva base usada no envio de e-mails de informações relevantes em uma pasta do Drive de RF."""

    context_script = gera_context_script()

    df_movimentacoes_rf_monitoradas = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/btg/s3/monitoramento_aplicacoes_rf"
    )

    df_movimentacoes_rf_monitoradas_drive = df_movimentacoes_rf_monitoradas.loc[
        :,
        [
            "Conta",
            "Nome",
            "Produto",
            "Movimentação",
            "Emissor",
            "Ativo",
            "Data",
            "Valor Líquido",
            "PL",
            "Aplicacação (% PL)",
            "Comercial",
            "Operacional RF",
            "Operacional RV",
        ],
    ]

    drive_microsoft = context_script["drive_microsoft"]

    with io.BytesIO() as buffer:
        df_movimentacoes_rf_monitoradas_drive.to_csv(
            buffer, encoding="latin1", sep=";", decimal=",", index=False
        )
        bytes_df_movimentacoes_rf_monitoradas_drive = buffer.getvalue()
        drive_microsoft.upload_file(
            context_script[f"caminho_arquivo_monitoramento_rf"],
            bytes_df_movimentacoes_rf_monitoradas_drive,
        )


def gera_base_movimentacoes_relevantes_rf_lake(dag_data_interval_end: datetime) -> None:
    """Salva base usada no envio de e-mails de informações relevantes na Gold.

    Args:
        dag_data_interval_end (datetime): Data de execução do DAG.
    """

    context_script = gera_context_script()

    df_movimentacao_rf = carrega_movimentacao_rf(dag_data_interval_end)
    df_pl = carrega_pl_de_posicao_tratada(dag_data_interval_end)
    df_clientes_assessores = carrega_clientes_assessores()

    df_aplicacoes_rf_monitoradas_dag_run = estabelece_movimentacoes_rf_monitoradas(
        dag_data_interval_end, df_movimentacao_rf, df_pl, df_clientes_assessores
    )

    df_aplicacoes_rf_monitoradas_dag_run["Parâmetro Relevância"] = context_script[
        "parametro_relevancia_rf"
    ]
    df_aplicacoes_rf_monitoradas_dag_run["Data de atualização"] = context_script[
        "datetime_hoje_brasil"
    ]

    df_aplicacoes_rf_monitoradas_historico = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/btg/s3/monitoramento_aplicacoes_rf"
    )

    df_aplicacoes_rf_monitoradas_historico = df_aplicacoes_rf_monitoradas_historico.loc[
        df_aplicacoes_rf_monitoradas_historico["Data"] != dag_data_interval_end
    ]

    colunas_necessarias = [
        "Conta",
        "Market",
        "Sub Market",
        "Produto",
        "Movimentação",
        "Emissor",
        "Ativo",
        "Data",
        "Valor Líquido",
        "Produto Clean",
        "PL",
        "Nome",
        "Comercial",
        "Operacional RF",
        "Operacional RV",
        "Aplicacação (% PL)",
        "Parâmetro Relevância",
        "Data de atualização",
    ]

    df_aplicacoes_rf_monitoradas_dag_run = df_aplicacoes_rf_monitoradas_dag_run.loc[
        :, colunas_necessarias
    ]
    df_aplicacoes_rf_monitoradas_historico = df_aplicacoes_rf_monitoradas_historico.loc[
        :, colunas_necessarias
    ]

    df_aplicacoes_rf_monitoradas_atualizado = pd.concat(
        [df_aplicacoes_rf_monitoradas_historico, df_aplicacoes_rf_monitoradas_dag_run]
    )
    df_aplicacoes_rf_monitoradas_atualizado = (
        df_aplicacoes_rf_monitoradas_atualizado.drop_duplicates()
    )
    df_aplicacoes_rf_monitoradas_atualizado.sort_values(
        by="Data de atualização"
    ).to_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/btg/s3/monitoramento_aplicacoes_rf/monitoramento_aplicacoes_rf.parquet"
    )


def envia_emails_aplicacoes_relevantes_advisors(
    dag_data_interval_end: datetime,
) -> None:
    """Envia os e-mails de aplicações relevantes para os advisors.

    Args:
        dag_data_interval_end (datetime): Data de execução do DAG.
    """

    context_script = gera_context_script()

    df_movimentacoes_rf_monitoradas = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/btg/s3/monitoramento_aplicacoes_rf"
    )
    df_movimentacoes_rf_monitoradas = df_movimentacoes_rf_monitoradas.loc[
        df_movimentacoes_rf_monitoradas["Data"] == dag_data_interval_end
    ]

    df_socios = carrega_socios()

    lista_advisors = (
        df_movimentacoes_rf_monitoradas["Operacional RF"].drop_duplicates().to_list()
    )

    (
        lista_advisors.remove("One Investimentos")
        if "One Investimentos" in lista_advisors
        else lista_advisors
    )

    percent_aplicacao_patrimonio = context_script["parametro_relevancia_rf"]
    colunas_visualizadas = [
        "Conta",
        "Nome",
        "Produto",
        "Movimentação",
        "Emissor",
        "Ativo",
        "Data",
        "Valor Líquido",
        "PL",
        "Aplicacação (% PL)",
        "Comercial",
        "Operacional RF",
        "Operacional RV",
    ]

    filtro_relevancia_rf = (
        df_movimentacoes_rf_monitoradas["Aplicacação (% PL)"]
        >= percent_aplicacao_patrimonio
    )

    filtro_resgates = df_movimentacoes_rf_monitoradas["Aplicacação (% PL)"] < 0

    df_aplicacoes_rf_monitoradas = df_movimentacoes_rf_monitoradas.loc[
        filtro_relevancia_rf, colunas_visualizadas
    ].sort_values(by="Aplicacação (% PL)", ascending=False)
    df_resgates_rf_monitorados = df_movimentacoes_rf_monitoradas.loc[
        filtro_resgates, colunas_visualizadas
    ].sort_values(by="Aplicacação (% PL)", ascending=True)

    assunto_email = f"Aplicações RF relevantes (Team Leader) - {dag_data_interval_end.strftime('%d/%m/%Y')}"
    lista_destinatarios = [
        "samuel.machado@investimentos.one",
        "diogo.cabral@investimentos.one",
        "luis.martins@investimentos.one",
        "gustavo.sousa@investimentos.one",
    ]
    remetente = context_script["id_microsoft_tecnologia"]

    # # ATENÇÃO: DESCOMENTE PARA RODAR LOCAL
    # assunto_email = f"[TESTE] Aplicações RF relevantes (Team Leader) - {dag_data_interval_end.strftime('%d/%m/%Y')}"
    # lista_destinatarios = [
    #     # "samuel.machado@investimentos.one",
    #     "gustavo.sousa@investimentos.one",
    # ]

    mail_microsoft = Mail(
        context_script["dict_credencias_microsft"]["client_id"],
        context_script["dict_credencias_microsft"]["client_secret"],
        context_script["dict_credencias_microsft"]["tenant_id"],
        assunto_email,
        lista_destinatarios,
        remetente,
    )

    team_leader = "Samuel Machado"
    primeiro_nome_team_leader = team_leader.split(" ")[0]

    df_emails_aplicacoes_monitoradas_team_leader_email = (
        df_aplicacoes_rf_monitoradas.copy()
    )

    df_emails_aplicacoes_monitoradas_team_leader_email = (
        df_emails_aplicacoes_monitoradas_team_leader_email.loc[
            df_emails_aplicacoes_monitoradas_team_leader_email["Operacional RF"]
            == "One Investimentos"
        ]
    )

    df_emails_aplicacoes_monitoradas_team_leader_email_anexo = (
        df_emails_aplicacoes_monitoradas_team_leader_email.copy()
    )

    df_emails_aplicacoes_monitoradas_team_leader_email["Aplicacação (% PL)"] = (
        df_emails_aplicacoes_monitoradas_team_leader_email["Aplicacação (% PL)"].apply(
            lambda x: f"{formata_numero_para_texto_monetario(x * 100)}%"
        )
    )
    df_emails_aplicacoes_monitoradas_team_leader_email["PL"] = (
        df_emails_aplicacoes_monitoradas_team_leader_email["PL"].apply(
            lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
        )
    )
    df_emails_aplicacoes_monitoradas_team_leader_email["Valor Líquido"] = (
        df_emails_aplicacoes_monitoradas_team_leader_email["Valor Líquido"].apply(
            lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
        )
    )
    df_emails_aplicacoes_monitoradas_team_leader_email = (
        df_emails_aplicacoes_monitoradas_team_leader_email.fillna("-")
    )

    df_emails_resgates_monitorados_team_leader_email = df_resgates_rf_monitorados.copy()
    df_emails_resgates_monitorados_team_leader_email = (
        df_emails_resgates_monitorados_team_leader_email.loc[
            df_emails_resgates_monitorados_team_leader_email["Operacional RF"]
            == "One Investimentos"
        ]
    )
    df_emails_resgates_monitorados_team_leader_email_anexo = (
        df_emails_resgates_monitorados_team_leader_email.copy()
    )
    df_emails_resgates_monitorados_team_leader_email["Aplicacação (% PL)"] = (
        df_emails_resgates_monitorados_team_leader_email["Aplicacação (% PL)"].apply(
            lambda x: f"{formata_numero_para_texto_monetario(x * 100)}%"
        )
    )
    df_emails_resgates_monitorados_team_leader_email["PL"] = (
        df_emails_resgates_monitorados_team_leader_email["PL"].apply(
            lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
        )
    )
    df_emails_resgates_monitorados_team_leader_email["Valor Líquido"] = (
        df_emails_resgates_monitorados_team_leader_email["Valor Líquido"].apply(
            lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
        )
    )
    df_emails_resgates_monitorados_team_leader_email = (
        df_emails_resgates_monitorados_team_leader_email.fillna("-")
    )

    mail_microsoft.paragraph(
        f"Prezado {primeiro_nome_team_leader}, {mail_microsoft.Greetings.time_of_day()}."
    )
    mail_microsoft.paragraph(
        f"Este é um e-mail automatizado de tech com as operações de RF que representam mais de {percent_aplicacao_patrimonio * 100}% do patrimônio de cada cliente, bem como todas as operações de fundos e previdência."
    )
    mail_microsoft.paragraph("Ele é o consolidado de toda a mesa de advisors.")
    mail_microsoft.title("Aplicações relevantes")

    if not df_emails_aplicacoes_monitoradas_team_leader_email.empty:
        mail_microsoft.table(
            df_emails_aplicacoes_monitoradas_team_leader_email,
            pgnr_textual_number=["Aplicacação (% PL)", "PL", "Valor Líquido"],
        )
        mail_microsoft.table_as_xlsx(
            f"aplicacoes_relevantes_{dag_data_interval_end.strftime('%Y%m%d')}.xlsx",
            df_emails_aplicacoes_monitoradas_team_leader_email_anexo,
        )
    else:
        mail_microsoft.paragraph(
            f"Não houve aplicações que superaram {percent_aplicacao_patrimonio * 100}% do PL."
        )

    mail_microsoft.title("Resgates")
    if not df_emails_resgates_monitorados_team_leader_email.empty:
        mail_microsoft.table(
            df_emails_resgates_monitorados_team_leader_email,
            pgnr_textual_number=["Aplicacação (% PL)", "PL", "Valor Líquido"],
        )
        mail_microsoft.table_as_xlsx(
            f"resgates_{dag_data_interval_end.strftime('%Y%m%d')}.xlsx",
            df_emails_resgates_monitorados_team_leader_email_anexo,
        )
    else:
        mail_microsoft.paragraph(f"Não houve resgates.")

    mail_microsoft.line_break()
    mail_microsoft.paragraph("Quaisquer dúvidas responda a este mesmo e-mail!")
    mail_microsoft.send_mail()

    print(f"E-mail enviado com sucesso para o Team Leader dos advisors, {team_leader}!")

    for advisor in lista_advisors:
        primeiro_nome_advisor = advisor.split(" ")[0]
        email_advisor = df_socios.loc[df_socios["Nome_bd"] == advisor][
            "E-mail"
        ].to_list()[0]

        df_emails_aplicações_monitoradas_advisor_anexo = (
            df_aplicacoes_rf_monitoradas.loc[
                df_aplicacoes_rf_monitoradas["Operacional RF"] == advisor
            ]
        )
        df_emails_resgates_monitorados_advisor_anexo = df_resgates_rf_monitorados.loc[
            df_resgates_rf_monitorados["Operacional RF"] == advisor
        ]

        df_emails_aplicacoes_monitoradas_advisor_email = (
            df_emails_aplicações_monitoradas_advisor_anexo.copy()
        )
        df_emails_aplicacoes_monitoradas_advisor_email["Aplicacação (% PL)"] = (
            df_emails_aplicações_monitoradas_advisor_anexo["Aplicacação (% PL)"].apply(
                lambda x: f"{formata_numero_para_texto_monetario(x * 100)}%"
            )
        )
        df_emails_aplicacoes_monitoradas_advisor_email["PL"] = (
            df_emails_aplicacoes_monitoradas_advisor_email["PL"].apply(
                lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
            )
        )
        df_emails_aplicacoes_monitoradas_advisor_email["Valor Líquido"] = (
            df_emails_aplicacoes_monitoradas_advisor_email["Valor Líquido"].apply(
                lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
            )
        )
        df_emails_aplicacoes_monitoradas_advisor_email = (
            df_emails_aplicacoes_monitoradas_advisor_email.fillna("-")
        )

        df_emails_resgates_monitorados_advisor_email = (
            df_emails_resgates_monitorados_advisor_anexo.copy()
        )
        df_emails_resgates_monitorados_advisor_email["Aplicacação (% PL)"] = (
            df_emails_resgates_monitorados_advisor_anexo["Aplicacação (% PL)"].apply(
                lambda x: f"{formata_numero_para_texto_monetario(x * 100)}%"
            )
        )
        df_emails_resgates_monitorados_advisor_email["PL"] = (
            df_emails_resgates_monitorados_advisor_email["PL"].apply(
                lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
            )
        )
        df_emails_resgates_monitorados_advisor_email["Valor Líquido"] = (
            df_emails_resgates_monitorados_advisor_email["Valor Líquido"].apply(
                lambda x: f"R$ {formata_numero_para_texto_monetario(x)}"
            )
        )
        df_emails_resgates_monitorados_advisor_email = (
            df_emails_resgates_monitorados_advisor_email.fillna("-")
        )

        assunto_email = (
            f"Aplicações RF relevantes - {dag_data_interval_end.strftime('%d/%m/%Y')}"
        )
        lista_destinatarios = [
            email_advisor,
            "samuel.machado@investimentos.one",
            "diogo.cabral@investimentos.one",
            "luis.martins@investimentos.one",
            "gustavo.sousa@investimentos.one",
        ]
        remetente = context_script["id_microsoft_tecnologia"]

        # # ATENÇÃO: DESCOMENTE PARA RODAR LOCAL
        # assunto_email = f"[TESTE] Aplicações RF relevantes - {dag_data_interval_end.strftime('%d/%m/%Y')}"
        # lista_destinatarios = ["gustavo.sousa@investimentos.one"]

        mail_microsoft = Mail(
            context_script["dict_credencias_microsft"]["client_id"],
            context_script["dict_credencias_microsft"]["client_secret"],
            context_script["dict_credencias_microsft"]["tenant_id"],
            assunto_email,
            lista_destinatarios,
            remetente,
        )

        mail_microsoft.paragraph(
            f"Prezado {primeiro_nome_advisor}, {mail_microsoft.Greetings.time_of_day()}."
        )
        mail_microsoft.paragraph(
            f"Este é um e-mail automatizado de tech com as operações de RF que representam mais de {percent_aplicacao_patrimonio * 100}% do patrimônio de cada cliente, bem como todas as operações de fundos e previdência."
        )

        mail_microsoft.title("Aplicações relevantes")
        if not df_emails_aplicacoes_monitoradas_advisor_email.empty:
            mail_microsoft.table(
                df_emails_aplicacoes_monitoradas_advisor_email,
                pgnr_textual_number=["Aplicacação (% PL)", "PL", "Valor Líquido"],
            )
            mail_microsoft.table_as_xlsx(
                f"aplicacoes_relevantes_{dag_data_interval_end.strftime('%Y%m%d')}.xlsx",
                df_emails_aplicações_monitoradas_advisor_anexo,
            )
        else:
            mail_microsoft.paragraph(
                f"Não houve aplicações que superaram {percent_aplicacao_patrimonio * 100}% do PL."
            )
        mail_microsoft.title("Resgates")

        if not df_emails_resgates_monitorados_advisor_email.empty:
            mail_microsoft.table(
                df_emails_resgates_monitorados_advisor_email,
                pgnr_textual_number=["Aplicacação (% PL)", "PL", "Valor Líquido"],
            )
            mail_microsoft.table_as_xlsx(
                f"resgates_{dag_data_interval_end.strftime('%Y%m%d')}.xlsx",
                df_emails_resgates_monitorados_advisor_anexo,
            )
        else:
            mail_microsoft.paragraph(f"Não houve resgates.")

        mail_microsoft.line_break()
        mail_microsoft.paragraph("Quaisquer dúvidas responda a este mesmo e-mail!")
        mail_microsoft.send_mail()

        print(f"E-mail enviado com sucesso para o advisor {advisor}!")
