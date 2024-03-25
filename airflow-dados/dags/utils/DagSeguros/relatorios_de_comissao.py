import json
import pandas as pd
import io
import pandas_market_calendars as mcal
from datetime import datetime
from dateutil.relativedelta import relativedelta

from utils.OneGraph.drive import OneGraphDrive
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name


def relatorios_de_comissao(drive_id):
    # De-Para das colunas de acordo com o relatório de cada seguradora
    cols_names = {
        # Azos
        "Nº da Apólice": "numero_da_apolice",
        "Nome do Segurado": "nome_do_cliente",
        "Prêmio": "premio_pago",
        "Comissão Bruta": "valor_bruto_comissao",
        "Mês de Competência": "mes_de_competencia",
        # MAG
        "Inscrição": "numero_da_apolice",  # Utilizar a Inscrição deseconsiderando os últimos 4 dígitos
        "Nome Segurado": "nome_do_cliente",
        "Valor Base (R$)": "premio_pago",
        "Comissão (R$)": "valor_bruto_comissao",
        "Comissão (%)": "percentual_de_comissao",
        "Data": "mes_de_competencia",
        # MetLife
        "Apólice": "numero_da_apolice",
        "Segurado": "nome_do_cliente",
        "Premio Líquido": "premio_pago",
        "Valor Comissão": "valor_bruto_comissao",
        "Dt Pagto": "mes_de_competencia",
        # Omint
        "Apólice": "numero_da_apolice",
        "Segurado / Estipulante": "nome_do_cliente",
        "Premio Líquido": "premio_pago",
        "Vl. a Receber": "valor_bruto_comissao",
        "% de Comissão": "percentual_de_comissao",
        "Dt. Comissão": "mes_de_competencia",
        # Prudential
        "Apólice": "numero_da_apolice",
        "Segurado": "nome_do_cliente",
        "Prêmio Líquido": "premio_pago",
        "Comissão N2": "valor_bruto_comissao",  # Ou Comissão Renovação N2 ("fillna")
        "% Comissão N2": "percentual_de_comissao",
        "Data Geração Comissão Direta": "mes_de_competencia",
    }

    # Credenciais da Graph API da Microsoft
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    # Definir os buckets
    bucket_lake_landing = get_bucket_name("lake-landing")
    bucket_lake_bronze = get_bucket_name("lake-bronze")
    bucket_lake_silver = get_bucket_name("lake-silver")

    now = datetime.now()
    last_month = (now - relativedelta(months=1)).replace(day=1)
    last_month = last_month.strftime("%Y-%m-%d")

    # Identificar o último dia útil (D-1)
    bmf_raw = mcal.get_calendar("BMF")
    bmf_raw = bmf_raw.schedule(
        start_date=last_month,
        end_date=now.date(),
    )

    last_business_day = bmf_raw[["market_open"]].iloc[-2][0]
    last_business_day = last_business_day.date()

    # Caminho da pasta que contém todos os relatórios de comissão
    path = ["One Analytics", "Seguros", "Relatórios de Comissão"]

    # Identificar o ID dessa pasta
    id_seguros = drive._get_ids_from_path(path)

    # Listar todos os IDs das subpastas
    ids = drive.list_children_endpoint(id_seguros[-1]["id"]).content
    ids = json.loads(ids.decode("utf-8"))

    last_year = (now - relativedelta(years=1)).replace(day=1)
    last_year = last_year.strftime("%Y-%m-%d")

    # Ler todos os relatórios
    for id in ids["value"]:
        # Identifcar se a pasta foi criada nos últimos 12 meses
        # para evitar de ler muitos arquivos
        if (
            pd.to_datetime(id["createdDateTime"]).date()
            >= pd.to_datetime(last_year).date()
        ):
            # Extrair o ID da pasta a partir do dicionário
            id = id["id"]

            # Listar os dicionários das subpastas da pasta de cada mês
            ids_month = drive.list_children_endpoint(id).content

            # Transformar string em json
            ids_month = json.loads(ids_month.decode("utf-8"))

            if "value" in ids_month.keys():
                for file in ids_month["value"]:
                    # Extrair o ID do arquivo a partir do dicionário
                    id_month = file["id"]

                    # Extrair o nome do arquivo a partir do dicionário
                    name = file["name"]
                    name = name.split("/")[-1].split(".xlsx")[0]

                    # Extrair ano e mês a partir do nome da pasta
                    folder_name = file["parentReference"]["name"]

                    if folder_name != "Histórico":
                        ano = folder_name.split("-")[0]
                        mes = folder_name.split("-")[-1]
                        ano_mes = f"{ano}{mes}"

                    else:
                        ano = "historico"
                        mes = "historico"
                        ano_mes = "historico"

                    # Identificar a data da última modificação dos arquivos
                    file_metadata = drive.get_drive_item_endpoint(id_month).content
                    file_metadata = json.loads(file_metadata.decode("utf-8"))

                    last_modified = pd.to_datetime(
                        file_metadata["lastModifiedDateTime"]
                    ).date()

                    # Caso o arquivo tenha sido modificado entre D-1 e hoje,
                    # salvar na landing e bronze
                    if last_modified >= last_business_day:
                        # Extrair o conteúdo do arquivo
                        df = drive.download_file_endpoint(id_month).content

                        # Transformar em dataframe
                        df = pd.read_excel(io.BytesIO(df), thousands=".", decimal=",")

                        df.to_excel(
                            f"s3://{bucket_lake_landing}/one/seguros/relatorios_de_comissao/{name}/ano={ano}/mes={mes}/{name}_{ano_mes}.xlsx"
                        )

                        # Padronizar os nomes das colunas
                        df = df.rename(cols_names, axis=1)

                        # Preencher o nome da seguradora
                        df["Seguradora"] = name

                        # Manter apenas um registro por apólice e mês de competência,
                        # visto que algumas seguradoras possuem uma linha por produto,
                        # mesmo todos estando na mesma apólice
                        df = df.groupby(
                            [
                                "Número da Apólice",
                                "Nome do Cliente",
                                "Mês de competência",
                                "Seguradora",
                            ],
                            as_index=False,
                        ).sum()

                        df.to_parquet(
                            f"s3://{bucket_lake_bronze}/one/seguros/relatorios_de_comissao/{name}/ano={ano}/mes={mes}/{name}_{ano_mes}.parquet"
                        )

    # Consolidar todos os relatórios de comissão
    df_comissao = pd.read_parquet(
        f"s3://{bucket_lake_bronze}/one/seguros/relatorios_de_comissao/"
    )

    df_comissao = df_comissao.reset_index(drop=True)

    # Calcular o % de comissão após o Groupby
    df_comissao["% de comissão"] = (
        df_comissao["Valor bruto de comissão"] / df_comissao["Prêmio Pago"]
    )

    # O % de comissão da seguradora Azos é fixo em 25%
    df_comissao.loc[df_comissao["Seguradora"] == "Azos", "% de comissão"] = 0.25

    # A inscrição da seguradora MAG é o número da apólice + o código do produto,
    # por isso é necessário descnosiderar os último 4 digítos (código do produto)
    df_comissao["Número da Apólice"] = df_comissao["Número da Apólice"].astype(str)

    df_comissao.loc[
        df_comissao["Seguradora"] == "MAG", "Número da Apólice"
    ] = df_comissao["Número da Apólice"].str[:-4]

    # Nos casos de renovação da Prudential o valor da comissão aparece em outra coluna,
    # por isso é necessário fazer o tratamento abaixo, similar a um fillna
    if "Comissão Renovação N2" in df_comissao.columns.tolist():
        df_comissao.loc[
            (df_comissao["Seguradora"] == "Prudential")
            & (df_comissao["Valor bruto de comissão"].isna()),
            "Valor bruto de comissão",
        ] = df_comissao["Comissão Renovação N2"]

    # Selecionar colunas que serão utilizadas
    df_comissao = df_comissao[
        [
            "Número da Apólice",
            "Seguradora",
            "Nome do Cliente",
            "Prêmio Pago",
            "Valor bruto de comissão",
            "% de comissão",
            "Mês de competência",
        ]
    ]

    # Remover linhas vazias
    # Usei essa coluna, pois é a única que aparece em todos os relatórios
    df_comissao = df_comissao.dropna(subset=["Mês de competência"])

    # Extrair o mês da data de competência
    df_comissao["Mês de competência"] = pd.to_datetime(
        df_comissao["Mês de competência"]
    ).dt.to_period("M")

    # Salvar a base consolidada na silver pariticionada por ano e mês
    df_comissao["Ano"] = df_comissao["Mês de competência"].dt.year
    df_comissao["Mes"] = df_comissao["Mês de competência"].dt.month

    df_comissao.to_parquet(
        f"s3://{bucket_lake_silver}/one/seguros/relatorios_de_comissao/",
        partition_cols=["Ano", "Mes"],
    )

    # Salvar a base consolidada no OneDrive
    buffer = io.BytesIO()
    df_comissao.to_excel(buffer, index=False)
    object_content = buffer.getvalue()

    path_upload = path + ["consolidado_comissoes.xlsx"]
    drive.upload_file(path_upload, object_content)
