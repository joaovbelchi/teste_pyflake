import pandas as pd
import io
from utils.DagSaldoEmConta.functions import (
    prepara_nome_para_join,
)
from utils.Commons.DagsReservasOfertasPublicasPushesOfertasPublicas.context_script import (
    gera_context_script,
)


def excecao_bases_possiveis(base: str) -> None:
    """Exceção para travar caso a base escolhida não seja possível.

    Args:
        base (str): Opção de 'pushes' ou 'reservas'.
    """
    bases_possiveis = ["reservas", "pushes"]
    if base not in bases_possiveis:
        raise Exception(
            f"A base '{base}' não pode ser usada. As possíveis de ser usadas neste processo são {str(bases_possiveis).strip('[').strip(']')}!"
        )


def carrega_clientes_assessores() -> pd.DataFrame:
    """Carrega base de contas de clientes por assessor.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de clientes por assessor.
    """

    context_script = gera_context_script()

    print("Carregando base de contas de clientes por assessor...")

    df_clientes_assessores = pd.read_parquet(
        f"s3://{context_script['bucket_lake_silver']}/api/hubspot/clientesassessores"
    )

    data_mais_recente_clientes_assessores = df_clientes_assessores["REGISTRO"].max()
    df_clientes_assessores = df_clientes_assessores.loc[
        df_clientes_assessores["REGISTRO"] == data_mais_recente_clientes_assessores,
        ["Conta", "Comercial", "Operacional RF", "Operacional RV"],
    ]

    df_clientes_assessores["Conta"] = df_clientes_assessores["Conta"].astype(str)

    df_clientes_assessores["Comercial (c. esp.)"] = df_clientes_assessores["Comercial"]
    df_clientes_assessores["Operacional RF (c. esp.)"] = df_clientes_assessores[
        "Operacional RF"
    ]
    df_clientes_assessores["Operacional RV (c. esp.)"] = df_clientes_assessores[
        "Operacional RV"
    ]

    df_clientes_assessores["Comercial"] = df_clientes_assessores["Comercial"].apply(
        prepara_nome_para_join
    )
    df_clientes_assessores["Operacional RF"] = df_clientes_assessores[
        "Operacional RF"
    ].apply(prepara_nome_para_join)
    df_clientes_assessores["Operacional RV"] = df_clientes_assessores[
        "Operacional RV"
    ].apply(prepara_nome_para_join)

    print(
        f"A base ClientesAssessores foi carregada e filtrada com informações do DAG que rodou em: {data_mais_recente_clientes_assessores}"
    )

    return df_clientes_assessores


def carrega_socios() -> pd.DataFrame:
    """Carrega base de sócios.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de socios.
    """

    context_script = gera_context_script()

    print("Carregando base de sócios...")

    df_socios = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/socios/"
    )

    df_socios["Nome_bd"] = df_socios["Nome_bd"].apply(prepara_nome_para_join)
    df_socios["E-mail"] = df_socios["E-mail"].str.replace(
        "oneinv.com", "investimentos.one"
    )

    print("Base de sócios carregada com sucesso!")
    return df_socios


def carrega_pl() -> pd.DataFrame:
    """Carrega base de PL de cada cliente da ONE.

    Returns:
        df_socios (pd.DataFrame): Dataframe com a base de PL de cada cliente da ONE.
    """

    context_script = gera_context_script()

    print("Carregando base de PL de cada cliente da ONE...")

    df_pl = pd.read_parquet(f"s3://{context_script['bucket_lake_gold']}/btg/api/PL")

    data_mais_recente_dag_pl = df_pl["Data"].max()

    df_pl = df_pl.loc[
        (df_pl["Data"] == data_mais_recente_dag_pl) & (df_pl["Mercado"] != "TOTAL"),
        ["Conta", "PL"],
    ]
    df_pl["Conta"] = df_pl["Conta"].astype(str)
    df_pl = df_pl.groupby("Conta").sum("PL").reset_index()

    print(
        f"A base de PL foi carregada e filtrada com informações de PL para a data: {data_mais_recente_dag_pl}"
    )
    return df_pl


def carrega_pushes_ou_reservas(base: str) -> pd.DataFrame:
    """Carrega base de pushes ou de reservas consolidada.

    Args:
        base (str): Opção de 'pushes' ou 'reservas'.

    Returns:
        pd.DataFrame: DataFrame com base de pushes ou de reservas.
    """

    context_script = gera_context_script()

    excecao_bases_possiveis(base)

    print(f"Carregando base de {base}...")
    df_pushes_ou_reservas = pd.read_parquet(
        f"s3://{context_script['bucket_lake_gold']}/api/one/Ofertas_Publicas/acompanhamento_{base}.parquet"
    )

    df_pushes_ou_reservas = df_pushes_ou_reservas.rename(columns={"CONTA": "Conta"})
    df_pushes_ou_reservas["Conta"] = df_pushes_ou_reservas["Conta"].astype(str)

    print(f"Base de {base} carregada com sucesso!")
    return df_pushes_ou_reservas


def estabelece_base_emails_pushes_reservas(
    df_pushes_ou_reservas: pd.DataFrame,
    df_clientes_assessores: pd.DataFrame,
    df_socios: pd.DataFrame,
    df_pl: pd.DataFrame,
) -> pd.DataFrame:
    """Estabelece a base usada no envio de e-mails de pushes ou reservas.

    Args:
        df_pushes_ou_reservas (pd.DataFrame): Base consolidade de pushes ou reservas.
        df_clientes_assessores (pd.DataFrame): Base de clientes por assessor.
        df_socios (pd.DataFrame): Base de sócios.
        df_pl (pd.DataFrame): Base de PL dos clientes.

    Returns:
        pd.DataFrame: DataFrame com base pronto para gerar e-mails de reservas ou pushes.
    """

    context_script = gera_context_script()

    df_emails_pushes_reservas = pd.merge(
        df_pushes_ou_reservas, df_clientes_assessores, how="left", on="Conta"
    )

    df_emails_pushes_reservas = pd.merge(
        df_emails_pushes_reservas, df_pl, how="left", on="Conta"
    )

    ### filtro de qualidade, será melhorado com ge
    df_socios = df_socios.loc[
        ~df_socios["E-mail"].isin(["conta_encerrada@investimentos.one"])
    ]

    df_emails_tipos_comerciais = df_socios.loc[
        :,
        ["Nome_bd", "E-mail", "sub_area"],
    ].rename(
        columns={
            "Nome_bd": "Comercial",
            "E-mail": "E-mail Comercial",
            "sub_area": "Tipo",
        }
    )

    df_emails_advisors = df_socios.loc[
        df_socios["Nome_bd"].isin(
            df_emails_pushes_reservas["Operacional RF"].drop_duplicates()
        ),
        ["Nome_bd", "E-mail"],
    ].rename(columns={"Nome_bd": "Operacional RF", "E-mail": "E-mail Operacional RF"})

    df_emails_traders = df_socios.loc[
        df_socios["Nome_bd"].isin(
            df_emails_pushes_reservas["Operacional RV"].drop_duplicates()
        ),
        ["Nome_bd", "E-mail"],
    ].rename(columns={"Nome_bd": "Operacional RV", "E-mail": "E-mail Operacional RV"})

    df_emails_pushes_reservas = pd.merge(
        df_emails_pushes_reservas,
        df_emails_tipos_comerciais,
        how="left",
        on="Comercial",
    )

    df_emails_pushes_reservas = pd.merge(
        df_emails_pushes_reservas, df_emails_advisors, how="left", on="Operacional RF"
    )

    df_emails_pushes_reservas = pd.merge(
        df_emails_pushes_reservas, df_emails_traders, how="left", on="Operacional RV"
    )

    return df_emails_pushes_reservas


def gera_base_reservas_ou_pushes_consolidados(base: str) -> None:
    """Gera base consolidade de pushes ou reservas e salva no S3.

    Args:
        base (str): Opção de 'pushes' ou 'reservas'.
    """

    context_script = gera_context_script()

    excecao_bases_possiveis(base)

    drive_microsoft = context_script["drive_microsoft"]
    df_lista_arquivos_individuais = drive_microsoft.list_drive_dir(
        context_script[f"lista_caminhos_{base}"]
    )
    if (
        not isinstance(df_lista_arquivos_individuais, pd.DataFrame)
        and df_lista_arquivos_individuais == None
    ):
        print(
            f"A pasta com os relatórios de {base} está vazia e não há nada a consolidar!"
        )
    else:
        df_lista_arquivos_individuais = df_lista_arquivos_individuais.loc[
            df_lista_arquivos_individuais["nome_arquivo"].str.endswith(".xlsx")
        ]
        df_arquivo_consolidado = pd.concat(
            [
                pd.read_excel(link_download)
                for link_download in df_lista_arquivos_individuais[
                    "link_download"
                ].to_list()
            ]
        )

        df_arquivo_consolidado = (
            df_arquivo_consolidado.loc[
                df_arquivo_consolidado["STATUS"].str.contains("pendente", case=False)
            ]
            if base == "pushes"
            else df_arquivo_consolidado
        )

        df_arquivo_consolidado.to_parquet(
            f"s3://{context_script['bucket_lake_gold']}/api/one/Ofertas_Publicas/acompanhamento_{base}.parquet"
        )
        print(f"O arquivo de '{base}' foi consolidado e salvo no S3!")


def gera_base_emails_pushes_reservas(base: str) -> None:
    """Gera base de envio de e-mails de pushes ou reservas e salva no S3.

    Args:
        base (str): Opção de 'pushes' ou 'reservas'.
    """

    context_script = gera_context_script()

    excecao_bases_possiveis(base)

    df_pushes_ou_reservas = carrega_pushes_ou_reservas(base)
    df_socios = carrega_socios()
    df_pl = carrega_pl()
    df_clientes_assessores = carrega_clientes_assessores()

    df_emails_pushes_reservas = estabelece_base_emails_pushes_reservas(
        df_pushes_ou_reservas, df_clientes_assessores, df_socios, df_pl
    )
    df_emails_pushes_reservas.to_parquet(
        f"s3://{context_script['bucket_lake_silver']}/btg/onedrive/{base}/base_emails_{base}/base_emails_{base}.parquet"
    )

    if base == "reservas":
        df_emails_pushes_reservas_para_back = df_emails_pushes_reservas.rename(
            columns={"TITULAR": "Nome"}
        )
        df_emails_pushes_reservas_para_back["Nome"] = (
            df_emails_pushes_reservas_para_back["Nome"].str.split(" ").str[0]
        )
        df_emails_pushes_reservas_para_back = df_emails_pushes_reservas_para_back.drop(
            columns=["Comercial", "Operacional RF", "Operacional RV"]
        )
        try:
            df_emails_pushes_reservas_para_back = (
                df_emails_pushes_reservas_para_back.loc[
                    :,
                    [
                        "Conta",
                        "Nome",
                        "Comercial (c. esp.)",
                        "Operacional RF (c. esp.)",
                        "Operacional RV (c. esp.)",
                        "FIM DA RESERVA",
                        "FIM DA RESERVA VINCULADOS",
                        "NOME DA OFERTA",
                        "VALOR DESEJADO",
                        "VALOR POSSÍVEL",
                        "PREÇO UNITÁRIO",
                        "TAXA SOLICITADA",
                    ],
                ]
            )
        except:
            df_emails_pushes_reservas_para_back = (
                df_emails_pushes_reservas_para_back.loc[
                    :,
                    [
                        "Conta",
                        "Nome",
                        "Comercial (c. esp.)",
                        "Operacional RF (c. esp.)",
                        "Operacional RV (c. esp.)",
                        "NOME DA OFERTA",
                        "ATIVO",
                        "DATA DE LIQUIDAÇÃO",
                        "VALOR OBTIDO",
                        "PREÇO UNITÁRIO",
                        "TAXA",
                        "Tipo",
                    ],
                ]
            )

        df_emails_pushes_reservas_para_back = (
            df_emails_pushes_reservas_para_back.rename(
                columns={
                    "Comercial (c. esp.)": "Comercial",
                    "Operacional RF (c. esp.)": "Operacional RF",
                    "Operacional RV (c. esp.)": "Operacional RV",
                }
            )
        )

    elif base == "pushes":
        df_emails_pushes_reservas_para_back = df_emails_pushes_reservas.rename(
            columns={"TITULAR": "Nome"}
        )
        df_emails_pushes_reservas_para_back["Nome"] = (
            df_emails_pushes_reservas_para_back["Nome"].str.split(" ").str[0]
        )
        df_emails_pushes_reservas_para_back = df_emails_pushes_reservas_para_back.drop(
            columns=["Comercial", "Operacional RF", "Operacional RV"]
        )
        df_emails_pushes_reservas_para_back = df_emails_pushes_reservas_para_back.loc[
            :,
            [
                "Conta",
                "Nome",
                "Comercial (c. esp.)",
                "Operacional RF (c. esp.)",
                "Operacional RV (c. esp.)",
                "TIPO DE OPERAÇÃO",
                "DATA DE SOLICITAÇÃO",
                "DATA LIMITE",
                "NOME DA OFERTA",
                "ATIVO",
                "STATUS",
                "VALOR",
                "PREÇO UNITÁRIO",
                "TAXA SOLICITADA",
            ],
        ]
        df_emails_pushes_reservas_para_back = (
            df_emails_pushes_reservas_para_back.rename(
                columns={
                    "Comercial (c. esp.)": "Comercial",
                    "Operacional RF (c. esp.)": "Operacional RF",
                    "Operacional RV (c. esp.)": "Operacional RV",
                }
            )
        )

    drive_microsoft = context_script["drive_microsoft"]
    with io.BytesIO() as buffer:
        df_emails_pushes_reservas_para_back.to_csv(
            buffer, encoding="latin1", sep=";", decimal=",", index=False
        )
        bytes_arquivo_consolidado = buffer.getvalue()
        drive_microsoft.upload_file(
            context_script[f"lista_caminhos_salvar_{base}"], bytes_arquivo_consolidado
        )
    print(
        f"O arquivo de '{base}' para envio de e-mails foi salvo no S3 e disponibilizado no formato CSV para o Back Office!"
    )


def monitora_arquivos_individuais(base: str) -> str:
    """Verifica se os arquivos baixados pelo Back Office e que são necessários ao processo foram atualizados nas pastas correspondentes do Drive.

    Args:
        base (str): Opção de 'pushes' ou 'reservas'.

    Returns:
        str: Tarefa que será executada dependendo da atualização ou não das planilhas no Drive.
    """

    context_script = gera_context_script()

    excecao_bases_possiveis(base)

    drive_microsoft = context_script["drive_microsoft"]
    lista_datas_arquivos_individuais = drive_microsoft.list_drive_dir(
        context_script[f"lista_caminhos_{base}"]
    )

    if lista_datas_arquivos_individuais is None:
        print(f"A pasta de {base} está vazia!")
        return "nao_faz_nada"

    lista_datas_arquivos_individuais = lista_datas_arquivos_individuais[
        "datetime_ultima_alteracao"
    ].to_list()

    # verificar o verdadeiro critério de atualizacao
    x_min = 15
    arquivos_foram_atualizados_ultimos_x_min = all(
        pd.Timestamp.now("UTC") - timestamp_arquivo_individual
        <= pd.Timedelta(hours=3, minutes=x_min)
        for timestamp_arquivo_individual in lista_datas_arquivos_individuais
    )

    # RODAR LOCAL
    # arquivos_foram_atualizados_ultimos_x_min = True

    if arquivos_foram_atualizados_ultimos_x_min:
        print(
            f"Os arquivos individuais de '{base}' foram atualizados nos últimos {x_min} minutos, vamos seguir com a consolidação das bases e envio dos e-mails"
        )
        return f"atualiza_consolidado_{base}"
    else:
        print(
            f"Os arquivos individuais de '{base}' não foram atualizados nos últimos {x_min} minutos, nada será feito."
        )
        return "nao_faz_nada"
