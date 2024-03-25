import pandas as pd
from utils.DagReceita.functions import (
    carrega_e_limpa_base_previdencia,
    carrega_e_limpa_base_guia_previdencia,
)
from deltalake import DeltaTable, write_deltalake


def prev_btg_bronze_2_silver(
    ano_particao: str,
    mes_particao: str,
    s3_silver_step_1_path: str,
    s3_bronze_path: str,
):
    """Esta função extrai e pre processa a base prev btg da camada
    bronze do data lake.

    Args:
        ano_particao (str): Ano da date_inverval_end da dag
        mes_particao (str): Mes da date_inverval_end da dag
        s3_silver_step_1_path (str): URI da tabela na camada silver
        s3_bronze_path (str): URI da tabela na camada bronze

    """
    rename_dict = {
        "Data da Posicao": "Data",
        "Certificado": "Certificado",
        "Documento Fundo": "CNPJ",
        "Conta Corrente": "Conta",
        "Saldo Financeiro": "Saldo",
        "Taxa de Administracao": "Taxa_adm",
        "Percentual de Comissao": "Repasse_adm",
        "Comissao Bruta": "Receita_bruta",
        "Comissao Liquida": "Comissao",
    }

    # Realiza a leitura da base e filtra apenas a última run dentro da
    # partição
    df = DeltaTable(
        s3_bronze_path,
    ).to_pandas(
        partitions=[
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ]
    )

    filtro = df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze"].max()
    df = (
        df.loc[filtro]
        .drop(
            columns=[
                "timestamp_dagrun",
                "timestamp_escrita_bronze",
                "Fundo",
                "Saldo em Cotas",
                "Assessor",
                "CPF",
            ]
        )
        .rename(columns=rename_dict)
        .copy()
    )

    # Trata a coluna de CNPJ
    df["CNPJ"] = df["CNPJ"].astype("int64").astype("str").str.zfill(14)

    df["atualizacao_incremental"] = "Sim"

    # Seleciona as colunas necessárias
    df = df.loc[
        :,
        [
            "Conta",
            "Data",
            "Certificado",
            "CNPJ",
            "Saldo",
            "Taxa_adm",
            "Repasse_adm",
            "Receita_bruta",
            "Comissao",
            "ano_particao",
            "mes_particao",
            "atualizacao_incremental",
        ],
    ]

    df = df.reset_index(drop=True)

    df["ano_particao"] = df["ano_particao"].astype("int64[pyarrow]")
    df["mes_particao"] = df["mes_particao"].astype("int64[pyarrow]")

    write_deltalake(
        s3_silver_step_1_path,
        df,
        partition_filters=[
            ("atualizacao_incremental", "=", "Sim"),
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ],
        mode="overwrite",
    )

    # Compactação da partição
    dt_posicao = DeltaTable(s3_silver_step_1_path)
    dt_posicao.optimize.compact(
        partition_filters=[
            ("atualizacao_incremental", "=", "Sim"),
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ],
    )
    print("Partição compactada")

    # Vacuum
    dt_posicao.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def carrega_e_limpa_previdencia_btg(df_prev_btg: pd.DataFrame) -> pd.DataFrame:
    """Recebe a base de dados prev btg e realiza o processamento e a aplicação
    de regras de negócio.
    Args:
        df_prev_btg (pd.DataFrame): Objeto do tipo DataFrame com a base de dados
        prev btg.

    Returns:
        pd.DataFrame:  Objeto do tipo DataFrame com a base de dados
        prev btg processada.
    """

    column_types = {
        "Conta": "str",
        "CNPJ": "str",
        "Certificado": "str",
        "Comissao": "float",
        "Data": "datetime64[ns]",
    }

    columns_rename = {
        "Conta": "Conta",
        "Data": "Data",
        "CNPJ": "Produto_2",
        "Certificado": "Certificado",
        "Comissao": "Comissão",
        "ano_particao": "ano_particao",
        "mes_particao": "mes_particao",
        "atualizacao_incremental": "atualizacao_incremental",
    }

    df_prev_btg = (
        df_prev_btg.loc[:, columns_rename.keys()]
        .astype(column_types)
        .rename(
            {
                "Conta": "Conta",
                "Data": "Data",
                "CNPJ": "Produto_2",
                "Certificado": "Certificado",
                "Comissao": "Comissão",
            },
            axis=1,
        )
    )

    df_prev_btg["Produto_1"] = "ADMINISTRAÇAO"
    filtro = df_prev_btg["Produto_2"].str.contains("campanha", case=False)
    df_prev_btg.loc[filtro, "Produto_1"] = "CAMPANHA"

    # Carrega e prepara a base previdencia
    df_previdencia = carrega_e_limpa_base_previdencia()
    df_previdencia = df_previdencia.sort_values(
        by="Categoria", na_position="last"
    ).drop_duplicates(subset="Certificado", keep="first")

    # Carrega e prepara a base guia previdencia
    df_guia_previdencia = carrega_e_limpa_base_guia_previdencia()

    # Em um número de certificado terá previdências de apenas uma categoria (PGBL ou VGBL)
    # O trecho abaixo irá extrair da base previdência esta informação que será a
    # coluna Categoria da base.
    df_prev_btg = df_prev_btg.merge(
        df_previdencia[["Certificado", "Categoria"]],
        on=["Certificado"],
        how="left",
        validate="m:1",
    ).drop("Certificado", axis=1)
    df_prev_btg["Categoria"] = df_prev_btg["Categoria"].fillna("NAO IDENTIFICADO")

    # Extrai o fornecedor da base Prev_guide
    df_prev_btg = df_prev_btg.merge(
        df_guia_previdencia[["CNPJ", "Fornecedor"]],
        left_on="Produto_2",
        right_on="CNPJ",
        how="left",
        validate="m:1",
    ).drop("CNPJ", axis=1)
    df_prev_btg["Fornecedor"] = df_prev_btg["Fornecedor"].fillna("NAO IDENTIFICADO")

    # Definições de negócio que serão comuns para toda a base de dados.
    df_prev_btg["Financeira"] = "BTG Pactual"
    df_prev_btg["Intermediador"] = "BTG Pactual"
    df_prev_btg["Tipo"] = "PREVIDENCIA PRIVADA"

    # Regra de negócio definida junto ao Guilherme Isaac.
    df_prev_btg.loc[
        (df_prev_btg["Produto_2"] == "CampanhaPREV22"), "Data"
    ] = pd.Timestamp("2022-08-31")

    return df_prev_btg
