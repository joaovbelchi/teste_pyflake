import pandas as pd
import json
import boto3
import pytz
from datetime import datetime
from typing import Callable
from deltalake import DeltaTable, write_deltalake
from utils.OneGraph.drive import OneGraphDrive
from utils.msteams import msteams_qualidade_dados
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name
from airflow.models import Variable
from dateutil.relativedelta import relativedelta


def leitura_parquet_landing(path: str) -> pd.DataFrame:
    """Função que realiza a leitura de um arquivo em .parquet

    Args:
        path (str): URI do objeto salvo em parquet no S3.

    Returns:
        pd.DataFrame: Base de dados em um DataFrame pandas.
    """
    df = pd.read_parquet(path)
    df = df.reset_index(drop=True)
    return df


def leitura_excel_landing(path: str) -> pd.DataFrame:
    """Função que realiza a leitura de um arquivo em .xlsx

    Args:
        path (str): URI do objeto em parquet no S3

    Returns:
        pd.DataFrame: Base de dados em um DataFrame pandas.
    """
    df = pd.read_excel(path)
    return df


def processa_bases_incrementais(
    atualizacao: str,
    ano_particao: int,
    mes_particao: int,
    s3_silver_step_1_path: str,
    s3_silver_step_2_path: str,
    schema: dict,
    funcao_processamento: Callable,
):
    """É um wrapper para o processamento das bases de dados
    da camada silver. Esta função irá verificar se a atualização
    é ou não histórica e carregará a base de acordo com esta validação.
    adicionalmente o processo de salvamento da base de dados será
    executado também por esta função. O processamento da base de dados
    será feita pela função que deverá ser passada no parâmetro
    funcao_processamento.

    Args:
        atualizacao (str): Tipo do processamento que se deseja executar
        (histórica ou incremental)
        ano_particao (str): Ano da partição que está sendo processada
        mes_particao (str): Mes da partição que está sendo processada
        s3_silver_step_1_path (str): path do s3 onde a base pre processada
        está salva.
        s3_silver_step_2_path (str): path do s3 onde a base processada
        será salva.
        funcao_processamento (Callable): função de processamento da base
    """
    # Se a atualização for histórica a base de dados é totalmente
    # carregada, processada e salva.
    if atualizacao == "histórica":
        df = DeltaTable(
            s3_silver_step_1_path,
        ).to_pandas()

        # Faz processamento da base de dados
        df = funcao_processamento(df)

        # Aplica o schema na base de dados
        for col in df.columns:
            df[col] = df[col].astype(f"{schema[col]['data_type']}[pyarrow]")
        print("Schema aplicado no DataFrame")

        # Salva a base de dados
        write_deltalake(
            s3_silver_step_2_path,
            df,
            mode="overwrite",
            partition_by=["atualizacao_incremental", "ano_particao", "mes_particao"],
        )

        # Compactação da partição
        dt_posicao = DeltaTable(s3_silver_step_2_path)
        dt_posicao.optimize.compact()
        print("Partição compactada")

        # Vacuum
        dt_posicao.vacuum(dry_run=False)
        print("Arquivos antigos apagados")

    # Caso a atualização seja incremetnal apenas a partição específica
    # será carregada, processada e salva no step 2 da camada silver.
    elif atualizacao == "incremental":
        df = DeltaTable(
            s3_silver_step_1_path,
        ).to_pandas(
            partitions=[
                ("atualizacao_incremental", "=", "Sim"),
                ("ano_particao", "=", str(ano_particao)),
                ("mes_particao", "=", str(mes_particao)),
            ],
        )

        df = funcao_processamento(df)

        # Aplica dtypes
        for col in df.columns:
            df[col] = df[col].astype(f"{schema[col]['data_type']}[pyarrow]")
        print("Schema aplicado no DataFrame")

        write_deltalake(
            s3_silver_step_2_path,
            df,
            mode="overwrite",
            partition_filters=[
                ("atualizacao_incremental", "=", "Sim"),
                ("ano_particao", "=", str(ano_particao)),
                ("mes_particao", "=", str(mes_particao)),
            ],
        )

        # Compactação da partição
        dt_posicao = DeltaTable(s3_silver_step_2_path)
        dt_posicao.optimize.compact(
            partition_filters=[
                ("atualizacao_incremental", "=", "Sim"),
                ("ano_particao", "=", str(ano_particao)),
                ("mes_particao", "=", str(mes_particao)),
            ]
        )
        print("Partição compactada")

        # Vacuum
        dt_posicao.vacuum(dry_run=False)
        print("Arquivos antigos apagados")


def processa_bases_historicas(
    ano_particao: str,
    mes_particao: str,
    s3_bronze_path: str,
    s3_silver_step_2_path: str,
    funcao_processamento: Callable,
):
    """É um wrapper para o processamento das bases de dados
    da camada silver. Esta função sempre irá processar historicamente
    a base de dados.

    Args:
        ano_particao (str): Ano da partição que está sendo processada
        mes_particao (str): Mes da partição que está sendo processada
        s3_bronze_path (str): path do s3 para a camada bronze
        onde a base está salva
        s3_silver_step_2_path (str): path do s3 onde a base processada
        será salva.
        funcao_processamento (Callable): função de processamento da base
    """

    df = DeltaTable(s3_bronze_path).to_pandas(
        partitions=[
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ]
    )

    filtro_escrita_bronze = (
        df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze"].max()
    )
    df = df.loc[filtro_escrita_bronze].copy()

    df = funcao_processamento(df)

    df.to_parquet(s3_silver_step_2_path)


def bronze_2_silver(
    ano_particao: str,
    mes_particao: str,
    s3_silver_step_1_path: str,
    s3_bronze_path: str,
    schema: dict,
):
    """Esta função faz a transferência de bases de dados da camada
    bronze para a camada silver. Para estas bases não há a aplicação de
    nenhum tipo de processamento apenas a adição da coluna 'atualizacao_
    incremental' que indica se a base está sendo atualizada de forma
    incremental.

    Args:
        ano_particao (str): Ano partição extraído da data que a DAG está rodando
        mes_particao (str): Mês partição extraído da data que a DAG está rodando
        s3_silver_step_1_path (str): Path para a base de dados na camada silver
        s3_bronze_path (str): Path para a base na camada bronze
    """

    df = DeltaTable(
        s3_bronze_path,
    ).to_pandas(
        partitions=[
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ]
    )

    filtro_mais_recente = (
        df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze"].max()
    )

    df["atualizacao_incremental"] = "Sim"

    df = (
        df.loc[filtro_mais_recente, :]
        .drop(columns=["timestamp_dagrun", "timestamp_escrita_bronze"])
        .reset_index(drop=True)
    )

    # Aplica dtypes
    df["ano_particao"] = df["ano_particao"].astype("int64[pyarrow]")
    df["mes_particao"] = df["mes_particao"].astype("int64[pyarrow]")
    skip_col = ["atualizacao_incremental", "ano_particao", "mes_particao"]

    for col in df.columns:
        if col in skip_col:
            continue
        df[col] = df[col].astype(f"{schema[col]['data_type']}[pyarrow]")
    print("Schema aplicado no DataFrame")

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

    # Otimiza arquivos em partições
    # e exclui arquivos antigos
    dt = DeltaTable(s3_silver_step_1_path)
    dt.optimize.compact(
        partition_filters=[
            ("atualizacao_incremental", "=", "Sim"),
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ]
    )
    print("Partição compactada")

    # Vacuum
    dt.vacuum(dry_run=False)
    print("Arquivos antigos apagados")


def carrega_e_limpa_base_previdencia(drive_id=None) -> pd.DataFrame:
    """Esta função carrega e limpa a base de previdencias

    Args:
        path (str): Caminho para a base de previdências

    Returns:
        pd.DataFrame: Data Frame com a base de previdências
    """

    column_types = {
        "account": "str",
        "name": "str",
        "cnpj": "str",
        "product": "str",
        "stock": "str",
        "prev_type": "str",
    }

    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive_id = Variable.get("guilherme_drive_id")
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    path = ["One Analytics", "Banco de dados", "Histórico", "PREVIDÊNCIA.csv"]
    df_previdencia = drive.download_file(
        path=path,
        to_pandas=True,
        sep=";",
        usecols=["account", "name", "cnpj", "product", "stock", "prev_type", "date"],
        dtype=column_types,
        parse_dates=["date"],
    ).rename(
        {
            "account": "Conta",
            "name": "Nome",
            "cnpj": "CNPJ",
            "product": "Produto",
            "stock": "Certificado",
            "prev_type": "Categoria",
            "date": "Data",
        },
        axis=1,
    )

    df_previdencia["CNPJ"] = df_previdencia["CNPJ"].str.split(",").str[0]
    df_previdencia["Certificado"] = (
        df_previdencia["Certificado"].str.findall(r"\d+").str[-1]
    )
    df_previdencia["Certificado"] = df_previdencia["Certificado"].str.lstrip("0")

    # Garante que a chave Certificado é única
    df_previdencia = df_previdencia.drop_duplicates("Certificado")

    return df_previdencia


def carrega_e_limpa_base_guia_previdencia(drive_id: str = None) -> pd.DataFrame:
    """Carrega e limpa a base guida de previdencia

    Args:
        path (str): Caminho para a base guia de previdencia
        sheet_name (str): Nome da planilha do arquivo excel que será carregado.

    Returns:
        pd.DataFrame: Data Frame com a base guia de previdência
    """
    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive_id = Variable.get("guilherme_drive_id")
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    path = [
        "One Analytics",
        "Banco de dados",
        "Histórico",
        "Receita",
        "Informacoes",
        "Prev_Guide.xlsx",
    ]

    df_guia_previdencia_1 = drive.download_file(
        path=path,
        to_pandas=True,
        header=2,
        usecols=["Fundo", "CNPJ", "Gestão"],
        sheet_name="Universo de Fundos",
        dtype={"Fundo": "str", "CNPJ": "str", "Gestão": "str"},
    ).rename({"Gestão": "Fornecedor"}, axis=1)

    df_guia_previdencia_1["CNPJ"] = df_guia_previdencia_1["CNPJ"].str.replace(
        r"[^0-9]", "", regex=True
    )

    df_guia_previdencia_2 = (
        drive.download_file(
            path=path,
            to_pandas=True,
            header=2,
            usecols=["Fundo", "CNPJ", "Gestão"],
            sheet_name="Fundos PREV Plataforma BTG",
        )
        .rename({"Gestão": "Fornecedor"}, axis=1)
        .astype({"Fundo": "str", "CNPJ": "str", "Fornecedor": "str"})
    )

    df_guia_previdencia_2 = df_guia_previdencia_2.rename(
        {"Gestão": "Fornecedor"}, axis=1
    )

    df_guia_previdencia_2["CNPJ"] = df_guia_previdencia_2["CNPJ"].str.replace(
        r"[^0-9]", "", regex=True
    )

    # Garante que a chave CNPJ é única
    df_guia_previdencia = pd.concat([df_guia_previdencia_1, df_guia_previdencia_2])

    df_guia_previdencia = df_guia_previdencia.drop_duplicates("CNPJ")

    return df_guia_previdencia


def processa_consolidado_historico_silver(
    ano_particao: int,
    mes_particao: int,
    bases_processadas: dict,
    bases_historicas: list,
):
    """Une as bases intermediárias da receita em uma única
    tabela que é salva na camada silver do DataLake.

    Args:
        ano_particao (int): Ano da partição que está sendo processada
        mes_particao (int): Mes da partição que está sendo processada
        bases_processadas (dict): Dicionário com as bases que estão
        sendo processadas
        bases_historicas (list): lista com as bases que possuem
        processamento histórico
    """

    s3_bucket_silver = get_bucket_name("lake-silver")
    # Lista as bases que são incrementais e históricas
    bases_incrementais = [
        base for base in bases_processadas if base not in bases_historicas
    ]
    bases_processamento_historico = [
        base for base in bases_processadas if base in bases_historicas
    ]

    # Carrega as bases que possuem atualização incremental e atualiza as
    # partições da tabela consolidado histórico
    for base in bases_incrementais:
        if bases_processadas[base] == "incremental":
            df = DeltaTable(
                f"s3://{s3_bucket_silver}/btg/onedrive/receita/{base}_processada/"
            ).to_pandas(
                partitions=[
                    ("atualizacao_incremental", "=", "Sim"),
                    ("ano_particao", "=", str(ano_particao)),
                    ("mes_particao", "=", str(mes_particao)),
                ]
            )

            df["Base"] = base
            df = df.drop(columns="atualizacao_incremental")
            df = df.loc[
                :,
                [
                    "Conta",
                    "Categoria",
                    "Produto_1",
                    "Produto_2",
                    "Comissão",
                    "Data",
                    "ano_particao",
                    "mes_particao",
                    "Tipo",
                    "Fornecedor",
                    "Intermediador",
                    "Financeira",
                    "Base",
                ],
            ]

            write_deltalake(
                f"s3://{s3_bucket_silver}/btg/onedrive/receita/consolidado_historico_silver/",
                df,
                partition_filters=[
                    ("Base", "=", base),
                    ("ano_particao", "=", str(ano_particao)),
                    ("mes_particao", "=", str(mes_particao)),
                ],
                partition_by=["Base", "ano_particao", "mes_particao"],
                mode="overwrite",
            )

            # Otimiza arquivos em partições
            # e exclui arquivos antigos
            dt = DeltaTable(
                f"s3://{s3_bucket_silver}/btg/onedrive/receita/consolidado_historico_silver/"
            )
            dt.optimize.compact(
                partition_filters=[
                    ("Base", "=", base),
                    ("ano_particao", "=", str(ano_particao)),
                    ("mes_particao", "=", str(mes_particao)),
                ],
            )
            print("Partição compactada")

            dt.vacuum(dry_run=False)
            print("Arquivos antigos apagados")

        # Carrega as bases que possuem atualização histórica e atualiza as
        # partições da tabela consolidado histórico
        if bases_processadas[base] == "histórica":
            df = DeltaTable(
                f"s3://{s3_bucket_silver}/btg/onedrive/receita/{base}_processada/"
            ).to_pandas()

            print("base lida")

            df["Base"] = base
            df = df.drop(columns="atualizacao_incremental")
            df = df.loc[
                :,
                [
                    "Conta",
                    "Categoria",
                    "Produto_1",
                    "Produto_2",
                    "Comissão",
                    "Data",
                    "ano_particao",
                    "mes_particao",
                    "Tipo",
                    "Fornecedor",
                    "Intermediador",
                    "Financeira",
                    "Base",
                ],
            ]

            write_deltalake(
                f"s3://{s3_bucket_silver}/btg/onedrive/receita/consolidado_historico_silver/",
                df,
                partition_filters=[("Base", "=", base)],
                partition_by=["Base", "ano_particao", "mes_particao"],
                mode="overwrite",
            )

            print("base salva")

    for base in bases_processamento_historico:
        df = pd.read_parquet(
            f"s3://{s3_bucket_silver}/btg/onedrive/receita/{base}_processada/"
        )

        if (base == "semanal") & (df.empty):
            dt = DeltaTable(
                f"s3://{s3_bucket_silver}/btg/onedrive/receita/consolidado_historico_silver/"
            )
            dt.delete("Base='semanal'")
            continue

        

        df["Base"] = base
        df["ano_particao"] = ano_particao
        df["mes_particao"] = mes_particao

        df = df.loc[
            :,
            [
                "Conta",
                "Categoria",
                "Produto_1",
                "Produto_2",
                "Comissão",
                "Data",
                "ano_particao",
                "mes_particao",
                "Tipo",
                "Fornecedor",
                "Intermediador",
                "Financeira",
                "Base",
            ],
        ]

        df = df.reset_index(drop=True)

        write_deltalake(
            f"s3://{s3_bucket_silver}/btg/onedrive/receita/consolidado_historico_silver/",
            df,
            partition_filters=[
                ("Base", "=", base),
            ],
            partition_by=["Base", "ano_particao", "mes_particao"],
            mode="overwrite",
        )

        # Otimiza arquivos em partições
        # e exclui arquivos antigos
        dt = DeltaTable(
            f"s3://{s3_bucket_silver}/btg/onedrive/receita/consolidado_historico_silver/"
        )
        dt.optimize.compact()
        print("Partição compactada")

        dt.vacuum(dry_run=False)
        print("Arquivos antigos apagados")


def calcula_receita_consolidada_aplicativo(ano_particao: int, mes_particao: int):
    """Realiza o cálculo de uma base auxiliar que é utilizada pelo aplicativo
    de receita. Esta base possui o somatório da receita dos últimos 6 meses
    segregada por tipo de receita e é armazenada na camada gold do
    Data Lake.

    Args:
        ano_particao (int): Ano do processamento da base receita
        mes_particao (int): Mes do processamento da base receita
    """

    s3_bucket_gold = get_bucket_name("lake-gold")
    data_processamento = datetime(ano_particao, mes_particao, 1)
    data_inicio = data_processamento + relativedelta(months=-6)
    ano_inicio = data_inicio.year
    mes_inicio = data_inicio.month

    df_receita = pd.read_parquet(
        f"s3://{s3_bucket_gold}/btg/receita/tb_consolidado_historico_gold/",
        filters=[
            [
                ("ano_particao", ">=", ano_inicio),
                ("mes_particao", ">", mes_inicio),
            ],
            [("ano_particao", ">", ano_inicio)],
        ],
    )

    filtro = df_receita["data"] >= data_inicio
    df_receita_por_mes_e_tipo = (
        df_receita.loc[filtro, ["mes", "tipo", "receita_bruta"]]
        .astype({"mes": "str"})
        .groupby(["mes", "tipo"], as_index=False, dropna=False)["receita_bruta"]
        .sum()
    )

    df_receita_por_mes_e_tipo_pivot = pd.pivot_table(
        df_receita_por_mes_e_tipo,
        values="receita_bruta",
        index=["tipo"],
        columns=["mes"],
        aggfunc="sum",
    )

    df_receita_por_mes_e_tipo_pivot.to_parquet(
        f"s3://{s3_bucket_gold}/btg/receita/resumo_receita_aplicativo/resumo_receita_aplicativo.parquet"
    )


def qualidade_silver(ano_particao: str, mes_particao: str):
    """Verifica se as categorias presentes nas colunas
    Tipo e Categoria da tabela consolidado_historico_silver
    seguem o padrão esperado nas variáveis definidas nesta
    etapa de qualidade.

    Args:
        ano_particao (str): Ano da data de processamento do
        pipeline
        mes_particao (str): Mes da data de processamento do
        pipeline
    """

    s3_bucket_silver = get_bucket_name("lake-silver")

    categorias_na_coluna_tipo = [
        "AJUSTE",
        "BANKING",
        "CAMBIO",
        "CONSORCIO",
        "CONSULTORIA EMPRESARIAL",
        "CREDITO",
        "FINANCIAMENTO IMOBILIARIO",
        "M&A",
        "MERCADO DE CAPITAIS",
        "OFFSHORE",
        "PLANEJAMENTO SUCESSORIO",
        "PREVIDENCIA PRIVADA",
        "RECEITA NAO OPERACIONAL",
        "SEGUROS",
        "VALORES MOBILIARIOS",
    ]

    categorias_na_coluna_categoria = [
        "AJUSTE",
        "ASSINATURA BANKING",
        "CARTAO",
        "CREDITO",
        "Mensalidade - Plano Avancado",
        "Saldo",
        "plus",
        "BANKING",
        "CAMBIO",
        "CAMBIO TURISMO",
        "CANCELAMENTO",
        "ENCARGOS",
        "ENVIO",
        "ERRO CÂMBIO",
        "INCENTIVO",
        "INVESTIMENTO",
        "NAO IDENTIFICADO",
        "NDF",
        "RECEBIMENTO",
        "CONSORCIO AUTO",
        "CONSULTORIA EMPRESARIAL",
        "ACC",
        "CAPITAL DE GIRO",
        "CCB",
        "CCBR",
        "CCE",
        "CONTA VIRADA",
        "CPR-F",
        "FIANÇA",
        "LOAN",
        "MANDATO",
        "NAO IDENTIFICADO",
        "NCE",
        "NCE - Rolagem",
        "RISCO SACADO",
        "Rural",
        "FINANCIAMENTO IMOBILIARIO",
        "INDICACAO",
        "MANDATO",
        "CRI",
        "DCM",
        "DERIVATIVOS",
        "RENDA FIXA",
        "AVENUE",
        "CAYMAN",
        "US",
        "PLANEJAMENTO SUCESSORIO",
        "NAO IDENTIFICADO",
        "PGBL",
        "VGBL",
        "BONUS",
        "LEROY",
        "BONUS",
        "D&O",
        "FIANÇA LOCATIVA",
        "NAO IDENTIFICADO",
        "PLANO DE SAÚDE",
        "RESPONSABILIDADE CIVIL",
        "SEGURO AUTO",
        "SEGURO D&O",
        "SEGURO DE EVENTOS",
        "SEGURO DE VIDA",
        "SEGURO E&O",
        "SEGURO EMPRESARIAL",
        "SEGURO GARANTIA",
        "SEGURO PATRIMONIAL",
        "SEGURO RESIDENCIAL",
        "SEGURO RESPONSABILIDADE CIVIL",
        "SEGURO SAUDE",
        "SEGURO SAUDE INTERNACIONAL",
        "SEGURO VIAGEM",
        "CRIPTOMOEDA",
        "CUSTOS",
        "DERIVATIVOS",
        "ERRO OPERACIONAL",
        "FUNDOS",
        "FUNDOS GESTORA",
        "NAO IDENTIFICADO",
        "RENDA FIXA",
        "RENDA VARIAVEL",
        "SALDO REMUNERADO",
    ]

    df = DeltaTable(
        f"s3://{s3_bucket_silver}/btg/onedrive/receita/consolidado_historico_silver/"
    ).to_pandas(
        partitions=[
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ]
    )

    # Valida a coluna tipo
    filtro_tipo_novo = ~(df["Tipo"].isin(categorias_na_coluna_tipo))
    tem_tipo_novo = not (df[filtro_tipo_novo].empty)

    if tem_tipo_novo:
        novos_tipos = df[filtro_tipo_novo]["Tipo"].unique().tolist()
    else:
        novos_tipos = None

    # Valida a coluna categoria
    filtro_categoria_novo = ~(df["Categoria"].isin(categorias_na_coluna_categoria))
    tem_categoria_novo = not (df[filtro_categoria_novo].empty)
    if tem_categoria_novo:
        novas_categorias = df[filtro_categoria_novo]["Categoria"].unique().tolist()
    else:
        novas_categorias = None

    condicao = tem_tipo_novo or tem_categoria_novo

    if condicao:
        # Cria o log de qualidade
        log_qualidade_json = json.dumps(
            {
                "bucket": s3_bucket_silver,
                "tabela": "consolidado_historico_silver",
                "novas_categorias_tipo": novos_tipos,
                "novas_categorias_categoria": novas_categorias,
                "criado_em": datetime.now(tz=pytz.utc).isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        )

        print(log_qualidade_json)

        webhook = Variable.get("msteams_webhook_qualidade")
        msteams_qualidade_dados(log_qualidade_json, webhook)
