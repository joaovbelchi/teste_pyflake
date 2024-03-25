import pandas as pd
from datetime import datetime
import json
from utils.secrets_manager import get_secret_value
from utils.OneGraph.drive import OneGraphDrive
import re
from airflow.models import Variable


def carrega_e_limpa_bases_de_para(sheet_name: str) -> pd.DataFrame:
    """Esta função carrega e retorna um Data Frame com as planilhas que possuem as relações
    'de-para' para a troca de entradas da base de dados.

    Args:
        path (str): Caminho para o arquivo excel correcao_categorias_consolidado_historico.
        sheet_name (str): Nome da aba do arquivo excel com a tabela que será carregada.

    Returns:
        pd.DataFrame: Data Frame com a planilha do arquivo excel.
    """

    credentials = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )
    drive_id = Variable.get("guilherme_drive_id")
    drive = OneGraphDrive(drive_id=drive_id, **credentials)

    path = ["One Analytics", "Banco de dados", "Histórico", "Receita", "de_para.xlsx"]
    df = drive.download_file(path=path, to_pandas=True, sheet_name=sheet_name)

    return df


def carrega_e_limpa_base_fund_guide() -> pd.DataFrame:
    """Esta função carrega e limpa a base de dados fund guide.

    Args:
        path (str): Caminho para a base fund guide.

    Returns:
        pd.DataFrame: Data Frame com a base fund guide limpa.
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
        "Bases Intermediárias",
        "AUX FUND GUIDE",
        "fund_guide.xlsx",
    ]

    column_types = {"CNPJ": "str", "Gestão": "str"}

    df_fund_guide = drive.download_file(
        path=path,
        to_pandas=True,
        header=2,
        sheet_name="Universo de Fundos",
        usecols=["CNPJ", "Gestão"],
    ).astype(column_types)

    # Remove os caracteres especiais da coluna CNPJ retornando apenas os números
    df_fund_guide["CNPJ"] = df_fund_guide["CNPJ"].str.replace(r"[^0-9]", "", regex=True)
    df_fund_guide = df_fund_guide.drop_duplicates("CNPJ")

    return df_fund_guide


def corrige_cnpj(cnpj: str) -> str:
    """Esta função completa com zeros a esquerda uma string contendo um código CNPJ que esteja incompleto

    Args:
        cnpj (str): Código CNPJ que será tratado

    Returns:
        str: Código CNPJ tratado
    """

    e_numerico = cnpj.isnumeric()
    len_cnpj = len(cnpj)
    e_cnpj = (len_cnpj >= 11) & (len_cnpj < 14)

    if e_numerico & e_cnpj:
        n_zeros = ["0"] * (14 - len_cnpj)
        cnpj = "".join(n_zeros) + cnpj

    return cnpj


def corrige_produto_2(texto: str) -> str:
    """Esta função realiza um dos passos de tratamento dos textos da coluna Produto_2 da base AAI.
    O passo realizado por esta função se aplica a limpeza de textos que possua o seguinte caracter
    "|". O texto será dividido em "|" e será retornada a string com maior comprimento de forma a isolar
    o código do presente no Produto_2

    Args:
        texto (str): Texto que será tratado

    Returns:
        str: Texto tratado
    """
    texto_dividido = re.split(r"\s\|\s", texto)
    texto_corrigido = max(texto_dividido, key=len)

    return texto_corrigido


def carrega_e_limpa_base_aai(df_aai: pd.DataFrame) -> pd.DataFrame:
    """Esta função Carrega e limpa a base AAI.

    Args:
        path (str): Caminho para a base AAI.
        df_posicao (pd.DataFrame): Base posicao tratada.

    Returns:
        pd.DataFrame: Data Frame com a base AAI limpa.
    """

    column_types = {
        "Conta": "str",
        "Categoria": "str",
        "Produto": "str",
        "Ativo": "str",
        "Código/CNPJ": "str",
        "Comissão": "float",
        "Tipo Receita": "str",
        "ano_particao": "str",
        "mes_particao": "str",
    }

    columns = [
        "Conta",
        "Categoria",
        "Produto",
        "Ativo",
        "Código/CNPJ",
        "Comissão",
        "Ajuste data2",
        "Tipo Receita",
        "ano_particao",
        "mes_particao",
        "atualizacao_incremental",
    ]

    # Correções feitas na base original.
    df_aai["Comissão"] = df_aai["Comissão"].fillna(0)
    df_aai["Conta"] = df_aai["Conta"].fillna("0")

    df_aai = df_aai.loc[:, columns].astype(column_types).copy()

    df_aai = df_aai.rename(
        {"Produto": "Produto_1", "Código/CNPJ": "Produto_2", "Ajuste data2": "Data"},
        axis=1,
    )

    # Corrige problema ao carregar a base
    df_aai["Conta"] = df_aai["Conta"].str.split(".").str[0]

    # Correções gerais
    df_aai["Tipo Receita"] = df_aai["Tipo Receita"].fillna("NAO IDENTIFICADO")

    # Inclui a definição de negócio sobre o tipo da base de dados.
    df_aai["Tipo"] = "VALORES MOBILIARIOS"

    # Criando uma variável auxiliar que será utilizada nas junções e na
    # criação do Produto_2 da categoria de RENDA FIXA
    # Preenche os Nans para permitir os tratamentos de string abaixo
    df_aai["Produto_2"] = df_aai["Produto_2"].fillna("")

    df_aai["Produto_2_aux"] = df_aai["Produto_2"]

    # O trecho abaixo realiza diversos tratamento em uma coluna
    # com códigos de ativos. Este tratamentos tem como objetivo
    # isolar os códigos de ativos específicos, bem como, tratar
    # códigos que são cnpjs.
    df_aai["Produto_2_aux"] = (
        df_aai["Produto_2_aux"]
        .apply(corrige_produto_2)
        .str.split(" ")
        .str[0]
        .str.split("CDB-")
        .str[-1]
        .str.split("CRA-")
        .str[-1]
        .str.replace("-", "")
        .str.split(".")
        .str[0]
        .apply(corrige_cnpj)
    )

    # Onde não houver Produto_2 preenche com o Produto_1
    filtro = df_aai["Produto_2"] == ""
    df_aai.loc[filtro, "Produto_2"] = df_aai["Produto_1"]

    # Fornecedor de FUNDOS
    # O fornecedor está na base fund guide na coluna Gestão, a chave pra fazer essa busca é a coluna CNPJ
    # na base fund guide e coluna Produto_2 na base AAI.
    df_fund_guide = carrega_e_limpa_base_fund_guide()
    df_aai = df_aai.merge(
        df_fund_guide,
        left_on="Produto_2_aux",
        right_on="CNPJ",
        how="left",
        validate="m:1",
    ).drop("CNPJ", axis=1)

    filtro = df_aai["Categoria"].isin(
        ["FUNDOS", "PRÊMIO", "FIP", "AJUSTE", "AJUSTE COMISSAO"]
    )
    df_aai.loc[filtro, "Fornecedor"] = df_aai["Gestão"]
    df_aai = df_aai.drop("Gestão", axis=1)

    # Carrega base de posição
    df_posicao = pd.read_parquet(
        "s3://prd-dados-lake-silver-oneinvest/btg/onedrive/receita/posicao_auxiliar/"
    )

    # Fornecedor de Renda Fixa e Variável
    # O fornecedor está na base posicao na coluna emissor,
    # a chave para fazer essa busca é a coluna Ativo na base posicao e coluna Produto_2 na base AAI.
    df_aai = (
        df_aai.merge(
            df_posicao,
            left_on="Produto_2_aux",
            right_on="Ativo",
            how="left",
            validate="m:1",
        )
        .rename({"Ativo_x": "Ativo"}, axis=1)
        .drop("Ativo_y", axis=1)
    )

    filtro = (df_aai["Produto_1"].isin(["CDB", "LCI", "LCA", "LF", "LFSN"])) & (
        df_aai["Produto_2_aux"].str.startswith("XX")
    )  # neste caso específico o fornecedor está na própria base AAI.
    df_aai.loc[filtro, "Emissor"] = df_aai["Ativo"]

    filtro = df_aai["Categoria"].isin(
        [
            "RENDA FIXA",
            "OFERTA PRIMÁRIA",
            "OFERTA PÚBLICA RF",
            "ERRO OPERACIONAL RF",
            "AJUSTE",
            "AJUSTE COMISSAO",
        ]
    ) & (df_aai["Fornecedor"].isna())
    df_aai.loc[filtro, "Fornecedor"] = df_aai["Emissor"]

    filtro = df_aai["Produto_2"].isin(["SECUNDARIO CETIPADO"])
    df_aai.loc[filtro, "Fornecedor"] = "BTG PACTUAL"

    # Fornecedor das demais categorias está em uma base "de-para".
    df_map_fornecedores = carrega_e_limpa_bases_de_para("aai_fornecedores")
    df_map_fornecedores.set_index("categoria_antiga", inplace=True)
    dict_map_fornecedores = df_map_fornecedores["fornecedor"].to_dict()

    # A Coluna Fornecedor_aux irá mapear cada categoria para o seu fornecedor na base "de-para".
    df_aai["Fornecedor_aux"] = df_aai["Categoria"]
    df_aai["Fornecedor_aux"] = df_aai["Fornecedor_aux"].replace(dict_map_fornecedores)
    filtro = (df_aai["Categoria"].isin(df_map_fornecedores.index)) & (
        df_aai["Fornecedor"].isna()
    )
    df_aai.loc[filtro, "Fornecedor"] = df_aai["Fornecedor_aux"]

    # Para os casos onde fornecedor não foi encontrado será NAO IDENTIFICADO.
    df_aai["Fornecedor"].fillna("NAO IDENTIFICADO", inplace=True)

    df_aai = df_aai.drop(["Emissor", "Fornecedor_aux"], axis=1)

    df_aai["Intermediador"] = "BTG PACTUAL"
    df_aai["Financeira"] = "BTG PACTUAL"

    # Em alguns casos a informação da categoria está em outras colunas, o código abaixo busca essas informações
    # e inclui na coluna Categoria.
    # Categoria = AJUSTE
    filtro = df_aai["Categoria"] == "AJUSTE"
    df_aai.loc[filtro, "Categoria"] = df_aai["Ativo"]

    # Categoria = AJUSTE COMISSAO
    filtro = df_aai["Categoria"] == "AJUSTE COMISSAO"
    df_aai.loc[filtro, "Categoria"] = df_aai["Produto_1"]

    filtro = (df_aai["Categoria"] == "AJUSTE COMISSAO") & (
        df_aai["Produto_1"] == "AJUSTE COMISSAO"
    )
    df_aai.loc[filtro, "Categoria"] = df_aai["Ativo"]

    filtro = df_aai["Produto_1"] == "SECUNDARIO CETIPADO"
    df_aai.loc[filtro, "Categoria"] = df_aai["Produto_1"]

    # Troca dos nomes das categorias originais da base por nomes com maior sentido para o negocio
    # a base "de-para" fica armazenada em um arquivo excel atualizado manualmente
    df_map_categoria = carrega_e_limpa_bases_de_para("aai_categorias")

    # Cria um dicionario com a relação categoria_antiga: categoria_nova
    df_map_categoria.set_index("categoria_antiga", inplace=True)
    dict_correcao_aai_categoria = df_map_categoria["categoria_nova"].to_dict()

    # Troca o nome das categorias
    df_aai["Categoria"] = df_aai["Categoria"].replace(dict_correcao_aai_categoria)

    # O código abaixo troca o nome de categorias com padrões repetidos
    df_map_padrao = carrega_e_limpa_bases_de_para("aai_categoria_recorrente")
    df_map_padrao.set_index("padrao", inplace=True)
    dict_map_padrao = df_map_padrao["categoria_nova"].to_dict()

    # Para categorias que possuem padrao similar a função abaixo ira renomear todas as obervações
    for padrao in dict_map_padrao:
        filtro = df_aai["Categoria"].str.contains(padrao)
        df_aai.loc[filtro, "Categoria"] = dict_map_padrao[padrao]

    df_aai = df_aai.drop(["Ativo"], axis=1)

    # Ajusta Produto_1 da categoria FUNDOS
    # Carrega base de para
    df_de_para_produto_1_fundos = carrega_e_limpa_bases_de_para("aai_produto_1_fundos")
    df_de_para_produto_1_fundos.set_index("tipo_receita", inplace=True)
    # Transforma base em um dicionario
    dict_de_para_produto_1_fundos = df_de_para_produto_1_fundos["produto_1"].to_dict()
    # Ajusta produto 1
    filtro = df_aai["Categoria"] == "FUNDOS"
    df_aai.loc[filtro, "Produto_1"] = df_aai["Tipo Receita"].replace(
        dict_de_para_produto_1_fundos
    )

    # Ajuste Produto_1 da categoria RENDA VARIÁVEL
    # Carrega base de para
    df_de_para_produto_1_rv = carrega_e_limpa_bases_de_para("aai_produto_1_rv")
    df_de_para_produto_1_rv.set_index("produto_1_antigo", inplace=True)
    # Transforma base em um dicionario
    dict_de_para_produto_1_rv = df_de_para_produto_1_rv["produto_1_novo"].to_dict()
    # Ajusta produto 1
    filtro = df_aai["Categoria"] == "RENDA VARIAVEL"
    df_aai.loc[filtro, "Produto_1"] = df_aai["Produto_1"].replace(
        dict_de_para_produto_1_rv
    )

    # Ajustes para a categoria RENDA VARIÁVEL
    # Carrega base de-para
    df_de_para_produto_1_rf = carrega_e_limpa_bases_de_para("aai_produto_1_rf")
    df_de_para_produto_1_rf.set_index("produto_1_antigo", inplace=True)
    # Transforma base em um dicionario
    dict_de_para_produto_1_rf = df_de_para_produto_1_rf["produto_1_novo"].to_dict()
    # Cria o Produto_1 auxiliar
    filtro = df_aai["Categoria"] == "RENDA FIXA"
    df_aai.loc[filtro, "Produto_1_aux"] = df_aai["Produto_1"].replace(
        dict_de_para_produto_1_rf
    )

    # Definindo o Produto_1_aux para casos específicos
    filtro = df_aai["Produto_2"].isin(["Fee Litoral Sul", "São Simão"])
    df_aai.loc[filtro, "Produto_1_aux"] = "DEBENTURE"
    filtro = df_aai["Produto_2"] == "MRL"
    df_aai.loc[filtro, "Produto_1_aux"] = "CRI"
    filtro = df_aai["Produto_2"] == "SECUNDARIO CETIPADO"
    df_aai.loc[filtro, "Produto_1_aux"] = "SECUNDARIO CETIPADO"

    # O Produto_2 para RENDA FIXA será o Produto_1_aux + Produto_2_aux, quando Produto_2_aux for ""
    # será apenas Produto_1_aux
    filtro = df_aai["Categoria"] == "RENDA FIXA"
    df_aai.loc[filtro, "Produto_2"] = (
        df_aai["Produto_1_aux"] + "-" + df_aai["Produto_2_aux"]
    )

    filtro = df_aai["Produto_2_aux"] == ""
    df_aai.loc[filtro, "Produto_2"] = df_aai["Produto_1_aux"]

    # Transforma o Produto_1 que será mapeado utilizando a base de-para
    # Carrega base de para
    df_de_para_produto_1_rf = carrega_e_limpa_bases_de_para("aai_produto_1_rf_2")
    df_de_para_produto_1_rf.set_index("produto_1_antigo", inplace=True)
    # Transforma base em um dicionario
    dict_de_para_produto_1_rf = df_de_para_produto_1_rf["produto_1_novo"].to_dict()
    # Ajusta Produto_1
    filtro = df_aai["Categoria"] == "RENDA FIXA"
    df_aai.loc[filtro, "Produto_1"] = df_aai["Produto_1"].replace(
        dict_de_para_produto_1_rf
    )

    # Em alguns casos o Produto_1 será mapeado de Tipo Receita
    # Carrega base de para
    df_de_para_produto_1_rf = carrega_e_limpa_bases_de_para("aai_produto_1_rf_3")
    df_de_para_produto_1_rf.set_index("tipo_receita", inplace=True)
    # Transforma base em um dicionario
    dict_de_para_produto_1_rf = df_de_para_produto_1_rf["produto_1"].to_dict()
    # Ajusta Produto_1
    filtro_1 = df_aai["Categoria"] == "RENDA FIXA"
    filtro_2 = df_aai["Produto_1"].isna()
    df_aai.loc[filtro_1 & filtro_2, "Produto_1"] = df_aai["Tipo Receita"].replace(
        dict_de_para_produto_1_rf
    )

    # Dropa as colunas que não serão mais necessárias
    df_aai = df_aai.drop(["Produto_1_aux", "Produto_2_aux", "Tipo Receita"], axis=1)
    df_aai = df_aai.reset_index(drop=True)

    return df_aai
