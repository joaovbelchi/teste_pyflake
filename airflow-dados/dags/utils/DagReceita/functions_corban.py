import pandas as pd
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import hashlib
from utils.secrets_manager import get_secret_value
from deltalake import DeltaTable, write_deltalake
from utils.DagReceita.functions_aai import carrega_e_limpa_bases_de_para
from utils.OneGraph.mail import Mail
import json
from utils.secrets_manager import get_secret_value
from utils.bucket_names import get_bucket_name


def leitura_corban_landing(path: str) -> pd.DataFrame:
    """Função específica para leitura da base corban salva em
    xlsx. Esta função realiza a leitura das abas 'Detalhe'
    presente no arquivo excel.

    Args:
        path (str): URI do objeto salvo em xlsx no S3.

    Returns:
        pd.DataFrame: Base de dados em um DataFrame pandas.
    """

    df = pd.read_excel(path, sheet_name="Detalhe", skiprows=4)

    df = df.dropna(subset="Conta")

    df = df.reset_index(drop=True)

    return df


def correct_date(df: pd.DataFrame, mes_corrente: str, coluna_data: str) -> pd.DataFrame:
    """
    Corrige as datas da coluna especificada (coluna_data) no DataFrame (df)
    para o primeiro dia útil do mês corrente (mes_corrente).
    Se alguma data na coluna_data pertencer a um mês diferente do mes_corrente,
    ela será ajustada para o primeiro dia útil desse mês.

    Args:
        df (pd.DataFrame): DataFrame que requer correção.
        mes_corrente (str): Mês atual para a correção.
        coluna_data (str): Nome da coluna contendo as datas na base de dados.

    Returns:
        pd.DataFrame: DataFrame com a coluna de datas corrigida.
    """

    df[coluna_data] = pd.to_datetime(df[coluna_data], format="%d/%m/%Y")

    df["ano"] = df[coluna_data].dt.year.astype("str")
    df["mes"] = df[coluna_data].dt.month.astype("str")
    df["dia"] = df[coluna_data].dt.day.astype("str")

    filtro = df["mes"] != str(mes_corrente)

    df.loc[filtro, coluna_data] = pd.to_datetime(
        df["ano"] + "-" + str(mes_corrente) + "-" + "01"
    )
    df.drop(columns=["ano", "dia", "mes"], inplace=True)

    return df


def reduce_number(number: int) -> str:
    """Função que cria um número de forma
    consistente para contas do btg empresa.

    Args:
        number (int): Número que servirá de referência

    Returns:
        str: string com número da conta btg empresa
    """
    hashed = hashlib.sha256(str(number).encode()).hexdigest()
    reduced = int(hashed, 16) % 100000
    return f"{reduced:05}"


def carrega_e_limpa_base_corban(df_corban, semanal=False) -> pd.DataFrame:
    """Esta função carrega e limpa a base Corban

    Args:
        path (str): Caminho para a base Corban.
        query_operacoes (str): Query para consulta à base Operações de Cambio
        df (pd.DataFrame, optional): Data Frame com a base Corban. Defaults to None.

    Returns:
        pd.DataFrame: Data Frame com a base Corban limpa.
    """

    if semanal is False:
        df_corban = df_corban.reset_index(drop=True)

        # Seleciona as colunas que serão utilizadas
        df_corban = df_corban.loc[
            :,
            [
                "Conta",
                "Categoria",
                "Comissão",
                "Tipo Receita",
                "Código/CNPJ",
                "Data Receita",
                "Ativo",
                "Receita Bruta",
                "ano_particao",
                "mes_particao",
                "atualizacao_incremental",
            ],
        ]

        # Atualiza o tipo das colunas
        column_type = {
            "Conta": "str",
            "Categoria": "str",
            "Comissão": "float",
            "Tipo Receita": "str",
            "Código/CNPJ": "str",
            "Ativo": "str",
            "Receita Bruta": "float",
            "Data Receita": "str",
        }
        df_corban = df_corban.astype(column_type)
        df_corban["Data Receita"] = pd.to_datetime(df_corban["Data Receita"])

        # Renomeia as colunas
        novo_nome_colunas = {
            "Tipo Receita": "Produto_1",
            "Código/CNPJ": "Produto_2",
            "Data Receita": "Data",
            "Receita Bruta": "Receita_bruta",
        }
        df_corban.rename(columns=novo_nome_colunas, inplace=True)

    else:
        df_corban = df_corban.loc[
            :,
            [
                "Conta",
                "Categoria",
                "Comissão",
                "Tipo Receita",
                "Código/CNPJ",
                "Data Receita",
                "Ativo",
                "Receita Bruta",
                "ano_particao",
                "mes_particao",
                "atualizacao_incremental",
            ],
        ]
        column_type = {
            "Conta": "str",
            "Categoria": "str",
            "Comissão": "float",
            "Tipo Receita": "str",
            "Código/CNPJ": "str",
            "Ativo": "str",
            "Receita Bruta": "float",
        }
        df_corban = df_corban.astype(column_type)
        # Ajusta a coluna data, o procedimento abaixo é necessário para que a data fique correta.

        novo_nome_colunas = {
            "Tipo Receita": "Produto_1",
            "Código/CNPJ": "Produto_2",
            "Data Receita": "Data",
            "Receita Bruta": "Receita_bruta",
        }
        # Renomeia colunas
        df_corban.rename(columns=novo_nome_colunas, inplace=True)
        df_corban["Data"] = pd.to_datetime(df_corban["Data"])

    # Renomeia colunas
    df_corban.rename(columns=novo_nome_colunas, inplace=True)

    # Tratamentos do código antigo.
    df_corban["Produto_1"] = df_corban["Produto_1"].fillna(df_corban["Produto_2"])
    df_corban["Categoria"] = df_corban["Categoria"].str.replace(
        "CRÉDITO ESTRUTURADO", "CREDITO ESTRUTURADO"
    )

    type1 = ["CÂMBIO", "ENCARGOS", "ERRO CÂMBIO", "CAMBIO"]

    type2 = [
        "CREDITO ESTRUTURADO",
        "CRÉDITO ESTRUTURADO",
        "AJUSTE COMISSAO CORBAN",
        "CREDITO",
    ]  # Credito: Busca categoria pelo ativo, tem condição especial para CCB

    type3 = [
        "FEE TRANSACIONAL",
        "FEE TRANSACIONAL PME",
    ]  # Banking: Busca categoria pelo ativo

    type4 = ["CONTA VIRADA"]

    type5 = ["AJUSTE"]

    ## Separação por tipo de receita dentro de CORBAN (NOME DOS TIPOS ALTERADOS)
    df_corban["Tipo"] = np.where(
        df_corban["Categoria"].astype(str).isin(type1), "CAMBIO", np.nan
    )
    df_corban["Tipo"] = np.where(
        df_corban["Categoria"].astype(str).isin(type2), "CREDITO", df_corban["Tipo"]
    )
    df_corban["Tipo"] = np.where(
        df_corban["Categoria"].astype(str).isin(type5), "AJUSTE", df_corban["Tipo"]
    )
    df_corban["Tipo"] = np.where(
        df_corban["Categoria"].astype(str).str.contains("|".join(["CRÉDITO"])),
        "CREDITO",
        df_corban["Tipo"],
    )
    df_corban["Tipo"] = np.where(
        df_corban["Categoria"].astype(str).str.contains("|".join(["CRI"])),
        "MERCADO DE CAPITAIS",
        df_corban["Tipo"],
    )
    df_corban["Tipo"] = np.where(
        df_corban["Categoria"].astype(str).isin(type3), "BANKING", df_corban["Tipo"]
    )
    df_corban["Tipo"] = np.where(
        df_corban["Categoria"].astype(str).isin(type4), "CREDITO", df_corban["Tipo"]
    )
    df_corban = df_corban.replace("CÂMBIO", "CAMBIO")

    # DE-PARA Ativos
    map_ativos = carrega_e_limpa_bases_de_para("corban_ativo")
    dictionary = map_ativos.set_index(map_ativos.columns[0]).to_dict()[
        map_ativos.columns[1]
    ]
    df_corban["Ativo"] = df_corban["Ativo"].replace(dictionary)

    # tudo que é BANKING pode pegar a categoria pela coluna Ativo
    df_corban.loc[(df_corban["Tipo"] == "BANKING"), "Categoria"] = df_corban["Ativo"]

    # As linhas que são do tipo CREDITO e não são CONTA VIRADA, podem ter a categoria alterada para o nome do ativo
    df_corban.loc[
        (df_corban["Tipo"] == "CREDITO") & (df_corban["Categoria"] != "CONTA VIRADA"),
        "Categoria",
    ] = df_corban["Ativo"]

    padrao = r"^(?!.*CCBR).*CCB.*$"
    df_corban.loc[
        (df_corban["Tipo"] == "CREDITO")
        & (df_corban["Categoria"] != "CONTA VIRADA")
        & (df_corban["Produto_2"]).str.contains(padrao, regex=True),
        "Categoria",
    ] = "CCB"

    # DE-PARA categorias, email FRANCISCO  -- para dados recentes, não fizemos o de para de dados mais antigos
    map_categoria = carrega_e_limpa_bases_de_para("corban_categoria")
    dictionary = map_categoria.set_index(map_categoria.columns[0]).to_dict()[
        map_categoria.columns[1]
    ]
    df_corban["Categoria"] = df_corban["Categoria"].replace(dictionary)

    # DE-PARA PRODUTO_2, email FRANCISCO -- para dados recentes, nao fizemos o de para de dados mais antigos
    map_produto_2 = carrega_e_limpa_bases_de_para("corban_produto_2")
    dictionary = map_produto_2.set_index(map_produto_2.columns[0]).to_dict()[
        map_produto_2.columns[1]
    ]
    df_corban["Produto_2"] = df_corban["Produto_2"].replace(dictionary)

    filtro = df_corban["Categoria"] == "CAMBIO"
    df_corban["Categoria"] = df_corban["Categoria"].fillna("NAO IDENTIFICADO")

    df_corban = df_corban.drop(["Receita_bruta"], axis=1)

    df_corban["Intermediador"] = "BTG PACTUAL"
    df_corban["Fornecedor"] = "BTG PACTUAL"
    df_corban["Financeira"] = "BTG PACTUAL"

    ajuste_linha_credito = df_corban[
        (df_corban["Conta"] == "4009320") & (df_corban["Categoria"] == "IDENTIFICAR")
    ]
    df_corban.loc[ajuste_linha_credito.index, "Tipo"] = "CREDITO"
    df_corban.loc[ajuste_linha_credito.index, "Categoria"] = "RISCO SACADO"

    # Renomeia categorias de cambio que vieram da base operações.
    filtro = df_corban["Categoria"] == "COMPRA"
    df_corban.loc[filtro, "Categoria"] = "RECEBIMENTO"

    filtro = df_corban["Categoria"] == "VENDA"
    df_corban.loc[filtro, "Categoria"] = "ENVIO"

    df_corban.drop(columns=["Ativo"], inplace=True)

    # Este passo foi uma solicitação feita pelo Francisco para correção
    # da receita que veio de forma errada no relatório do BTG. Este trecho de
    # código deverá ser mantido para que a correção seja aplicada.

    filtro = (
        (df_corban["Conta"] == "1580384")
        & (df_corban["Data"] == "2022-09-30")
        & (df_corban["Produto_2"] == "FI255-22")
    )

    df_corban.loc[filtro, "Conta"] = "4041903"

    return df_corban


def corban_bronze_2_silver(
    ano_particao: str,
    mes_particao: str,
    s3_silver_step_1_path: str,
    s3_bronze_path: str,
):
    """Realiza o pre processamento da base corban da camada bronze para a camada
    silver

    Args:
        ano_particao (str): ano de processamento utilizado para particionar a base
        mes_particao (str): mes de processamento utilizado para particionar a base
        s3_silver_step_1_path (str): path para a base pre processada na camada silver
        s3_bronze_path (str): path para a base salva na camada bronze
        schema (dict): schema da tabela que será salva
    """
    # Realiza o leitura do DataFrame na camada bronze e seleciona apenas a data
    # mais recente
    df = DeltaTable(s3_bronze_path).to_pandas(
        partitions=[
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ]
    )

    df = df[df["timestamp_escrita_bronze"] == df["timestamp_escrita_bronze"].max()]

    df = df.drop(
        columns=["Código Assessor", "timestamp_dagrun", "timestamp_escrita_bronze"]
    )

    df["Conta"] = df["Conta"].str.split(".").str[0]

    data_processamento = datetime(ano_particao, mes_particao, 1)
    data_receita = data_processamento - relativedelta(months=1)

    df = correct_date(df, str(data_receita.month), "Data Receita")

    contas_btg_empresas = df[df["Conta"] == "9300"][
        ["Assessor Principal", "Código/CNPJ"]
    ]

    # Dividindo a coluna "Código/CNPJ" em duas colunas
    contas_btg_empresas[["CNPJ", "Cliente"]] = contas_btg_empresas[
        "Código/CNPJ"
    ].str.split(pat="|", n=1, expand=True)

    # Removendo espaços em branco extras
    contas_btg_empresas["CNPJ"] = contas_btg_empresas["CNPJ"].str.strip()
    contas_btg_empresas["Cliente"] = contas_btg_empresas["Cliente"].str.strip()

    # Aplicar a função à coluna e criar uma nova coluna com os valores numéricos
    contas_btg_empresas["Conta"] = "BTGE" + contas_btg_empresas["CNPJ"].apply(
        reduce_number
    )

    contas_btg_empresas = (
        contas_btg_empresas.loc[:, ["Código/CNPJ", "Conta"]]
        .drop_duplicates("Conta")
        .reset_index(drop=True)
    )

    # Obtenha as contas únicas do DataFrame contas_btg_empresas
    contas_unicas = contas_btg_empresas["Código/CNPJ"].unique()

    # Crie um filtro para cada conta única e combine-os usando | (ou lógico)
    filtros = [df["Código/CNPJ"].str.contains(conta) for conta in contas_unicas]
    filtro_final = pd.concat(filtros, axis=1).any(axis=1)

    # Aplique o filtro no DataFrame receita_mes
    resultado = df.loc[filtro_final, :]
    resultado = resultado.drop("Conta", axis=1)

    receita_mes_final = df.drop(resultado.index)

    resultado = resultado.merge(contas_btg_empresas, on="Código/CNPJ", how="left")
    final_result_receita_mes = pd.concat([resultado, receita_mes_final], axis=0)

    final_result_receita_mes["atualizacao_incremental"] = "Sim"
    final_result_receita_mes["Comissão bruta"] = pd.NA
    final_result_receita_mes["Comissão bruta"] = final_result_receita_mes[
        "Comissão bruta"
    ].astype("double[pyarrow]")

    final_result_receita_mes = final_result_receita_mes.loc[
        :,
        [
            "Data Receita",
            "Conta",
            "Cliente",
            "Assessor Principal",
            "Categoria",
            "Produto",
            "Ativo",
            "Código/CNPJ",
            "Tipo Receita",
            "Receita Bruta",
            "Receita Líquida",
            "Comissão bruta",
            "Comissão",
            "ano_particao",
            "mes_particao",
            "atualizacao_incremental",
        ],
    ]
    final_result_receita_mes["ano_particao"] = final_result_receita_mes[
        "ano_particao"
    ].astype("int64[pyarrow]")
    final_result_receita_mes["mes_particao"] = final_result_receita_mes[
        "mes_particao"
    ].astype("int64[pyarrow]")
    final_result_receita_mes = final_result_receita_mes.reset_index(drop=True)

    write_deltalake(
        s3_silver_step_1_path,
        final_result_receita_mes,
        partition_filters=[
            ("atualizacao_incremental", "=", "Sim"),
            ("ano_particao", "=", str(ano_particao)),
            ("mes_particao", "=", str(mes_particao)),
        ],
        mode="overwrite",
    )


def envio_email_btg_empresas(
    ano_particao: int, mes_particao: int, atualizacao: str, s3_silver_step_1_path: str
):
    """Esta função verifica se existe alguma contra do tipo BTGE
    que foi adicionada na base Corban e que não está cadastrada no
    Hubspot. Caso existam contas que atendam a esta condição um
    e-mail é enviado para a o backoffice solicitando o cadastramento
    desta conta.

        Args:
            ano_particao (int): ano particao da base corban pre processada
            mes_particao (int): mes particao da base corban pre processada
            atualizacao (str): indica se é uma atualização incremental ou
            histórica. Este parâmetro irá direcionar a forma com a base
            corban será carregada, caso seja incremental será carregada
            apenas a partição indicado nas variáveis ano_particao e mes_particao
            caso seja histórica será carregada a base por completo.
            s3_silver_step_1_path: path para a base corban pre processada.

    """

    bucket_silver = get_bucket_name("lake-silver")

    secret_dict = json.loads(
        get_secret_value("apis/microsoft_graph", append_prefix=True)
    )

    # Carrega a base Corban pre processada da camada silver e
    # seleciona apenas as contas que são do BTG Empresas.

    if atualizacao == "incremental":
        df = DeltaTable(s3_silver_step_1_path).to_pandas(
            partitions=[
                ("atualizacao_incremental", "=", "Sim"),
                ("ano_particao", "=", str(ano_particao)),
                ("mes_particao", "=", str(mes_particao)),
            ]
        )

    elif atualizacao == "histórica":
        df = DeltaTable(s3_silver_step_1_path).to_pandas()

    filtro_contas_btge = df["Conta"].str.contains("BTGE")
    df = df.loc[filtro_contas_btge, :].copy()

    # Carrega e seleciona da base clientes assessores apenas as contas que são
    # do BTG Empresas e que estão presentes na data mais atual da base
    # clientes_assessores.
    df_clientes_assessores = pd.read_parquet(
        f"s3://{bucket_silver}/api/hubspot/clientesassessores/",
        columns=["Conta", "REGISTRO"],
    )
    data_max_clientes_assessores = df_clientes_assessores["REGISTRO"].max()
    filtro_data_max = df_clientes_assessores["REGISTRO"] == data_max_clientes_assessores
    filtro_contas_btge = df_clientes_assessores["Conta"].str.contains("BTGE")

    contas_banking = df_clientes_assessores.loc[
        filtro_data_max & filtro_contas_btge, "Conta"
    ].unique()

    # Verifica na base Corban quais contas do BTG Empresas ainda não foram criadas
    # e gera um e-mail solicitando a criação destas contas no hubspot
    filtro_contas_btge_novas = ~df["Conta"].isin(contas_banking)
    df_contas_btge_novas = (
        df.loc[filtro_contas_btge_novas, ["Assessor Principal", "Cliente", "Conta"]]
        .drop_duplicates("Cliente")
        .reset_index(drop=True)
        .copy()
    )

    if not df_contas_btge_novas.empty:
        email = Mail(
            client_id=secret_dict["client_id"],
            client_secret=secret_dict["client_secret"],
            tenant_id=secret_dict["tenant_id"],
            subject="[Solicitação] Criação de contas - BTG Empresas",
            recipients=[
                "eduardo.queiroz@investimentos.one",
                "guilherme.isaac@investimentos.one",
                "back.office@investimentos.one",
            ],
            sender_id="d64bcd7f-c47a-4343-b0b4-73f4a8e927a5",  # ID do e-mail tech@investimentos.one
        )

        email.paragraph(
            """<p>Bom dia!</p>
        <p>Favor criar as seguintes contas no Hubspot que correspondem ao BTG Empresas.</p>
        <p><strong>Passo a passo:</strong></p>
        <ol>
            <li>Verifique se as contas existem no Hubspot j&aacute;, procurando pelo nome da empresa</li>
            <li>Caso n&atilde;o exista, crie uma nova empresa</li>
            <li>Na cria&ccedil;&atilde;o da Conta, basta selecionar a Localiza&ccedil;&atilde;o: <strong>&quot;BTG Empresas&quot;</strong></li>
            <li>A conta do BTG empresas sempre ser&aacute; no modelo:<strong>&nbsp;&quot;BTGE&quot;+ 5 n&uacute;meros</strong></li>
            <li>Colocar o comercial informado na tabela abaixo, caso o assessor na tabela n&atilde;o seja comercial, colocar no Hubspot como One Investimento</p></li>
        </ol>"""
        )

        email.table(df_contas_btge_novas)

        email.send_mail()

    else:
        print("Não existem contas BTGE novas")
