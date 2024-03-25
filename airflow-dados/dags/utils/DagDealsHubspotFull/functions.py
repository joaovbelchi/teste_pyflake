import pandas as pd
from datetime import datetime
from deltalake import DeltaTable, write_deltalake
from utils.bucket_names import get_bucket_name
from utils.DagDealsHubspotFull.schemas import SCHEMA_ID_RECEITA


def processa_tabela_id_receita_hubspot():
    """Realiza o processamento da tabela id_receita_hubspot
    que possui a relação, extraída dos deals do hubspot, das propriedades
    ID Receita, Proprietário do negócio e Pipeline. Esta tabela é utilizada
    no processamento da base de receita para fazer o direcionamento
    de receita para seus responsáveis com base nos deals que
    estão lançados no hubspot. O armazenamento desta tabela é feito em Delta
    na camada silver do DataLake.
    """

    def carrega_base_deals_hubspot() -> pd.DataFrame:
        """Carrega a tabela deals hubspot em sua última
        data de processamento selecionando as colunas e
        registros que são necessárias para o processamento
        da tabela id_receita_hubspot. Todos os registros
        que tiverem valores vazios na coluna id_receita serão
        desconsiderados durante o carregamento da base.

        Returns:
            pd.DataFrame: DataFrame pandas com a tabela
            deals hubspot pre processada.
        """

        today = datetime.now().strftime("%Y-%m-%d")
        bucket_name_bronze = get_bucket_name("lake-bronze")

        df_deals = pd.read_parquet(
            f"s3://{bucket_name_bronze}/api/hubspot/tb_deals/{today}",
            columns=["id_receita", "hubspot_owner_id", "pipeline"],
        ).dropna(subset="id_receita")
        return df_deals

    def carrega_base_socios() -> pd.DataFrame:
        """Carrega a tabela socios da camada gold
        do DataLake e realiza o pre processamento
        necessário para que esta tabela possa ser utilizada
        no processamento da tabela id_receita_hubspot

        Returns:
            pd.DataFrame: DataFrame pandas com a tabela sócios
            da camada gold do DataLake pre processada.
        """

        bucket_name_gold = get_bucket_name("lake-gold")
        df_socios = (
            pd.read_parquet(
                f"s3://{bucket_name_gold}/api/one/socios/",
                columns=["Nome_bd", "Id"],
            )
            .dropna(subset="Id")
            .drop_duplicates(subset="Nome_bd")
        )

        df_socios["Nome_bd"] = df_socios["Nome_bd"].str.lstrip().str.rstrip()

        return df_socios

    # Cria um dicionário com a chave sendo o nome orinal das colunas
    # e o valor o novo nome que será adotado
    rename_dict = {
        nome_original["nomes_origem"][0]: novo_nome
        for novo_nome, nome_original in SCHEMA_ID_RECEITA.items()
    }

    # Carrega as bases de dados
    df_socios = carrega_base_socios()
    df_delas = carrega_base_deals_hubspot()

    # Realiza o merge com a tabela de sócios para
    # que seja obtido o nome do responsável pela receita
    # a partir do hubspot_owner_id
    df_id_receita = (
        df_delas.merge(
            df_socios,
            left_on="hubspot_owner_id",
            right_on="Id",
            how="left",
            validate="m:1",
        )
        .drop(columns=["hubspot_owner_id", "Id"])
        .rename(columns=rename_dict)
        .reset_index(drop=True)
    )

    # Define os tipos de dados das colunas segundo schema
    for col_name in SCHEMA_ID_RECEITA.keys():
        df_id_receita[col_name] = df_id_receita[col_name].astype(
            SCHEMA_ID_RECEITA[col_name]["data_type"]
        )

    # Salva a tabela
    bucket_name_silver = get_bucket_name("lake-silver")
    write_deltalake(
        table_or_uri=f"s3://{bucket_name_silver}/hubspot/id_receita_hubspot/",
        data=df_id_receita,
        mode="overwrite",
    )

    dt_id_receita = DeltaTable(f"s3://{bucket_name_silver}/hubspot/id_receita_hubspot/")
    dt_id_receita.optimize.compact()
    print("Partição compactada")
    dt_id_receita.vacuum(retention_hours=4320,dry_run=False)
    print("Arquivos antigos apagados")
