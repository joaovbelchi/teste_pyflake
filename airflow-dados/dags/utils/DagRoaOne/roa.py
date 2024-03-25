import pandas as pd
from dateutil.parser import isoparse
from datetime import datetime
from dateutil.relativedelta import relativedelta

from utils.bucket_names import get_bucket_name
from utils.DagRoaOne.functions import (
    obter_particao_mais_recente,
    receita_estimada,
    base_para_roa,
)


def define_parametros_roa_realizado_ou_estimado(tipo: str, timestamp_dagrun: str):
    """Calcular o ROA de acordo com os parâmetros definidos

    Args:
        tipo (str): tipo da receita que será considerada (estimada ou realizada)
        timestamp_dagrun (str): Timestamp lógico de execução do pipeline.
    """

    bucket_lake_gold = get_bucket_name("lake-gold")

    if tipo == "estimado":
        # Ler, filtrar e padronizar colunas da receita estimada
        df_receita_completa = receita_estimada()

        # Definir parâmetros para calcular o ROA
        # Definir data máxima da receita estimada
        max_date = df_receita_completa["data"].max()

        data_particao_mais_recente = datetime.strptime(
            f"{max_date.year}-{max_date.month}", "%Y-%m"
        )

        # Definir meses para atualizar o ROA
        # A data máxima tem quer ser menor ou igual a hoje para considerar
        # o mês atual, por isso considero a data mínima do mês em questão
        datas_para_atualizacao = df_receita_completa.groupby(
            pd.Grouper(key="data", freq="M"), as_index=False
        )["data"].min()

        datas_para_atualizacao = datas_para_atualizacao["data"].to_list()

        # Definir colunas que serão usadas para agrupar
        group_cols = [
            pd.Grouper(key="data", freq="D"),
            "conta",
            "mercado",
            "comercial",
            "operacional_rf",
            "operacional_rv",
            "grupo_familiar",
            "nome",
        ]

        agg_cols = ["receita"]

        # Definir colunas que serão mantidas no dataframe final
        select_cols = [
            "conta",
            "mercado",
            "data",
            "comercial",
            "operacional_rf",
            "operacional_rv",
            "pl",
            "receita",
            "grupo_familiar",
            "nome",
            "ano_particao",
            "mes_particao",
            "indicador_auc_receita",
        ]

        # Definir path em que a base será salva
        path_save = f"s3a://{bucket_lake_gold}/one/receita/estimativa/roa"

    else:
        # Ler consolidado-histórico
        df_receita_completa = pd.read_parquet(
            f"s3://{bucket_lake_gold}/btg/receita/tb_consolidado_historico_gold",
        )

        # Verificação particao mais recente de receita
        data_particao_mais_recente = obter_particao_mais_recente(
            bucket_lake_gold, "btg/receita/tb_consolidado_historico_gold"
        )

        # Leitura últimos meses na auc_dia_dia gold e consolidado gold
        # Definir meses para atualizar o ROA
        ts_parsed = isoparse(timestamp_dagrun)
        ts_parsed_anterior = ts_parsed - relativedelta(months=1)
        datas_para_atualizacao = [ts_parsed_anterior, ts_parsed]

        # Definir colunas que serão usadas para agrupar
        group_cols = [
            pd.Grouper(key="data", freq="D"),
            "conta",
            "mercado",
            "comercial",
            "operacional_rf",
            "operacional_rv",
            "grupo_familiar",
            "nome",
            "id_gf",
            "origem_do_cliente",
            "id_conta",
            "contato_empresa",
            "origem_1",
            "origem_2",
            "origem_3",
            "origem_4",
            "negocio",
            "participacao",
            "departamento",
        ]

        agg_cols = ["receita_bruta", "receita_liquida", "receita_liquida_com_redutores"]

        # Definir colunas que serão mantidas no dataframe final
        select_cols = [
            "conta",
            "mercado",
            "data",
            "comercial",
            "operacional_rf",
            "operacional_rv",
            "pl",
            "receita_bruta",
            "receita_liquida",
            "receita_liquida_com_redutores",
            "grupo_familiar",
            "nome",
            "id_gf",
            "origem_do_cliente",
            "id_conta",
            "contato_empresa",
            "origem_1",
            "origem_2",
            "origem_3",
            "origem_4",
            "negocio",
            "participacao",
            "departamento",
            "ano_particao",
            "mes_particao",
            "indicador_auc_receita",
        ]
        # Definir path em que a base será salva
        path_save = f"s3a://{bucket_lake_gold}/btg/base_para_roa"

    # Calcular e salvar o ROA
    base_para_roa(
        datas_para_atualizacao,
        data_particao_mais_recente,
        df_receita_completa,
        group_cols,
        agg_cols,
        select_cols,
        path_save,
    )

    print(tipo, "concluído!")
