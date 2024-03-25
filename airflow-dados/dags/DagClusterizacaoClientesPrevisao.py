from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import (
    S3CopyObjectOperator,
    S3DeleteObjectsOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime
from utils.msteams import msteams_task_failed
from utils.bucket_names import get_bucket_name
from utils.DagClusterizacaoClientesPrevisao.functions import leitura_base_b3_landing
from utils.functions import (
    verifica_qualidade_landing,
    transferencia_landing_para_bronze,
)

from utils.DagClusterizacaoClientesPrevisao.functions import (
    bronze_2_silver,
    trigger_glue,
    processing_aa,
    predict_movimentacoes,
    predict_nnm,
    predict_aa,
)
from utils.DagClusterizacaoClientesPrevisao.schemas import (
    SCHEMA_B3_FII,
    SCHEMA_B3_FIP,
    SCHEMA_B3_ETF,
)
from utils.datasets import (
    TB_B3_FII_LANDING,
    TB_B3_FIP_LANDING,
    TB_B3_ETF_LANDING,
    TB_B3_FII_BRONZE,
    TB_B3_FIP_BRONZE,
    TB_B3_ETF_BRONZE,
    TB_B3_FII_SILVER,
    TB_B3_FIP_SILVER,
    TB_B3_ETF_SILVER,
    ASSET_ALLOCATION_PROCESSADA,
    MOVIMENTACAO_PROCESSADA,
    NNM_PROCESSADA,
    POSICAO_PROCESSADA,
    PREVISAO_ASSET_ALLOCATION,
    PREVISAO_MOVIMENTACAO,
    PREVISAO_NNM,
)


# Função de qualidade de dados
def qualidade_task_failed(context):
    msteams_task_failed(context, Variable.get("msteams_webhook_qualidade"))


# Define variáveis gerais
ano_particao = "{{ data_interval_end.year }}"
mes_particao = "{{ data_interval_end.month }}"
data_interval_end = "{{ data_interval_end.strftime('%Y%m%dT%H%M%S') }}"

# Define argumentos padrões
default_args = {
    "owner": "Eduardo Queiroz",
    "email": ["eduardo.queiroz@investimentos.one"],
    "on_failure_callback": msteams_task_failed,
    "start_date": datetime(2023, 11, 20),
    "retries": 0,
}

# Define os parâmetros que serão necessários para cada uma das bases
table_params = [
    {
        "s3_table_name": "fii",
        "extension": ".csv",
        "func_leitura_landing": leitura_base_b3_landing,
        "schema": SCHEMA_B3_FII,
        "datasets_list_landing": [TB_B3_FII_LANDING],
        "datasets_list_bronze": [TB_B3_FII_BRONZE],
        "datasets_list_silver": [TB_B3_FII_SILVER],
        "source_bucket_key": "api/one/b3/upload_manual/",
        "dest_bucket_key": "api/one/b3/fundos_listados/",
    },
    {
        "s3_table_name": "fip",
        "extension": ".csv",
        "func_leitura_landing": leitura_base_b3_landing,
        "schema": SCHEMA_B3_FIP,
        "datasets_list_landing": [TB_B3_FIP_LANDING],
        "datasets_list_bronze": [TB_B3_FIP_BRONZE],
        "datasets_list_silver": [TB_B3_FIP_SILVER],
        "source_bucket_key": "api/one/b3/upload_manual/",
        "dest_bucket_key": "api/one/b3/fundos_listados/",
    },
    {
        "s3_table_name": "etf",
        "extension": ".csv",
        "func_leitura_landing": leitura_base_b3_landing,
        "schema": SCHEMA_B3_ETF,
        "datasets_list_landing": [TB_B3_ETF_LANDING],
        "datasets_list_bronze": [TB_B3_ETF_BRONZE],
        "datasets_list_silver": [TB_B3_ETF_SILVER],
        "source_bucket_key": "api/one/b3/upload_manual/",
        "dest_bucket_key": "api/one/b3/fundos_listados/",
    },
]


with DAG(
    "dag_clusterizacao_clientes_previsao",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    task_sesor_s3_files = S3KeySensor(
        task_id="sensor_arquivos_baixados_b3",
        bucket_name=get_bucket_name("lake-landing"),
        bucket_key=[
            "api/one/b3/upload_manual/fii.csv",
            "api/one/b3/upload_manual/etf.csv",
            "api/one/b3/upload_manual/fip.csv",
        ],
        timeout=3600,
    )

    fundos_listados = []
    for params in table_params:
        # Cria alguma variáveis que serão utilizadas pelas tasks dentro deste loop
        landing_obj_filename = (
            f"{params['s3_table_name']}_{data_interval_end}{params['extension']}"
        )
        landing_obj_partition_key = (
            f"ano_particao={ano_particao}/mes_particao={mes_particao}"
        )
        source_bucket_key = f"{params['source_bucket_key']}{params['s3_table_name']}{params['extension']}"
        dest_bucket_key = f"{params['dest_bucket_key']}{params['s3_table_name']}/{landing_obj_partition_key}/{landing_obj_filename}"
        bucket_name_landing = get_bucket_name("lake-landing")
        bucket_name_bronze = get_bucket_name("lake-bronze")
        bucket_name_silver = get_bucket_name("lake-silver")

        # Início das tasks
        task_copiar_arquivo = S3CopyObjectOperator(
            task_id=f"copia_{params['s3_table_name']}_landing",
            source_bucket_name=bucket_name_landing,
            dest_bucket_name=bucket_name_landing,
            source_bucket_key=source_bucket_key,
            dest_bucket_key=dest_bucket_key,
            outlets=params["datasets_list_landing"],
        )

        task_deletar_arquivo = S3DeleteObjectsOperator(
            task_id=f"deleta_{params['s3_table_name']}_upload_manual",
            bucket=bucket_name_landing,
            keys=source_bucket_key,
        )

        task_verifica_qualidade_landing = PythonOperator(
            task_id=f"qualidade_landing_{params['s3_table_name']}",
            python_callable=verifica_qualidade_landing,
            op_kwargs={
                "s3_bucket": bucket_name_landing,
                "s3_table_key": f"{params['dest_bucket_key']}{params['s3_table_name']}",
                "s3_partitions_key": landing_obj_partition_key,
                "s3_filename": landing_obj_filename,
                "schema": params["schema"],
                "webhook": "{{ var.value.get('msteams_webhook_qualidade') }}",
                "funcao_leitura_landing": params["func_leitura_landing"],
                "kwargs_funcao_leitura_landing": {
                    "s3_path": f"s3://{bucket_name_landing}/" + dest_bucket_key
                },
            },
            on_failure_callback=qualidade_task_failed,
        )

        task_move_para_bronze = PythonOperator(
            task_id=f"transfer_{params['s3_table_name']}_bronze",
            python_callable=transferencia_landing_para_bronze,
            op_kwargs={
                "s3_bucket_bronze": bucket_name_bronze,
                "s3_table_key": f"{params['dest_bucket_key']}{params['s3_table_name']}",
                "s3_partitions_key": landing_obj_partition_key,
                "schema": params["schema"],
                "data_interval_end": data_interval_end,
                "funcao_leitura_landing": params["func_leitura_landing"],
                "kwargs_funcao_leitura_landing": {
                    "s3_path": f"s3://{bucket_name_landing}/" + dest_bucket_key,
                },
            },
            outlets=params["datasets_list_bronze"],
        )

        task_move_para_silver = PythonOperator(
            task_id=f"transfer_{params['s3_table_name']}_silver",
            python_callable=bronze_2_silver,
            op_kwargs={
                "path_bronze_s3": f"s3://{bucket_name_bronze}/"
                + params["dest_bucket_key"]
                + params["s3_table_name"],
                "path_silver_s3": f"s3://{bucket_name_silver}/"
                + params["dest_bucket_key"]
                + params["s3_table_name"]
                + params["extension"],
            },
            outlets=params["datasets_list_silver"],
        )

        (
            task_sesor_s3_files
            >> task_copiar_arquivo
            >> task_deletar_arquivo
            >> task_verifica_qualidade_landing
            >> task_move_para_bronze
            >> task_move_para_silver
        )

        fundos_listados.append(task_move_para_silver)

    task_processa_nnm = PythonOperator(
        task_id="processa_nnm_glue",
        python_callable=trigger_glue,
        op_kwargs={
            "date_interval_end": data_interval_end,
            "glue_job": "processing_NNM",
        },
    )

    task_processa_mov = PythonOperator(
        task_id="processa_mov_glue",
        python_callable=trigger_glue,
        op_kwargs={
            "date_interval_end": data_interval_end,
            "glue_job": "processing_Mov",
        },
    )

    task_processa_posicao = PythonOperator(
        task_id="processa_posicao_glue",
        python_callable=trigger_glue,
        op_kwargs={
            "date_interval_end": data_interval_end,
            "glue_job": "processing_Posicao",
        },
    )

    task_processa_aa = PythonOperator(
        task_id="processa_aa",
        python_callable=processing_aa,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
        outlets=[ASSET_ALLOCATION_PROCESSADA],
    )

    sensor_nnm_glue = GlueJobSensor(
        task_id="sensor_nnm_glue",
        job_name="processing_NNM",
        run_id="{{ ti.xcom_pull(key='processing_NNM_run_id', task_ids='processa_nnm_glue') }}",
        aws_conn_id="my_aws",
    )

    dataset_nnm_glue = EmptyOperator(
        task_id="dataset_nnm_glue", outlets=[NNM_PROCESSADA]
    )

    sensor_mov_glue = GlueJobSensor(
        task_id="sensor_mov_glue",
        job_name="processing_Mov",
        run_id="{{ ti.xcom_pull(key='processing_Mov_run_id', task_ids='processa_mov_glue') }}",
        aws_conn_id="my_aws",
    )

    dataset_mov_glue = EmptyOperator(
        task_id="dataset_mov_glue", outlets=[MOVIMENTACAO_PROCESSADA]
    )

    sensor_posicao_glue = GlueJobSensor(
        task_id="sensor_posicao_glue",
        job_name="processing_Posicao",
        run_id="{{ ti.xcom_pull(key='processing_Posicao_run_id', task_ids='processa_posicao_glue') }}",
        aws_conn_id="my_aws",
    )

    dataset_posicao_glue = EmptyOperator(
        task_id="dataset_posicao_glue", outlets=[POSICAO_PROCESSADA]
    )

    task_predict_nnm = PythonOperator(
        task_id="predict_nnm",
        python_callable=predict_nnm,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
        outlets=[PREVISAO_NNM],
    )

    task_predict_mov = PythonOperator(
        task_id="predict_mov",
        python_callable=predict_movimentacoes,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
        outlets=[PREVISAO_MOVIMENTACAO],
    )

    task_predict_aa = PythonOperator(
        task_id="predict_aa",
        python_callable=predict_aa,
        op_kwargs={
            "date_interval_end": data_interval_end,
        },
        outlets=[PREVISAO_ASSET_ALLOCATION],
    )

    task_processa_nnm >> sensor_nnm_glue >> dataset_nnm_glue >> task_predict_nnm
    task_processa_mov >> sensor_mov_glue >> dataset_mov_glue >> task_predict_mov
    (
        fundos_listados
        >> task_processa_posicao
        >> sensor_posicao_glue
        >> dataset_posicao_glue
    )
    task_processa_aa
    [dataset_posicao_glue, task_processa_aa] >> task_predict_aa
