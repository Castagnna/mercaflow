from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.slack.notifications.slack import send_slack_notification
from operators.dataproc.create import CreateClusterOperator
from operators.dataproc.submit import SubmitPySparkJobOperator


DAG_NAME = "MainFlow"
# Todo dia 1 ao meio dia
SCHEDULE_INTERVAL = "0 12 1 * *"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 12, 31),
    "email": ["dados@mercafacil.com.br"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    DAG_NAME,
    schedule_interval=SCHEDULE_INTERVAL,
    max_active_runs=1,
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_failure_callback=[
        send_slack_notification(
            text="The DAG {{ dag.dag_id }} failed",
            channel="#dados",
            username="Airflow",
        )
    ],
    tags=["dataproc"],
)

env = Variable.get("environment")

with dag:

    create_cluster = CreateClusterOperator(
        task_id="create_cluster",
        cluster_name="mainflow",
        master_machine_type="n2-highmem-8",
        worker_machine_type="n2-highmem-8",
        num_workers=4,
    )

    prev_execution_date = "{{ prev_execution_date.strftime('%Y%m%d-%H%M%S') }}"

    bronze_vendas = SubmitPySparkJobOperator(
        "bronze_vendas",
        "mainflow",
        "bronze",
        "Vendas",
        env,
        dttm=prev_execution_date,  # passa como arg o dia 1 do mês anterior, que já está fechado
    )

    bronze_categorias = SubmitPySparkJobOperator(
        "bronze_categorias",
        "mainflow",
        "bronze",
        "Categorias",
        env,
    )

    bronze_clientes = SubmitPySparkJobOperator(
        "bronze_clientes",
        "mainflow",
        "bronze",
        "Clientes",
        env,
    )

    bronze_produtos = SubmitPySparkJobOperator(
        "bronze_produtos",
        "mainflow",
        "bronze",
        "Produtos",
        env,
    )

    silver_deduplica_vendas = SubmitPySparkJobOperator(
        "silver_deduplica_vendas",
        "mainflow",
        "silver",
        "DeduplicaVendas",
        env,
    )

    silver_produtos_por_cliente = SubmitPySparkJobOperator(
        "silver_produtos_por_cliente",
        "mainflow",
        "silver",
        "ProdutosPorCliente",
        env,
    )

    silver_vendas_por_produto = SubmitPySparkJobOperator(
        "silver_vendas_por_produto",
        "mainflow",
        "silver",
        "VendasPorProduto",
        env,
    )

    with TaskGroup(group_id="gold_metrica_vendas") as gold_metrica_vendas:

        gold_metrica_vendas = SubmitPySparkJobOperator(
            "gold_metrica_vendas",
            "mainflow",
            "gold",
            "MetricaVendas",
            env,
        )

        drop_gold_metrica_vendas = BigQueryDeleteTableOperator(
            task_id="drop_gold_metrica_vendas",
            deletion_dataset_table="mercadata.gold.metrica_vendas",
            ignore_if_missing=True,
        )

        create_gold_metrica_vendas = BigQueryCreateExternalTableOperator(
            task_id="create_gold_metrica_vendas",
            table_resource={
                "tableReference": {
                    "projectId": "mercadata",
                    "datasetId": "gold",
                    "tableId": "metrica_vendas",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        "gs://mercafacil/data/prd/gold/metricas_de_vendas_por_produto"
                    ],
                    "autodetect": True,
                },
            },
        )

        [gold_metrica_vendas >> drop_gold_metrica_vendas >> create_gold_metrica_vendas]

    with TaskGroup(group_id="gold_upsell_categorias") as gold_upsell_categorias:

        gold_upsell_categorias = SubmitPySparkJobOperator(
            "gold_upsell_categorias",
            "mainflow",
            "gold",
            "UpSellCategoria",
            env,
        )

        drop_gold_upsell_categorias = BigQueryDeleteTableOperator(
            task_id="drop_gold_upsell_categorias",
            deletion_dataset_table="mercadata.gold.upsell_categorias",
            ignore_if_missing=True,
        )

        create_gold_upsell_categorias = BigQueryCreateExternalTableOperator(
            task_id="create_gold_upsell_categorias",
            table_resource={
                "tableReference": {
                    "projectId": "mercadata",
                    "datasetId": "gold",
                    "tableId": "upsell_categorias",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        "gs://mercafacil/data/prd/gold/top_5_produtos_para_o_cliente"
                    ],
                    "autodetect": True,
                },
            },
        )

        [
            gold_upsell_categorias
            >> drop_gold_upsell_categorias
            >> create_gold_upsell_categorias
        ]

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name="mainflow",
        region="southamerica-east1",
    )

    [
        create_cluster
        >> [bronze_vendas, bronze_categorias, bronze_clientes, bronze_produtos]
    ]

    (
        bronze_vendas
        >> silver_deduplica_vendas
        >> [silver_produtos_por_cliente, silver_vendas_por_produto]
    )

    [bronze_produtos, silver_vendas_por_produto] >> gold_metrica_vendas

    [
        bronze_produtos,
        silver_vendas_por_produto,
        silver_produtos_por_cliente,
    ] >> gold_upsell_categorias

    [
        bronze_categorias,
        bronze_clientes,
        gold_metrica_vendas,
        gold_upsell_categorias,
    ] >> delete_cluster
