from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
)
from operators.dataproc.defaults import REGION, MAIN_PATH, EGG_PATH


class SubmitPySparkJobOperator(DataprocSubmitPySparkJobOperator):
    @apply_defaults
    def __init__(
        self, task_id: str, arguments: list, cluster_name: str, *args, **kwargs
    ):
        super(SubmitPySparkJobOperator, self).__init__(
            task_id=task_id,
            main=MAIN_PATH,
            pyfiles=[
                EGG_PATH,
                MAIN_PATH,
            ],
            arguments=arguments,
            cluster_name=cluster_name,
            region=REGION,
            *args,
            **kwargs
        )
