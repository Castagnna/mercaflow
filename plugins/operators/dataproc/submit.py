from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
)
from operators.dataproc.defaults import REGION, MAIN_PATH, EGG_PATH


class SubmitPySparkJobOperator(DataprocSubmitPySparkJobOperator):
    @apply_defaults
    def __init__(
        self,
        task_id: str,
        cluster_name: str,
        layer: str,
        job_name: str,
        env: str = "prd",
        mode: str = "cluster",
        datetime: str = None,
        dry_run: bool = False,
        noop: bool = False,
        *args,
        **kwargs
    ):

        super(SubmitPySparkJobOperator, self).__init__(
            task_id=task_id,
            cluster_name=cluster_name,
            arguments=self.__make_arguments(
                layer, job_name, env, mode, datetime, dry_run, noop
            ),
            main=MAIN_PATH,
            pyfiles=[
                EGG_PATH,
                MAIN_PATH,
            ],
            region=REGION,
            *args,
            **kwargs
        )

    @staticmethod
    def __make_arguments(
        layer: str,
        job_name: str,
        env: str = "prd",
        mode: str = "cluster",
        dttm: str = None,
        dry_run: bool = False,
        noop: bool = False,
    ):
        arguments = [
            layer,
            job_name,
            "--env",
            env,
            "--mode",
            mode,
        ]

        if dttm:
            arguments.extend(["--datetime", dttm])

        if dry_run:
            arguments.append("--dry-run")

        if noop:
            arguments.append("--noop")

        return arguments
