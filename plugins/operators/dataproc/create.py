from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    ClusterGenerator,
)
from operators.dataproc.defaults import REGION, ZONE, PROJECT_ID


class CreateClusterOperator(DataprocCreateClusterOperator):
    @apply_defaults
    def __init__(
        self,
        task_id: str,
        cluster_name: str,
        master_machine_type: str,
        worker_machine_type: str,
        num_workers: int,
        *args,
        **kwargs
    ):
        super(CreateClusterOperator, self).__init__(
            task_id=task_id,
            cluster_name=cluster_name,
            region=REGION,
            project_id=PROJECT_ID,
            cluster_config=ClusterGenerator(
                project_id=PROJECT_ID,
                zone=ZONE,
                master_machine_type=master_machine_type,
                worker_machine_type=worker_machine_type,
                num_workers=num_workers,
                init_actions_uris=[
                    "gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"
                ],
                subnetwork_uri=Variable.get("subnetwork"),
                internal_ip_only=True,
                service_account=Variable.get("service_account"),
                image_version="2.0",
                enable_component_gateway=True,
            ).make(),
            *args,
            **kwargs
        )
