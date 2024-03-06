#!/usr/bin/env bash

export GCP_ENVIRONMENT=$2

DIR_BASE_LOCAL=$(pwd)

if [ $GCP_ENVIRONMENT = "dev" ]; then
  GCS_BUCKET_AIRFLOW="gs://southamerica-east1-airflow-xxxxxxxx-bucket"
elif [ $GCP_ENVIRONMENT = "prd" ]; then
  GCS_BUCKET_AIRFLOW="gs://southamerica-east1-airflow-yyyyyyyy-bucket"
fi

echo "Will deploy composer to env: "$GCP_ENVIRONMENT", bucket: "$GCS_BUCKET_AIRFLOW
gsutil -m rsync -d -r $DIR_BASE_LOCAL/dags "$GCS_BUCKET_AIRFLOW/dags"
gsutil -m rsync -d -r $DIR_BASE_LOCAL/plugins "$GCS_BUCKET_AIRFLOW/plugins"
