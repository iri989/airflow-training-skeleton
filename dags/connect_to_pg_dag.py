from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow import DAG
from datetime import datetime

args = {
    "start_date": datetime(2020, 1, 1),
}

dag = DAG(
    dag_id="conn_to_pg",
    default_args=args,
    schedule_interval='30 2 * * *',
    description="Demo DAG showing a hello world",
    start_date=datetime(year=2020, month=1, day=1),
)

operator = PostgresToGoogleCloudStorageOperator(
    task_id="pg_to_gcs",
    sql="SELECT transfer_date FROM land_registry_price_paid_uk LIMIT 10",
    bucket="gdd2020_bucket",
    filename="test",
    postgres_conn_id="postgres_default",
    google_cloud_storage_conn_id="google_cloud_storage_default",
    dag=dag
)
#
# hook = PostgresHook(
#     postgres_conn_id='postgres_default'
# )
#
#
# hook.get_records("SELECT transfer_date FROM land_registry_price_paid_uk LIMIT 10")
