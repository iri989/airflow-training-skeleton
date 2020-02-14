import json
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from operators.fp_launch_operator import LaunchLibraryOperator
ARGS = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10),
}


def _print_stats(ds, **context):
    with open(f"/tmp/testing_folder/ds={ds}/result.json") as f:
        data = json.load(f)
        rockets_launched = [launch["name"] for launch in data["launches"]]
        rockets_str = ""
        if rockets_launched:
            rockets_str = f" ({' & '.join(rockets_launched)})"
            print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")
        else:
            print(f"No rockets found in {f.name}")
dag = DAG(
    dag_id="download_rocket_launches",
    default_args=ARGS,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *"
)
download_rocket_launches = LaunchLibraryOperator(
    task_id="download_rocket_launches",
    request_conn_id='launch_rockets_conn',
    endpoint='launch',
    params=dict(startdate='{{ ds }}', enddate='{{ tomorrow_ds }}'),
    result_path='testing_folder',
    result_filename='result.json',
    do_xcom_push=False,
    provide_context=True,
    dag=dag
)
print_stats = PythonOperator(
    task_id="print_stats",
    python_callable=_print_stats,
    provide_context=True,
    dag=dag
)
download_rocket_launches >> print_stats
