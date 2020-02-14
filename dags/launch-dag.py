import json
import pathlib
import posixpath
import airflow
import requests

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id="download_rocket_launches",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *"
)


class LaunchHook(BaseHook):
    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass

    def __init__(self, conn_id):
        super().__init__(conn_id)

    def get_conn(self):
        pass

    def do_stuff(self):
        pass


class LaunchToGcsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 retries: int = 1,
                 *args,
                 **kwargs):
        super().__init__(retries=retries, *args, **kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        hook = LaunchHook(conn_id=self.conn_id)


def _download_rocket_launches(ds, tomorrow_ds, **context):
    query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
    result_path = f"/tmp/rocket_launches/ds={ds}"
    pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
    response = requests.get(query)
    print(f"response was {response}")

    with open(posixpath.join(result_path, "launches.json"), "w") as f:
        print(f"Writing to file {f.name}")
        f.write(response.text)


def _print_stats(ds, **context):
    with open(f"/tmp/rocket_launches/ds={ds}/launches.json") as f:
        data = json.load(f)
        rockets_launched = [launch["name"] for launch in data["launches"]]
        rockets_str = ""

        if rockets_launched:
            rockets_str = f" ({' & '.join(rockets_launched)})"
            print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")
        else:
            print(f"No rockets found in {f.name}")


download_rocket_launches = LaunchToGcsOperator(
    task_id="download_rocket_launches",
    conn_id="launchlibrary",
    endpoint="launch",
    params={"startdate": "{{ ds }}", "enddate": "{{ tomorrow_ds }}"},
    result_bucket="mydata",
    result_key="/data/rocket_launches/ds={{ ds }}/launches.json",
    dag=dag
)

print_stats = PythonOperator(
    task_id="print_stats",
    python_callable=_print_stats,
    provide_context=True,
    dag=dag
)

download_rocket_launches >> print_stats
