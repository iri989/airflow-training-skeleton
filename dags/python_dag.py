import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

args = {
    "start_date": datetime(2020, 1, 1),
}


def myfunc(**context):
    print(context["execution_date"])


dag = DAG(
    dag_id="my_first_dag",
    default_args=args,
    schedule_interval='30 2 * * *',
    description="Demo DAG showing a hello world",
    start_date=datetime(year=2020, month=1, day=1),
    catchup=True
)

wait5 = BashOperator(task_id="wait_5", dag=dag, bash_command="sleep 5")
wait1 = BashOperator(task_id="wait_1", dag=dag, bash_command="sleep 1")
wait10 = BashOperator(task_id="wait_10", dag=dag, bash_command="sleep 10")
the_end = DummyOperator(task_id="the_end", dag=dag)

pytask = PythonOperator(
    task_id='pyoptask',
    python_callable=myfunc,
    provide_context=True,
    dag=dag
)

pytask >> [wait1, wait5, wait10] >> the_end




