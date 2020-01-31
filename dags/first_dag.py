import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
args = {
    "start_date": datetime(2020, 1, 1),
}

dag = DAG(
    dag_id="my_first_dag",
    default_args=args,
    schedule_interval='@daily',
    description="Demo DAG showing a hello world"
)

task1 = DummyOperator(task_id="task1", dag=dag)
task2 = DummyOperator(task_id="task2", dag=dag)
task3 = DummyOperator(task_id="task3", dag=dag)
task4 = DummyOperator(task_id="task4", dag=dag)
task5 = DummyOperator(task_id="task5", dag=dag)

task1 >> task2 >> [task3, task4] >> task5
