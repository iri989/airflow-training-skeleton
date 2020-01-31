import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
args = {
    "start_date": airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id="my_first_dag",
    default_args=args,
    description="Demo DAG showing a hello world"
)

task1 = DummyOperator(task_id="task1", dag=dag)
task2 = DummyOperator(task_id="task2", dag=dag)
task3 = DummyOperator(task_id="task3", dag=dag)
task4 = DummyOperator(task_id="task4", dag=dag)
task5 = DummyOperator(task_id="task5", dag=dag)

task1 >> task2 >> [task3, task4] >> task5
