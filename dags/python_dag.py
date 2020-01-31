import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

import calendar

args = {
    "start_date": datetime(2020, 1, 1),
}


def myfunc(**context):
    print(context["execution_date"])


dag = DAG(
    dag_id="pyop_dag",
    default_args=args,
    schedule_interval='30 2 * * *',
    description="Demo DAG showing a hello world",
    start_date=datetime(year=2020, month=1, day=1),
    catchup=True
)

the_end = BashOperator(task_id="the_end", dag=dag, bash_command="echo 'done'")


# list(calendar.day_abbr)
# ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


def check_date(execution_date, **context):
    return execution_date.strftime("%a")


pytask = BranchPythonOperator(
    task_id='check_day',
    python_callable=check_date(),
    provide_context=True,
    dag=dag
)

week_day_person_to_email = {
    0: "Bob",  # Mon
    1: "Joe",  # Tue
    2: "Alice",  # Wed
    3: "Joe",  # Thu
    4: "Alice",  # Fri
    5: "Alice",  # Sat
    6: "Alice"  # Sun
}
sth = []
email_bob = DummyOperator(task_id="email_bob", dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)
email_joe = DummyOperator(task_id="email_joe", dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)
email_alice = DummyOperator(task_id="email_alice", dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS)


pytask >> [email_bob, email_joe, email_alice] >> the_end
