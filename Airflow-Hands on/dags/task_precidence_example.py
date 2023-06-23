from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator

# Define the DAG and its configuration
dag = DAG(
    'task_precedence_example',
    default_args={
        'start_date': datetime(2023, 1, 1),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='0 0 * * *',  # Runs daily at midnight
)

# Define the tasks in the DAG
task1 = DummyOperator(task_id='task1', dag=dag)
task2 = DummyOperator(task_id='task2', dag=dag)
task3 = DummyOperator(task_id='task3', dag=dag)
task4 = DummyOperator(task_id='task4', dag=dag, trigger_rule = 'none_skipped')

# Set the task dependencies
task2.set_upstream(task1)# task2 depends on task1
#task3.set_upstream(task1) # task3 depends on task1
task3.set_upstream(task2)  # task3 depends on task2task3.set_upstream(task2)
task4.set_upstream([task2, task3])  # task4 depends on both task2 and task3
