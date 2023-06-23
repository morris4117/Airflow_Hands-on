from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

dag = DAG(
    'file_sensor_demo',
    description='Example of FileSensor',
    start_date=datetime(2023, 6, 19),
    schedule_interval=None,
)

start_task = DummyOperator(task_id='start_task', dag=dag)
file_sensor = FileSensor(
        task_id='file_sensor_task',
    filepath='/root/airflow/files/file.txt',
    poke_interval=10,  # Interval to check for the file's presence (in seconds)
    dag=dag,
)



end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> file_sensor >> end_task
