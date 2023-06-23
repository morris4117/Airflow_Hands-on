#datatime
from datetime import timedelta, datetime

#The Dag Object
from airflow import DAG

#Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import   PythonOperator

#Initializing the default arguments
default_args = {
        'owner' : 'Morries',
        'start_date' : datetime(2023, 5, 5),
        'retries' : 3,
        'retry_delay' : timedelta(minutes=5)
}

#Instantiate a DAG object
hello_world_dag = DAG('hello_world_dag',
        default_args = default_args,
        description = 'Hello World DAG',
        schedule_interval = '* * * * *',
        catchup = False,
        tags = ['example', 'helloworld']
 )

#Create Python callable Function

def print_hello():
    return 'Hello world!'

#Creating tasks
#Create first task
start_task = DummyOperator(task_id = 'start_task', dag = hello_world_dag)

#create 2nd task
hello_world_task = PythonOperator(task_id = 'hello_world_task', python_callable = print_hello, dag = hello_world_dag)

#create 3rd task
end_task = DummyOperator(task_id = 'end_task', dag = hello_world_dag)

#set the order of execution of tasks

start_task >> hello_world_task >> end_task
