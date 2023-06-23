import airflow
from datetime import timedelta
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator



default_args = {
        'owner': 'airflow',
        'start_date': datetime(2023, 6, 21),
        #'end_date': datetime(),
        # 'depends_on_past': False,
        # 'email': ['airflow@example.com'],
        # 'email_on_failure': False,
        #'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}


dag_email = DAG(
        dag_id = 'emailjob_demo',
        default_args=default_args,
        schedule_interval='@once',
        dagrun_timeout=timedelta(minutes=60),
        description='use case of email operator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)


def start_task():
    return "task started"

start_task = PythonOperator(
            task_id='executetask',
            python_callable=start_task,
            dag=dag_email
)

send_email = EmailOperator(
            task_id='send_email',
            to=['morriesdanielinfo@gmail.com'],
            subject='ingestion complete',
            html_content='<h1>The Gmail dag is successfully running</h1>',
            dag=dag_email

)

send_email.set_upstream(start_task)
