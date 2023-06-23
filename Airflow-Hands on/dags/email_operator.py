from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from airflow.operators.email_operator import EmailOperator

default_args = {
        'owner': 'morris',
        'start_date': datetime(2023, 5, 5),
        'retries':1,
        'retry_delay':timedelta(minutes=2),
        'email':['morriesdanielinfo@gmail.com'],
        'email_on_failure':False,
        'email_on_retry':False
}

dag_email = DAG(
        dag_id='emailoperator_demo',
        default_args=default_args,
        schedule_interval='@daily',
        description='use case of email operator in airflow',
        catchup=False
)

def start_task():
    return "Starting task"

#default_args = {
#        'owner': 'morris',
#        'start_date': datetime(2023, 5, 5),
#        'retries':1,
#        'retry_delay':timedelta(minutes=2),
#        'email':['morriesdanielinfo@gmail.com'],
#        'email_on_failure':False,
#        'email_on_retry':False
#}

#dag_email = DAG(
#        dag_id='emailoperator_demo',
#        default_args=default_args,
#        schedule_interval='@daily',
#        description='use case of email operator in airflow',
#        catchup=False
#)

#send_email = EmailOperator(
#        task_id='send_email',
#        to=['morriesdanielinfo@gmail.com'],
#        subject='ingestion complete',
#        html_content='<h1>Message from Airflow --> Output file Generated</h1>',
#        dag=dag_email
#)

def run_spark_job():
    spark = SparkSession.builder.master("local").appName("TimberlandStockAnalysis").getOrCreate()

    input_file = "/root/airflow/input_file/timberland_stock.csv"

    # Read the CSV file using Spark
    data = spark.read.csv(input_file, header=True)

    data.createOrReplaceTempView("stock_data")
    # What day had the Peak High in Price?
    max_high_date = spark.sql("SELECT Date FROM stock_data ORDER BY High DESC LIMIT 1")

    # What is the mean of the Close column?
    close_mean = spark.sql("SELECT AVG(Close) FROM stock_data")

    # What is the max and min of the Volume column?
    volume_max = spark.sql("SELECT MAX(Volume) FROM stock_data")
    volume_min = spark.sql("SELECT MIN(Volume) FROM stock_data")

    # How many days was the Close lower than 60 dollars?
    close_lt_60_count = spark.sql("SELECT COUNT(*) FROM stock_data WHERE Close < 60")

    # What percentage of the time was the High greater than 80 dollars?
    high_gt_80_percentage = spark.sql("SELECT (COUNT(*) * 100) / (SELECT COUNT(*) FROM stock_data) FROM stock_data WHERE High > 80")

    # What is the Pearson correlation between High and Volume?
    correlation = spark.sql("SELECT corr(High, Volume) FROM stock_data")

    # What is the max High per year?
    max_high_per_year = spark.sql("SELECT YEAR(Date) AS Year, MAX(High) AS MaxHigh FROM stock_data GROUP BY YEAR(Date) ORDER BY Year")


    # What is the average Close for each Calendar Month?
    avg_close_per_month = spark.sql("SELECT MONTH(Date) AS Month, AVG(Close) AS AvgClose FROM stock_data GROUP BY MONTH(Date) ORDER BY Month")

    # Write the output DataFrame to a CSV file
    max_high_date.write.csv('/root/airflow/output_files/max_high_date.csv', mode="overwrite", header=True)
    close_mean.write.csv('/root/airflow/output_files/close_mean.csv', mode="overwrite", header=True)
    volume_max.write.csv('/root/airflow/output_files/volume_max.csv', mode="overwrite", header=True)
    volume_min.write.csv('/root/airflow/output_files/volume_min.csv', mode="overwrite", header=True)
    close_lt_60_count.write.csv('/root/airflow/output_files/close_lt_60_count.csv', mode="overwrite", header=True)
    high_gt_80_percentage.write.csv('/root/airflow/output_files/high_gt_80_percentage.csv', mode="overwrite", header=True)
    correlation.write.csv('/root/airflow/output_files/correlation.csv', mode="overwrite", header=True)
    max_high_per_year.write.csv('/root/airflow/output_files/max_high_per_year.csv', mode="overwrite", header=True)
    avg_close_per_month.write.csv('/root/airflow/output_files/avg_close_per_month.csv', mode="overwrite", header=True)

start_task = DummyOperator(
        task_id='start_task', 
        dag=dag_email
)
    
run_spark_job = PythonOperator(
        task_id='Execute_job', 
        python_callable=run_spark_job, 
        dag=dag_email
)

end_task = DummyOperator(
        task_id='end_task', 
        dag=dag_email
)

send_email = EmailOperator(
        task_id='send_email',
        to=['morriesdanielinfo@gmail.com'],
        subject='ingestion complete',
        html_content='<h1>Message from Airflow --> Output file Generated</h1>',
        dag=dag_email
)

start_task >> run_spark_job >> end_task >> send_email
