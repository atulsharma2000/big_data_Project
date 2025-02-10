from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor

args = {
    'owner': 'Atul',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='kafka_spark_dag',
    default_args=args,
    schedule_interval='@daily'
)

with dag:
    # Define the FileSensor to wait for a file
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/home/atul/Desktop/project_big_data/data.csv',  # path to the file you are waiting for
        poke_interval=10,  # check every 10 seconds
        timeout=600  # timeout after 600 seconds
    )

    producer_task = BashOperator(
        task_id='producer',
        bash_command='python3 /home/atul/Desktop/project_big_data/Big_data_Project/producer.py',
    )

    consumer_task = BashOperator(
        task_id='consumer',
        bash_command='python3 /home/atul/Desktop/project_big_data/Big_data_Project/consumer.py',
    )

    data_cleaning_task = BashOperator(
        task_id='data_cleaning',
        bash_command='python3 /home/atul/Desktop/project_big_data/Big_data_Project/clean_in_spark.py',
    )

    news_sentiment_task = BashOperator(
        task_id='news_sentiment',
        bash_command='python3 /home/atul/Desktop/project_big_data/Big_data_Project/news_sentiment_in_hdfs.py',
    )
    
    zeroShot_classification_task = BashOperator(
        task_id='zeroShot_classification',
        bash_command='python3 /home/atul/Desktop/project_big_data/Big_data_Project/zeroShot_classification.py',
    )

    # Set task dependencies
    wait_for_file >> producer_task >> consumer_task >> data_cleaning_task >> news_sentiment_task >> zeroShot_classification_task
