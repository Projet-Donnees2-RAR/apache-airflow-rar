from airflow.operators.python import PythonOperator
import boto3
from io import StringIO
from datetime import datetime

def load(**kwargs):
    country_df = kwargs['ti'].xcom_pull(task_ids='transform')

    # Convert the DataFrame to CSV format
    csv_buffer = StringIO()
    country_df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Get the current year, month, and day
    current_date = datetime.now()
    year = current_date.strftime('%Y')
    month = current_date.strftime('%m')
    day = current_date.strftime('%d')

    # Create the file name with the specified format
    file_name = f'country_{year}_{month}_{day}.csv'


    # Upload the CSV content to an AWS S3 bucket
    s3 = boto3.client('s3')
    bucket_name = 'rar-group-bucket'
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_content)

    return "Loading completed successfully!"

def create_load_task(dag):
    return PythonOperator(
        task_id='load', 
        dag=dag, 
        python_callable=load,
        provide_context=True
    )