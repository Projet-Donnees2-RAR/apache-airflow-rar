from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np

def transform(**kwargs):
    country_df, country_gdp_df, countries_population_df = kwargs['ti'].xcom_pull(task_ids='extract')
   
    # Merge country_gdp_df and countries_population_df into country_df
    country_df = pd.merge(country_df, country_gdp_df, on='country_id')

    country_df = pd.merge(country_df, countries_population_df, on='country_id')

    country_df

    # Drop rows where at least one element is null
    country_df.replace('', np.nan, inplace=True)
    
    country_df.dropna(subset=['capital', 'longitude', 'latitude'], inplace=True)

    return country_df

def create_transform_task(dag):
    return PythonOperator(
        task_id='transform', 
        dag=dag, 
        python_callable=transform,
        provide_context=True,
        op_args=[]
    )