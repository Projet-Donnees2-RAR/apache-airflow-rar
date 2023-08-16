from airflow.operators.python import PythonOperator
import pandas as pd
import requests

def extract(**kwargs):
    # Get countries data and put it into a dataframe

    countries_response = requests.get('https://api.worldbank.org/v2/country?format=json&per_page=100000')
    countries = countries_response.json()[1]

    country_data = []

    for entry in countries:
        country_data.append({
            "country_id": entry["iso2Code"],
            "country": entry["name"],
            "capital": entry["capitalCity"],
            "longitude": entry["longitude"],
            "latitude": entry["latitude"]
        })
        
    country_df = pd.DataFrame(country_data)

    # Get total gdp per countries and put it into a dataframe

    countries_gdp_response = requests.get('https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?mrnev=1&per_page=300&format=json')

    countries_gdp = countries_gdp_response.json()[1]

    country_gdp_data = []

    for entry in countries_gdp:
        country_gdp_data.append({
            "country_id": entry["country"]["id"],
            "total_gdp": entry["value"]
        })
        
    country_gdp_df = pd.DataFrame(country_gdp_data)

    # Get total population per countries and put it into a dataframe

    countries_population_response = requests.get('https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?mrnev=1&per_page=300&format=json')

    countries_population = countries_population_response.json()[1]

    countries_population_data = []

    for entry in countries_population:
        countries_population_data.append({
            "country_id": entry["country"]["id"],
            "total_population": entry["value"]
        })
        
    countries_population_df = pd.DataFrame(countries_population_data)

    return country_df, country_gdp_df, countries_population_df

def create_extract_task(dag):
    return PythonOperator(
        task_id='extract', 
        dag=dag, 
        python_callable=extract,
        provide_context=True,
        op_args=[]
    )