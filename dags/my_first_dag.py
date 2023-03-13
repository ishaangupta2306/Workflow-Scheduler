# Import the DAG object
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
import json
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from airflow.models import Variable
from sqlalchemy import types

def Hello():
    print("Hello")

def ingest_2018_data():  
    df1 = pd.read_json('./dataset/yellow_taxi_trip_2018.json', convert_dates=['datetime_field'])
    # Display the first 5 rows of the DataFrame
    Variable.set('df1', df1.to_json())
    print(df1.head())    
    print("Ingesting 2018")

def ingest_2019_data():  
    df2 = pd.read_json('./dataset/yellow_taxi_trip_2018.json', convert_dates=['datetime_field'])
    # Display the first 5 rows of the DataFrame
    Variable.set('df2', df2.to_json())
    print(df2.head())    
    print("Ingesting 2019")


def ingest_2020_data():  
    df3 = pd.read_json('./dataset/yellow_taxi_trip_2018.json', convert_dates=['datetime_field'])
    # Display the first 5 rows of the DataFrame
    Variable.set('df3', df3.to_json())
    print(df3.head())    
    print("Ingesting 2020")

def combine_dataset():
    # Create a cursor object mysql+mysqldb://root:<password>localhost/census_db
    df1_json = Variable.get('df1')
    df2_json = Variable.get('df2')
    df3_json = Variable.get('df3')

    df1 = pd.read_json(df1_json, convert_dates=['datetime_field'])
    df2 = pd.read_json(df2_json, convert_dates=['datetime_field'])
    df3 = pd.read_json(df3_json, convert_dates=['datetime_field'])
    df_concat = pd.concat([df1, df2, df3], axis=0, ignore_index=True)
    Variable.set('df_concat', df_concat.to_json())
    print("combine_dataset")

def clean_dataset():
    df_concat_json = Variable.get('df_concat')
    df = pd.read_json(df_concat_json, convert_dates=['datetime_field']) 
    # Convert datetime column to pandas datetime object
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df = df[(df['tpep_pickup_datetime'].dt.year >= 2018) & (df['tpep_pickup_datetime'].dt.year <= 2020)]
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df = df[(df['tpep_dropoff_datetime'].dt.year >= 2018) & (df['tpep_dropoff_datetime'].dt.year <= 2020)]
    df = df.loc[:, df.nunique() > 1]
    Variable.set('df', df.to_json(date_format='iso'))
    print("clean_dataset")

def load_database():
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')
    dtypes = {
    'tpep_pickup_datetime': types.DATETIME(),
    'tpep_dropoff_datetime': types.DATETIME()
    }
    
    df_concat_json = Variable.get('df')
    df = pd.read_json(df_concat_json) 
    df.to_sql('combined_dataset', engine, if_exists='replace', index=False, dtype=dtypes)
    
    df = pd.read_csv('./dataset/taxi+_zone_lookup.csv')
    table_name = 'taxi_zone_lookup'
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print("load_database")

def generate_report_1():
    # Monthly Summary Statistics reports providing the following information Trips per day - Average number of trips recorded each day
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')
    df = pd.read_sql_table('combined_dataset', con=engine)
    query = '''
        SELECT DATE(tpep_pickup_datetime) as date, COUNT(*) as num_trips 
        FROM combined_dataset 
        GROUP BY date
    '''
    df = pd.read_sql_query(query, con=engine)
    df['month'] = pd.to_datetime(df['date']).dt.to_period('M')  # add a new column with the month
    grouped_df = df.groupby('month')['num_trips'].mean()  # group by month and find the mean of value_column
    grouped_df = grouped_df.reset_index()  # reset the index to include the month column in the CSV file
    print(grouped_df)
    directory_path = './reports'
    file_name = 'report1.csv'
    file_path = f"{directory_path}/{file_name}"
    grouped_df.to_csv(file_path, index=False)   
    
    print("generate_report_1")

def generate_report_2():
    # Farebox per day - Total amount, across all vehicles, collected from all fares, surcharges, taxes, and tolls. Note: this amount does not include
    # amounts from credit card tips

    # fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge
    # if payment_type = 1, then ignore tips 
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')
    df = pd.read_sql_table('combined_dataset', con=engine)
    query = '''
        SELECT
            DATE(tpep_pickup_datetime) AS date,
            CASE WHEN payment_type = 1
                THEN SUM(fare_amount + extra + mta_tax + tolls_amount + improvement_surcharge)
                ELSE SUM(fare_amount + extra + mta_tax + tolls_amount + improvement_surcharge + tip_amount)
            END AS farebox
        FROM combined_dataset
        GROUP BY DATE(tpep_pickup_datetime), payment_type;
    '''
    df = pd.read_sql_query(query, con=engine)
    print(df)
    df['month'] = pd.to_datetime(df['date']).dt.to_period('M')  # add a new column with the month
    grouped_df = df.groupby('month')['farebox'].mean()  # group by month and find the mean of value_column
    grouped_df = grouped_df.reset_index()  # reset the index to include the month column in the CSV file
    print(grouped_df)
    directory_path = './reports'
    file_name = 'report2.csv'
    file_path = f"{directory_path}/{file_name}"
    grouped_df.to_csv(file_path, index=False)   
    
    print("generate_report_2")

def generate_report_3():    
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')
    query = '''
        select year, month, Borough as borough, sum(count) as pickup_and_dropoff_count
        from ((
        select YEAR(tpep_pickup_datetime) as year, MONTH(tpep_pickup_datetime) as month,
        pickup_zone.Borough, count(*) as count
        from combined_dataset
        inner join taxi_zone_lookup as pickup_zone on
        combined_dataset.pulocationid=pickup_zone.LocationID
        inner join taxi_zone_lookup as dropoff_zone on
        combined_dataset.dolocationid=dropoff_zone.LocationID
        group by year, month, pickup_zone.Borough, dropoff_zone.Borough
        )
        union
        (
        select YEAR(tpep_pickup_datetime) as year, MONTH(tpep_pickup_datetime) as month,
        dropoff_zone.Borough, count(*) as count
        from combined_dataset
        inner join taxi_zone_lookup as pickup_zone on
        combined_dataset.pulocationid=pickup_zone.LocationID
        inner join taxi_zone_lookup as dropoff_zone on
        combined_dataset.dolocationid=dropoff_zone.LocationID
        group by year, month, pickup_zone.Borough, dropoff_zone.Borough
        )) as pickup_and_dropoff_counts_separate
        group by year, month, Borough
        order by year, month'''
    
    df = pd.read_sql_query(query, con=engine)
    print(df)
    directory_path = './reports'
    file_name = 'report3.csv'
    file_path = f"{directory_path}/{file_name}"
    df.to_csv(file_path, index=False)   
    print("generate_report_4")

def generate_report_4():
    # Trip miles per month over time - Total distance in miles reported by the taximeter per month from 2018 to 2020
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')
    df = pd.read_sql_table('combined_dataset', con=engine)
    query = '''
        SELECT YEAR(tpep_pickup_datetime) as year, MONTH(tpep_pickup_datetime) as month, COUNT(trip_distance) as trip_miles 
        FROM combined_dataset 
        GROUP BY year, month
    '''
    df = pd.read_sql_query(query, con=engine)
    print(df)
    directory_path = './reports'
    file_name = 'report4.csv'
    file_path = f"{directory_path}/{file_name}"
    df.to_csv(file_path, index=False)   
    print("generate_report_4")

with DAG(dag_id="my_first_dag", start_date=datetime(2023,2,27), schedule=None, catchup=False) as dag:
    task1 = PythonOperator(task_id="ingest_2018_data", python_callable=ingest_2018_data)
    task3 = PythonOperator(task_id="ingest_2019_data", python_callable=ingest_2019_data)
    task2 = PythonOperator(task_id="combine_dataset", python_callable=combine_dataset)
    task4 = PythonOperator(task_id="ingest_2020_data", python_callable=ingest_2020_data)
    task5 = PythonOperator(task_id="clean_dataset", python_callable=clean_dataset)
    task6 = PythonOperator(task_id="load_database", python_callable=load_database)
    task7 = PythonOperator(task_id="generate_report_1", python_callable=generate_report_1)
    task8 = PythonOperator(task_id="generate_report_2", python_callable=generate_report_2)
    task9 = PythonOperator(task_id="generate_report_3", python_callable=generate_report_3)
    task10 = PythonOperator(task_id="generate_report_4", python_callable=generate_report_4)

[task1, task3, task4] >> task2 >> task5 >> task6
task6 >> task7
task6 >> task8
task6 >> task9
task6 >> task10

if __name__ == "__main__":
    # ingest_2018_data()
    # ingest_2019_data()
    # ingest_2020_data()
    # clean_dataset()
    # combine_dataset()
    load_database()
    generate_report_3()