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


def Hello():
    print("Hello")

df1 = None
df2 = None
df3 = None
def ingest_2018_data():  
    df1 = pd.read_json('./dataset/yellow_taxi_trip_2018.json')
    # Display the first 5 rows of the DataFrame
    Variable.set('df1', df1.to_json())
    print(df1.head())    
    print("Ingesting 2018")

def ingest_2019_data():  
    df2 = pd.read_json('./dataset/yellow_taxi_trip_2018.json')
    # Display the first 5 rows of the DataFrame
    Variable.set('df2', df2.to_json())
    print(df2.head())    
    print("Ingesting 2019")


def ingest_2020_data():  
    df3 = pd.read_json('./dataset/yellow_taxi_trip_2018.json')
    # Display the first 5 rows of the DataFrame
    Variable.set('df3', df3.to_json())
    print(df3.head())    
    print("Ingesting 2020")

def combine_dataset():
    # Create a cursor object mysql+mysqldb://root:<password>localhost/census_db
    df1_json = Variable.get('df1')
    df2_json = Variable.get('df2')
    df3_json = Variable.get('df3')

    df1 = pd.read_json(df1_json)
    df2 = pd.read_json(df2_json)
    df3 = pd.read_json(df3_json)
    df_concat = pd.concat([df1, df2, df3], axis=0, ignore_index=True)
    Variable.set('df_concat', df_concat.to_json())
    print("combine_dataset")

def clean_dataset():
    df_concat_json = Variable.get('df_concat')
    df = pd.read_json(df_concat_json) 
    # Convert datetime column to pandas datetime object
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df = df[(df['tpep_pickup_datetime'].dt.year >= 2018) & (df['tpep_pickup_datetime'].dt.year <= 2020)]
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df = df[(df['tpep_dropoff_datetime'].dt.year >= 2018) & (df['tpep_dropoff_datetime'].dt.year <= 2020)]
    df = df.loc[:, df.nunique() > 1]
    Variable.set('df', df.to_json())

    print("clean_dataset")

def load_database():
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')
    df_concat_json = Variable.get('df')
    df = pd.read_json(df_concat_json) 
    df.to_sql('combined_dataset', engine, if_exists='replace', index=False)
    print("load_database")

def generate_report_1():
    print("generate_report_1")

def generate_report_2():
    print("generate_report_2")

def generate_report_3():
    print("generate_report_3")

def generate_report_4():
    print("generate_report_4")


with DAG(dag_id="my_first_dag", start_date=datetime(2023,2,27), schedule=None, catchup=False) as dag:
    task1 = PythonOperator(task_id="ingest_2018_data", python_callable=ingest_2018_data)
    task3 = PythonOperator(task_id="ingest_2019_data", python_callable=ingest_2019_data)
    task2 = PythonOperator(task_id="combine_dataset", python_callable=combine_dataset)
    task4 = PythonOperator(task_id="ingest_2020_data", python_callable=ingest_2020_data)
    task5 = PythonOperator(task_id="clean_dataset", python_callable=clean_dataset)
    task6 = PythonOperator(task_id="load_database", python_callable=load_database)
    # task7 = PythonOperator(task_id="generate_report_1", python_callable=generate_report_1)
    # task8 = PythonOperator(task_id="generate_report_2", python_callable=generate_report_2)
    # task9 = PythonOperator(task_id="generate_report_3", python_callable=generate_report_3)
    # task10 = PythonOperator(task_id="generate_report_4", python_callable=generate_report_4)

[task1, task3, task4] >> task2 >> task5 >> task6
# task6 >> task7
# task6 >> task8
# task6 >> task9
# task6 >> task10

if __name__ == "__main__":
    ingest_2018_data()
    ingest_2019_data()
    ingest_2020_data()
    combine_dataset()