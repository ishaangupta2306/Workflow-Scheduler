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


def Hello():
    print("Hello")

def ingest_2018_data():  
    # Create a cursor object mysql+mysqldb://root:<password>localhost/census_db
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')

    # create a session factory
    Session = sessionmaker(bind=engine)

    # create a base class for declarative models
    Base = declarative_base()

    # define a model for the data
    class MyData(Base):
        __tablename__ = 'yellow_taxi_trip_2018'
        id = Column(Integer, primary_key=True, autoincrement=True)
        vendorid = Column(Integer)
        tpep_pickup_datetime = Column(DateTime)
        tpep_dropoff_datetime = Column(DateTime)
        passenger_count = Column(Integer)
        trip_distance = Column(Float)
        ratecodeid = Column(Integer)
        store_and_fwd_flag = Column(String(1))
        pulocationid = Column(Integer)
        dolocationid = Column(Integer)
        payment_type = Column(Integer)
        fare_amount =  Column(Float)
        extra = Column(Float)
        mta_tax = Column(Float)
        tip_amount = Column(Float)
        tolls_amount = Column(Float)
        improvement_surcharge = Column(Float)
        total_amount = Column(Float)

    # create the table in the database
    Base.metadata.create_all(engine)

    # load data from a JSON file
    with open('./dataset/yellow_taxi_trip_2018.json') as f:
        data = json.load(f)

    # create a list of MyData objects from the JSON data
    mydata_list = [MyData(vendorid=d['vendorid'], tpep_pickup_datetime=d['tpep_pickup_datetime'], 
        tpep_dropoff_datetime=d['tpep_dropoff_datetime'],
        passenger_count=d['passenger_count'],
        trip_distance=d['trip_distance'],
        ratecodeid=d['ratecodeid'],
        store_and_fwd_flag=d['store_and_fwd_flag'],
        pulocationid=d['pulocationid'],
        dolocationid=d['dolocationid'],
        payment_type=d['payment_type'],
        fare_amount=d['fare_amount'],
        extra=d['extra'],
        mta_tax=d['mta_tax'],
        tip_amount=d['tip_amount'],
        tolls_amount=d['tolls_amount'],
        improvement_surcharge=d['improvement_surcharge'],
        total_amount=d['total_amount']
    ) for d in data]

    # add the objects to the database
    session = Session()
    session.add_all(mydata_list)
    session.commit()
    print("Ingesting 2018")

def ingest_2019_data():  
    # Create a cursor object mysql+mysqldb://root:<password>localhost/census_db
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')

    # create a session factory
    Session = sessionmaker(bind=engine)

    # create a base class for declarative models
    Base = declarative_base()

    # define a model for the data
    class MyData(Base):
        __tablename__ = 'yellow_taxi_trip_2019'
        id = Column(Integer, primary_key=True, autoincrement=True)
        vendorid = Column(Integer)
        tpep_pickup_datetime = Column(DateTime)
        tpep_dropoff_datetime = Column(DateTime)
        passenger_count = Column(Integer)
        trip_distance = Column(Float)
        ratecodeid = Column(Integer)
        store_and_fwd_flag = Column(String(1))
        pulocationid = Column(Integer)
        dolocationid = Column(Integer)
        payment_type = Column(Integer)
        fare_amount =  Column(Float)
        extra = Column(Float)
        mta_tax = Column(Float)
        tip_amount = Column(Float)
        tolls_amount = Column(Float)
        improvement_surcharge = Column(Float)
        total_amount = Column(Float)

    # create the table in the database
    Base.metadata.create_all(engine)

    # load data from a JSON file
    with open('./dataset/yellow_taxi_trip_2019.json') as f:
        data = json.load(f)

    # create a list of MyData objects from the JSON data
    mydata_list = [MyData(vendorid=d['vendorid'], tpep_pickup_datetime=d['tpep_pickup_datetime'], 
        tpep_dropoff_datetime=d['tpep_dropoff_datetime'],
        passenger_count=d['passenger_count'],
        trip_distance=d['trip_distance'],
        ratecodeid=d['ratecodeid'],
        store_and_fwd_flag=d['store_and_fwd_flag'],
        pulocationid=d['pulocationid'],
        dolocationid=d['dolocationid'],
        payment_type=d['payment_type'],
        fare_amount=d['fare_amount'],
        extra=d['extra'],
        mta_tax=d['mta_tax'],
        tip_amount=d['tip_amount'],
        tolls_amount=d['tolls_amount'],
        improvement_surcharge=d['improvement_surcharge'],
        total_amount=d['total_amount']
    ) for d in data]

    # add the objects to the database
    session = Session()
    session.add_all(mydata_list)
    session.commit()
    print("Ingesting 2019")


def ingest_2020_data():  
    # Create a cursor object mysql+mysqldb://root:<password>localhost/census_db
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')

    # create a session factory
    Session = sessionmaker(bind=engine)

    # create a base class for declarative models
    Base = declarative_base()

    # define a model for the data
    class MyData(Base):
        __tablename__ = 'yellow_taxi_trip_2020'
        id = Column(Integer, primary_key=True, autoincrement=True)
        vendorid = Column(Integer)
        tpep_pickup_datetime = Column(DateTime)
        tpep_dropoff_datetime = Column(DateTime)
        passenger_count = Column(Integer)
        trip_distance = Column(Float)
        ratecodeid = Column(Integer)
        store_and_fwd_flag = Column(String(1))
        pulocationid = Column(Integer)
        dolocationid = Column(Integer)
        payment_type = Column(Integer)
        fare_amount =  Column(Float)
        extra = Column(Float)
        mta_tax = Column(Float)
        tip_amount = Column(Float)
        tolls_amount = Column(Float)
        improvement_surcharge = Column(Float)
        total_amount = Column(Float)

    # create the table in the database
    Base.metadata.create_all(engine)

    # load data from a JSON file
    with open('./dataset/yellow_taxi_trip_2020.json') as f:
        data = json.load(f)

    # create a list of MyData objects from the JSON data
    mydata_list = [MyData(vendorid=d['vendorid'], tpep_pickup_datetime=d['tpep_pickup_datetime'], 
        tpep_dropoff_datetime=d['tpep_dropoff_datetime'],
        passenger_count=d['passenger_count'],
        trip_distance=d['trip_distance'],
        ratecodeid=d['ratecodeid'],
        store_and_fwd_flag=d['store_and_fwd_flag'],
        pulocationid=d['pulocationid'],
        dolocationid=d['dolocationid'],
        payment_type=d['payment_type'],
        fare_amount=d['fare_amount'],
        extra=d['extra'],
        mta_tax=d['mta_tax'],
        tip_amount=d['tip_amount'],
        tolls_amount=d['tolls_amount'],
        improvement_surcharge=d['improvement_surcharge'],
        total_amount=d['total_amount']
    ) for d in data]

    # add the objects to the database
    session = Session()
    session.add_all(mydata_list)
    session.commit()
    print("Ingesting 2020")

def combine_dataset():
    # Create a cursor object mysql+mysqldb://root:<password>localhost/census_db
    engine = create_engine('mysql+pymysql://root:Iliketostore1!@localhost:33061/nyc_yellow_taxi_trip')

    # define the names of the tables to read from
    table_names = ['yellow_taxi_trip_2018', 'yellow_taxi_trip_2019', 'yellow_taxi_trip_2020']

    # read each table into a separate DataFrame
    dfs = []
    for table in table_names:
        df = pd.read_sql_table(table, con=engine)
        dfs.append(df)

    # concatenate the DataFrames into a single DataFrame
    combined_df = pd.concat(dfs)

    # save the combined DataFrame as a new table in the same MySQL database
    table_name = 'combined_yellow_taxi_trip'
    combined_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print("combine_dataset")

def clean_dataset():
    print("clean_dataset")

def load_database():
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
    # task5 = PythonOperator(task_id="clean_dataset", python_callable=clean_dataset)
    # task6 = PythonOperator(task_id="load_database", python_callable=load_database)
    # task7 = PythonOperator(task_id="generate_report_1", python_callable=generate_report_1)
    # task8 = PythonOperator(task_id="generate_report_2", python_callable=generate_report_2)
    # task9 = PythonOperator(task_id="generate_report_3", python_callable=generate_report_3)
    # task10 = PythonOperator(task_id="generate_report_4", python_callable=generate_report_4)

[task1, task3, task4] >> task2
# [task1, task3, task4] >> task2 >> task5 >> task6
# task6 >> task7
# task6 >> task8
# task6 >> task9
# task6 >> task10

if __name__ == "__main__":
    combine_dataset()