# Workflow Scheduler for New York City Taxi 

Yellow Cab taxis are an iconic method of transportation in the New York City. Using Apache Airflow, automated the following data engineering process for 3 years (2018-2020) for Yellow taxis Data
1. Data Ingestion 
2. Data Combining
3. Data Cleaning
4. Loading Data in MySQL database
5. Generating Monthly Summary Statistics reports 
    
![Airflow](https://lh4.googleusercontent.com/JvfkB7JkgUjAnbhbTwaTGyxAblaM4hBF8ws6cWYkTg2DzJjf5IwMq3u4JKkptDrecx7dLUa4IQGCwp-9jGON_cD8US93432eNBHefNYf-4Qgv42znqLCMo0QeZyI6OLSr1uPw0X8)
![MySQL](https://avatars.githubusercontent.com/u/2452804?s=200&v=4)



### Install Apache Airflow (in WSL)
```bash
$ virtualenv venv
$ source venv/bin/activate.
$ pip3 install apache-airflow
```
### Setup Apache Airflow (in WSL)
```bash
$ cd ~/airflow
$ airflow db init
$ mkdir dags 
```
### Create a new Airflow User: 
```bash
$ airflow users create --username <username> --password <password> --firstname <first name> --lastname <last name> --role Admin --email <email>
$ airflow users list
```

### Start Airflow Scheduler
```bash
$ airflow scheduler
```
![SchedulerStart](./resources/airflow-scheduler-cli-start.jpg)
#### Airflow Scheduler Logs
![SchedulerStart](./resources/airflow-scheduler-cli-logs.jpg)

### Start Airflow Webserver
```bash
$ airflow webserver --port 8000
```
![WebserverStart](./resources/airflow-webserver-cli-start.jpg)

#### Airflow Webserver Logs
![WebserverStart](./resources/airflow-webserver-cli-logs.jpg)

The web browser should open browser and go to http://localhost:8000 

### Install MySQL server in WSL
```bash
$ sudo apt update && sudo apt upgrade
$ sudo apt install mysql-server
$ mysql --version
$ mysql -u root -p
```
#### Connect MySQL Server in WSL with MySQL Workbench. Check out this: https://www.youtube.com/watch?v=DBsyCk2vZw4

### Worflow Framework 
    1. Ingest 2018, 2019, 2020 Dataset = Ingesting Data from API endpoints and saving as JSON files
    2. Combine Dataset = Combined the 3 Dataframes
    3. Cleane Dataset 
        (a) Discarded data entries with datatime field values outside 2018, 2019, 2020
        (b) Discard columns with no more than 1 unique entries
    4. Load Database = Cleaned dataset loaded in MySQL
    5. Generated reports     
![Subsampling](./resources/workflow-diagram.jpg)

### Load Database (Cleaned dataset loaded in MySQL)
#### Combined 2018-2020 NYC Taxi trip Dataset
![Subsampling](./resources/load-data-mysql.jpg)
#### Taxi and Zone Lookup Dataset
![Subsampling](./resources/load-data-mysql2.jpg)


### Dataset Sources & References
1. 2018: https://data.cityofnewyork.us/resource/t29m-gskq.jsonLinks 
2. 2019: https://data.cityofnewyork.us/resource/2upf-qytp.jsonLinks 
3. 2020: https://data.cityofnewyork.us/resource/kxp8-n2sj.jsonLinks 
4. Taxi Zones: https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv
5. Metadata info: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
