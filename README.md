# Workflow-Scheduler
Orchestrating workflow scheduling framework for NYC transportation reporting using Apache Airflow        
    
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

### Start Airflow Webserver
```bash
$ airflow webserver --port 8000
```

The web browser should open browser and go to http://localhost:8000 

### Install MySQL server in WSL
```bash
$ sudo apt update && sudo apt upgrade
$ sudo apt install mysql-server
$ mysql --version
$ mysql -u root -p
```
#### Connect MySQL Server in WSL with MySQL Workbench. Check out this: https://www.youtube.com/watch?v=DBsyCk2vZw4

