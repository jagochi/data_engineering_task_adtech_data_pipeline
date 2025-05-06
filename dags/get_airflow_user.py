from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import getpass

# Python Task Functions
def return_user():
    import os
    print("Local User:", getpass.getuser())
    print("Local User ID:", os.getuid())

# Set Properties
now = datetime.today() # get current datetime

# Set Default Arguments
def_args = {
    'owner': 'rtm',
    'start_date': datetime(now.year, now.month, now.day, 0, 0), # trigger after this datetime
    'retries': 1,
    'retry_delay': timedelta(seconds = 30)
}

# Set DAG
troubleshooting_check_user = DAG(
    dag_id = 'get_local_user_id',
    default_args = def_args,
    description = 'Returns the local user name and ID.',
    schedule_interval = None,
    catchup = False
)

# Set Tasks
get_local_user_id = PythonOperator(
    task_id = "get_local_user_id",
    python_callable = return_user,
    dag = troubleshooting_check_user
)