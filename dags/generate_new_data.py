from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
import time
import random
from datetime import date, datetime, timedelta

## Set Properties
now = datetime.today() # get current datetime

## Python Task Helper Functions
def select_random_task(**kwargs):
	random.seed()
	time.sleep(random.randint(0, 120))

	return random.choice(['generate_delta_load_advertisers', 'generate_delta_load_campaigns', 'generate_delta_load_impressions', 'generate_delta_load_clicks'])

## Python Task Functions

## Set Default Arguments
def_args = {
	'owner': 'rtm',
	'start_date': days_ago(1), # datetime(now.year, now.month, now.day, 0, 0)
	'retries': 1,
	'retry_delay': timedelta(seconds = 30)
}

## Set DAG
generate_new_data = DAG(
	dag_id = 'generate_new_data',
	default_args = def_args,
	description = 'Generates new data.',
	schedule_interval = timedelta(minutes = 3), # run every minute
	catchup = False,
	is_paused_upon_creation = False
)

## Set Tasks
select_random_generate_task = BranchPythonOperator(
    task_id = 'select_random_generate_task',
    python_callable = select_random_task,
    provide_context = True,
    dag = generate_new_data
)

generate_delta_load_advertisers = BashOperator(
	task_id = 'generate_delta_load_advertisers',
	bash_command = 'python /opt/airflow/scripts/main.py advertisers --count 1',
	dag = generate_new_data
)

generate_delta_load_campaigns = BashOperator(
	task_id = 'generate_delta_load_campaigns',
	bash_command = 'python /opt/airflow/scripts/main.py campaigns --advertiser-id 1 --count 2',
	dag = generate_new_data
)

generate_delta_load_impressions = BashOperator(
	task_id = 'generate_delta_load_impressions',
	bash_command = 'python /opt/airflow/scripts/main.py impressions --campaign-id 1 --count 500',
	dag = generate_new_data
)

generate_delta_load_clicks = BashOperator(
	task_id = 'generate_delta_load_clicks',
	bash_command = 'python /opt/airflow/scripts/main.py clicks --campaign-id 1 --ratio 0.12',
	dag = generate_new_data
)

select_random_generate_task >> [generate_delta_load_advertisers, generate_delta_load_campaigns, generate_delta_load_impressions, generate_delta_load_clicks]