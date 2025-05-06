from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client #from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from datetime import date, datetime, timedelta
import logging

## Set Properties
now = datetime.today() # get current datetime

## Python Task Helper Functions
def set_datime_no_tz(datetime_value):
	
	if isinstance(datetime_value, datetime):
		
		if datetime_value.tzinfo is not None:
			datetime_value = datetime_value.replace(tzinfo = None)
		
		return datetime_value

	elif isinstance(datetime_value, date):

		return datetime(datetime_value.year, datetime_value.month, datetime_value.day, 0, 0, 0, 0)

	return datetime_value

## Python Task Functions
def query_postres_tables(**kwargs):

	airflow_hook = PostgresHook(
		postgres_conn_id = 'postgres_default',
		host = 'postgres',
		schema = 'postgres',
		login = 'postgres',
		password = 'postgres',
		port = 5432
	)

	has_data = True
	tables = ['advertiser', 'campaign', 'impressions', 'clicks']

	for table in tables:

		sql_query = f'SELECT COUNT(*) FROM {table}'
		query_result = airflow_hook.get_first(sql_query)
		row_count = query_result[0]

		if row_count == 0:
			has_data = False

	if not has_data:
		return 'generate_initial_load'
	else:
		return 'create_olap_schema'

def create_clickhouse_schema(**kwargs):
	try:
		#logging.info("Connecting to ClickHouse Database")

		airflow_hook = Client(
			host = 'clickhouse',
			user = 'clickhouse',
			password = 'clickhouse',
			port = 9000 # 9000
		)

		has_schema_query = f"SELECT name FROM system.databases WHERE name = 'clickhouse'"
		has_schema = airflow_hook.execute(has_schema_query)

		if not has_schema:

			create_schema_query = f'CREATE DATABASE IF NOT EXISTS clickhouse'
			airflow_hook.execute(create_schema_query)

		#logging.info("Create Table Advertiser Campaigns (if does not exist)")
		create_schema_tables_queries = [
		'''
			CREATE TABLE IF NOT EXISTS clickhouse.advertiser_campaigns (
				advertiser_name String,
				campaign_name String,
				campaign_bid Float64,
				campaign_budget Float64,
				campaign_impressions_count_total Int32,
				campaign_clicks_count_total Int32,
				campaign_start_date DateTime,
				campaign_end_date DateTime,
				rtm_advertiser_id Int32,
				rtm_advertiser_gmt_created DateTime,
				rtm_advertiser_gmt_modified DateTime,
				rtm_campaign_id Int32,
				rtm_campaign_gmt_created DateTime,
				rtm_campaign_modified DateTime,
				rtm_impressions_gmt_created DateTime,
				rtm_clicks_gmt_created DateTime,
				rtm_delta DateTime
			) ENGINE = ReplacingMergeTree(rtm_delta)
			ORDER BY (rtm_advertiser_id, rtm_campaign_id);
		''',

		'''
			CREATE TABLE IF NOT EXISTS clickhouse.advertiser_campaigns_impressions (
				campaign_impressions_count_daily Int32,
				campaign_impressions_date Date,
				rtm_advertiser_id Int32,
				rtm_campaign_id Int32,
				rtm_delta DateTime
			) ENGINE = ReplacingMergeTree(rtm_delta)
			ORDER BY (rtm_advertiser_id, rtm_campaign_id, campaign_impressions_date);
		''',
		'''
			CREATE TABLE IF NOT EXISTS clickhouse.advertiser_campaigns_clicks (
				campaign_clicks_count_daily Int32,
				campaign_clicks_date Date,
				rtm_advertiser_id Int32,
				rtm_campaign_id Int32,
				rtm_delta DateTime
			) ENGINE = ReplacingMergeTree(rtm_delta)
			ORDER BY (rtm_advertiser_id, rtm_campaign_id, campaign_clicks_date);
		''']

		for query in create_schema_tables_queries:
			airflow_hook.execute(query)
		
		#validate_table_creation_query = f'DESCRIBE TABLE clickhouse.advertiser_campaigns'
		#is_valid = airflow_hook.execute(validate_table_creation_query)

		#if is_valid:
		#	logging.info('Table Advertiser Campaigns Created')
		#else:
		#	logging.error('Failed to Create Table Advertiser Campaigns')

	except Exception as e:
		#logging.error(f"An error occurred: {str(e)}")
		raise

def extract_transform_initial_totals_from_oltp(**kwargs):

	airflow_hook_oltp = PostgresHook(
		postgres_conn_id = 'postgres_default',
		host = 'postgres',
		schema = 'postgres',
		login = 'postgres',
		password = 'postgres',
		port = 5432
	)
	oltp_connection = airflow_hook_oltp.get_conn()
	oltp_cursor = oltp_connection.cursor()

	et_query = '''
		WITH Campaigns AS
		(
			SELECT
				advertiser.name AS advertiser_name,
				campaign.name AS campaign_name,
				campaign.bid AS campaign_bid,
				campaign.budget AS campaign_budget,
				campaign.start_date AS campaign_start_date,
				campaign.end_date AS campaign_end_date,
				advertiser.id AS rtm_advertiser_id,
				advertiser.created_at AS rtm_advertiser_gmt_created,
				advertiser.updated_at AS rtm_advertiser_gmt_modified,
				campaign.id AS rtm_campaign_id,
				campaign.created_at AS rtm_campaign_gmt_created,
				campaign.updated_at AS rtm_campaign_modified
			FROM campaign
			LEFT JOIN advertiser ON advertiser.id = campaign.advertiser_id
		),
		ImpressionsTotal AS
		(
			SELECT
				impressions.campaign_id,
				COUNT(*) AS campaign_impressions_count_total,
				MAX(impressions.created_at) AS rtm_impressions_gmt_created
			FROM impressions
			GROUP BY campaign_id
		),
		ClicksTotal AS
		(
			SELECT
				clicks.campaign_id,
				COUNT(*) AS campaign_clicks_count_total,
				MAX(clicks.created_at) AS rtm_clicks_gmt_created
			FROM clicks
			GROUP BY campaign_id
		),
		ExtractTransform AS
		(
			SELECT
				Campaigns.advertiser_name,
				Campaigns.campaign_name,
				Campaigns.campaign_bid,
				Campaigns.campaign_budget,
				ImpressionsTotal.campaign_impressions_count_total,
				ClicksTotal.campaign_clicks_count_total,
				Campaigns.campaign_start_date,
				Campaigns.campaign_end_date,
				Campaigns.rtm_advertiser_id,
				Campaigns.rtm_advertiser_gmt_created,
				Campaigns.rtm_advertiser_gmt_modified,
				Campaigns.rtm_campaign_id,
				Campaigns.rtm_campaign_gmt_created,
				Campaigns.rtm_campaign_modified,
				ImpressionsTotal.rtm_impressions_gmt_created,
				ClicksTotal.rtm_clicks_gmt_created,
				GREATEST(Campaigns.rtm_advertiser_gmt_modified, Campaigns.rtm_campaign_modified, ImpressionsTotal.rtm_impressions_gmt_created, ClicksTotal.rtm_clicks_gmt_created) AS rtm_delta
			FROM Campaigns
			LEFT JOIN ImpressionsTotal ON ImpressionsTotal.campaign_id = Campaigns.rtm_campaign_id
			LEFT JOIN ClicksTotal ON ClicksTotal.campaign_id = Campaigns.rtm_campaign_id
		)
		SELECT
			advertiser_name,
			campaign_name,
			campaign_bid,
			campaign_budget,
			campaign_impressions_count_total,
			campaign_clicks_count_total,
			campaign_start_date,
			campaign_end_date,
			rtm_advertiser_id,
			rtm_advertiser_gmt_created,
			rtm_advertiser_gmt_modified,
			rtm_campaign_id,
			rtm_campaign_gmt_created,
			rtm_campaign_modified,
			rtm_impressions_gmt_created,
			rtm_clicks_gmt_created,
			rtm_delta
		FROM ExtractTransform
	'''

	oltp_cursor.execute(et_query)
	oltp_data = oltp_cursor.fetchall()

	oltp_data = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_data]

	kwargs['ti'].xcom_push(key = 'oltp_data', value = oltp_data) # use task instance to keep the data object and send to another task

def extract_transform_initial_impressions_from_oltp(**kwargs):

	airflow_hook_oltp = PostgresHook(
		postgres_conn_id = 'postgres_default',
		host = 'postgres',
		schema = 'postgres',
		login = 'postgres',
		password = 'postgres',
		port = 5432
	)
	oltp_connection = airflow_hook_oltp.get_conn()
	oltp_cursor = oltp_connection.cursor()

	et_query = '''
		WITH ImpressionsDaily AS
		(
			SELECT
				campaign.advertiser_id AS rtm_advertiser_id,
				impressions.campaign_id AS rtm_campaign_id,
				COUNT(*) AS campaign_impressions_count_daily,
				CAST(impressions.created_at AS DATE) AS campaign_impressions_date,
				MAX(impressions.created_at) AS rtm_delta
			FROM impressions
			LEFT JOIN campaign ON campaign.id = impressions.campaign_id
			GROUP BY campaign.advertiser_id, impressions.campaign_id, CAST(impressions.created_at AS DATE)
		)
		SELECT
			campaign_impressions_count_daily,
			campaign_impressions_date,
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_delta
		FROM ImpressionsDaily
		ORDER BY rtm_advertiser_id, rtm_campaign_id, campaign_impressions_date
	'''

	oltp_cursor.execute(et_query)
	oltp_data = oltp_cursor.fetchall()

	oltp_data = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_data]

	kwargs['ti'].xcom_push(key = 'oltp_data', value = oltp_data)

def extract_transform_initial_clicks_from_oltp(**kwargs):

	airflow_hook_oltp = PostgresHook(
		postgres_conn_id = 'postgres_default',
		host = 'postgres',
		schema = 'postgres',
		login = 'postgres',
		password = 'postgres',
		port = 5432
	)
	oltp_connection = airflow_hook_oltp.get_conn()
	oltp_cursor = oltp_connection.cursor()

	et_query = '''
		WITH ClicksDaily AS
		(
			SELECT
				campaign.advertiser_id AS rtm_advertiser_id,
				clicks.campaign_id AS rtm_campaign_id,
				COUNT(*) AS campaign_clicks_count_daily,
				CAST(clicks.created_at AS DATE) AS campaign_clicks_date,
				MAX(clicks.created_at) AS rtm_delta
			FROM clicks
			LEFT JOIN campaign ON campaign.id = clicks.campaign_id
			GROUP BY campaign.advertiser_id, clicks.campaign_id, CAST(clicks.created_at AS DATE)
		)
		SELECT
			campaign_clicks_count_daily,
			campaign_clicks_date,
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_delta
		FROM ClicksDaily
		ORDER BY rtm_advertiser_id, rtm_campaign_id, campaign_clicks_date
	'''

	oltp_cursor.execute(et_query)
	oltp_data = oltp_cursor.fetchall()

	oltp_data = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_data]

	kwargs['ti'].xcom_push(key = 'oltp_data', value = oltp_data)

def load_initial_totals_to_olap(**kwargs):

	airflow_hook_olap = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000
	)

	olap_query = '''
		INSERT INTO clickhouse.advertiser_campaigns (advertiser_name, campaign_name, campaign_bid, campaign_budget, campaign_impressions_count_total, campaign_clicks_count_total, campaign_start_date, campaign_end_date, rtm_advertiser_id, rtm_advertiser_gmt_created, rtm_advertiser_gmt_modified, rtm_campaign_id, rtm_campaign_gmt_created, rtm_campaign_modified, rtm_impressions_gmt_created, rtm_clicks_gmt_created, rtm_delta)
		VALUES
	'''

	oltp_data = kwargs['ti'].xcom_pull(task_ids = 'et_initial_totals_oltp', key = 'oltp_data')

	oltp_rows = [tuple(item[:17]) for item in oltp_data]

	airflow_hook_olap.execute(olap_query, oltp_rows)


def load_initial_impressions_to_olap(**kwargs):

	airflow_hook_olap = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000
	)

	olap_query = '''
		INSERT INTO clickhouse.advertiser_campaigns_impressions (campaign_impressions_count_daily, campaign_impressions_date, rtm_advertiser_id, rtm_campaign_id, rtm_delta)
		VALUES
	'''	

	oltp_data = kwargs['ti'].xcom_pull(task_ids = 'et_initial_impressions_oltp', key = 'oltp_data')

	oltp_rows = [tuple(item[:17]) for item in oltp_data]

	airflow_hook_olap.execute(olap_query, oltp_rows)

def load_initial_clicks_to_olap(**kwargs):

	airflow_hook_olap = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000
	)

	olap_query = '''
		INSERT INTO clickhouse.advertiser_campaigns_clicks (campaign_clicks_count_daily, campaign_clicks_date, rtm_advertiser_id, rtm_campaign_id, rtm_delta)
		VALUES
	'''

	oltp_data = kwargs['ti'].xcom_pull(task_ids = 'et_initial_clicks_oltp', key = 'oltp_data')

	oltp_rows = [tuple(item[:17]) for item in oltp_data]

	airflow_hook_olap.execute(olap_query, oltp_rows)

def generate_totals_report_olap(**kwargs):

	airflow_hook_olap = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000
	)

	create_report_table_query = '''
		CREATE TABLE IF NOT EXISTS clickhouse.advertiser_campaigns_totals_report (
				advertiser_name String,
				campaign_name String,
				campaign_bid Float64,
				campaign_bids_max Float64,
				campaign_bids_remaining Int32,
				campaign_budget Float64,
				campaign_budget_used Float64,
				campaign_budget_status String,
				campaign_budget_daily_norm Float64,
				campaign_budget_daily_used Float64,
				campaign_budget_daily_status String,
				campaign_impressions_count_total Int32,
				campaign_clicks_count_total Int32,
				campaign_ctr Float64,
				campaign_cpm Float64,
				campaign_start_date Date,
				campaign_end_date Date,
				campaign_period_days Int32,
				campaign_period_days_elapsed Int32,
				campaign_days_remaining Int32,
				campaign_period_status String,
				rtm_advertiser_id Int32,
				rtm_campaign_id Int32,
				rtm_timestamp_datetime DateTime
			) ENGINE = MergeTree()
			ORDER BY (rtm_advertiser_id, rtm_campaign_id);
	'''
	airflow_hook_olap.execute(create_report_table_query)

	transform_intial_to_totals_report = '''
		WITH advertiser_campaigns AS
		(
			SELECT
				advertiser_name,
				campaign_name,
				campaign_bid,
				FLOOR(campaign_budget / campaign_bid) AS campaign_bids_max,
				FLOOR(campaign_budget / campaign_bid) - campaign_clicks_count_total AS campaign_bids_remaining,
				campaign_budget,
				(campaign_bid * campaign_clicks_count_total) AS campaign_budget_used,
				(CASE WHEN campaign_budget < (campaign_bid * campaign_clicks_count_total) THEN 'exceeded'
					WHEN 0 = (campaign_bid * campaign_clicks_count_total) THEN 'unused'
					WHEN campaign_budget > (campaign_bid * campaign_clicks_count_total) THEN 'unspent'
					ELSE NULL END) AS campaign_budget_status,
				ROUND((campaign_budget / dateDiff('day', campaign_start_date, campaign_end_date)), 2) AS campaign_budget_daily_norm,
				ROUND((campaign_bid * campaign_clicks_count_total) / dateDiff('day', campaign_start_date, campaign_end_date), 2) AS campaign_budget_daily_used,
				(CASE WHEN ROUND((campaign_budget / dateDiff('day', campaign_start_date, campaign_end_date)), 2) > ROUND((campaign_bid * campaign_clicks_count_total) / dateDiff('day', campaign_start_date, campaign_end_date), 2) THEN 'below_norm'
					WHEN ROUND((campaign_budget / dateDiff('day', campaign_start_date, campaign_end_date)), 2) < ROUND((campaign_bid * campaign_clicks_count_total) / dateDiff('day', campaign_start_date, campaign_end_date), 2) THEN 'above_norm'
					ELSE NULL END) AS campaign_budget_daily_status,
				campaign_impressions_count_total,
				campaign_clicks_count_total,
				((campaign_clicks_count_total / campaign_impressions_count_total) * 100) AS campaign_ctr,
				(campaign_bid * campaign_clicks_count_total) / campaign_impressions_count_total AS campaign_cpm,
				CAST(campaign_start_date AS DATE) AS campaign_start_date,
				CAST(campaign_end_date AS DATE) AS campaign_end_date,
				dateDiff('day', campaign_start_date, campaign_end_date) AS campaign_period_days,
				dateDiff('day', campaign_start_date, today()) AS campaign_period_days_elapsed,
				dateDiff('day', today(), campaign_end_date) AS campaign_days_remaining,
				(CASE WHEN dateDiff('day', today(), campaign_end_date) = 0 THEN 'ended'
					WHEN dateDiff('day', today(), campaign_end_date) > 0 THEN 'ongoing'
					WHEN dateDiff('day', today(), campaign_end_date) < 0 THEN 'exceeded'
					ELSE NULL END) AS campaign_period_status,
				rtm_advertiser_id,
				rtm_campaign_id,
				now() AS rtm_timestamp_datetime
			FROM clickhouse.advertiser_campaigns
		)
		INSERT INTO clickhouse.advertiser_campaigns_totals_report
		SELECT
			advertiser_name,
			campaign_name,
			campaign_bid,
			campaign_bids_max,
			campaign_bids_remaining,
			campaign_budget,
			campaign_budget_used,
			campaign_budget_status,
			campaign_budget_daily_norm,
			campaign_budget_daily_used,
			campaign_budget_daily_status,
			campaign_impressions_count_total,
			campaign_clicks_count_total,
			campaign_ctr,
			campaign_cpm,
			campaign_start_date,
			campaign_end_date,
			campaign_period_days,
			campaign_period_days_elapsed,
			campaign_days_remaining,
			campaign_period_status,
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_timestamp_datetime
		FROM advertiser_campaigns
	'''
	airflow_hook_olap.execute(transform_intial_to_totals_report)

def generate_daily_ctr_report_olap(**kwargs):

	airflow_hook_olap = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000
	)

	create_report_table_query = '''
		CREATE TABLE IF NOT EXISTS clickhouse.advertiser_campaigns_daily_ctr_report (
				advertiser_name	String,
				campaign_name String,
				campaign_day UInt64,
				campaign_impressions_date Date,
				campaign_impressions_count_daily Int32,
				campaign_clicks_count_daily Int32,
				campaign_ctr_daily Float64,
				rtm_advertiser_id Int32,
				rtm_campaign_id Int32
			) 
			ENGINE = MergeTree()
			ORDER BY (rtm_advertiser_id, rtm_campaign_id, campaign_day);
	'''
	airflow_hook_olap.execute(create_report_table_query)

	transform_intial_to_daily_ctr_report = '''
		WITH AdvertisersCampaigns AS
		(
			SELECT
				DISTINCT clickhouse.advertiser_campaigns.rtm_advertiser_id,
				clickhouse.advertiser_campaigns.rtm_campaign_id,
				clickhouse.advertiser_campaigns.advertiser_name,
				clickhouse.advertiser_campaigns.campaign_name
			FROM clickhouse.advertiser_campaigns
		),
		ImpressionsClicksDaily AS
		(
			SELECT
				AdvertisersCampaigns.advertiser_name,
				AdvertisersCampaigns.campaign_name,
				row_number() OVER (PARTITION BY advertiser_campaigns_impressions.rtm_advertiser_id, advertiser_campaigns_impressions.rtm_campaign_id ORDER BY advertiser_campaigns_impressions.campaign_impressions_date ASC) AS campaign_day,
				clickhouse.advertiser_campaigns_impressions.campaign_impressions_date,
				clickhouse.advertiser_campaigns_impressions.campaign_impressions_count_daily,
				clickhouse.advertiser_campaigns_clicks.campaign_clicks_count_daily,
				clickhouse.advertiser_campaigns_impressions.rtm_advertiser_id AS rtm_advertiser_id,
				clickhouse.advertiser_campaigns_impressions.rtm_campaign_id AS rtm_campaign_id
			FROM clickhouse.advertiser_campaigns_impressions
			LEFT JOIN AdvertisersCampaigns ON AdvertisersCampaigns.rtm_advertiser_id = clickhouse.advertiser_campaigns_impressions.rtm_advertiser_id AND AdvertisersCampaigns.rtm_campaign_id = clickhouse.advertiser_campaigns_impressions.rtm_campaign_id
			LEFT JOIN clickhouse.advertiser_campaigns_clicks ON clickhouse.advertiser_campaigns_clicks.rtm_advertiser_id = clickhouse.advertiser_campaigns_impressions.rtm_advertiser_id AND clickhouse.advertiser_campaigns_clicks.rtm_campaign_id = clickhouse.advertiser_campaigns_impressions.rtm_campaign_id AND clickhouse.advertiser_campaigns_clicks.campaign_clicks_date = clickhouse.advertiser_campaigns_impressions.campaign_impressions_date
		)
		INSERT INTO clickhouse.advertiser_campaigns_daily_ctr_report
		SELECT
			advertiser_name,
			campaign_name,
			campaign_day,
			campaign_impressions_date,
			campaign_impressions_count_daily,
			campaign_clicks_count_daily,
			ROUND(((campaign_clicks_count_daily / campaign_impressions_count_daily) * 100), 2) AS campaign_ctr_daily,
			rtm_advertiser_id,
			rtm_campaign_id
		FROM ImpressionsClicksDaily
		ORDER BY rtm_advertiser_id, rtm_campaign_id, campaign_day
	'''
	airflow_hook_olap.execute(transform_intial_to_daily_ctr_report)

## Set Default Arguments
def_args = {
	'owner': 'rtm',
	'start_date': now, # datetime(now.year, now.month, now.day, 0, 0)
	'retries': 1,
	'retry_delay': timedelta(seconds = 30)
}

## Set DAG
initial_load = DAG(
	dag_id = 'initial_load',
	default_args = def_args,
	description = 'Generates, extracts, transforms and loads the initial load to ClickHouse',
	schedule_interval = None,
	catchup = False,
	is_paused_upon_creation = False
)

## Set Tasks
check_initial_load = BranchPythonOperator(
	task_id = 'check_initial_load',
	provide_context = True,
	python_callable = query_postres_tables,
	dag = initial_load
)

generate_initial_load = BashOperator(
	task_id = 'generate_initial_load',
	bash_command = 'python /opt/airflow/scripts/main.py batch --advertisers 5 --campaigns 3 --impressions 1000 --ctr 0.08',
	dag = initial_load
)

create_olap_schema = PythonOperator(
	task_id = 'create_olap_schema',
	python_callable = create_clickhouse_schema,
	provide_context = True,
	dag = initial_load
)

et_initial_totals_oltp = PythonOperator(
	task_id = 'et_initial_totals_oltp',
	python_callable = extract_transform_initial_totals_from_oltp,
	provide_context = True,
	dag = initial_load
)

l_initial_totals_olap = PythonOperator(
	task_id = 'l_initial_totals_olap',
	python_callable = load_initial_totals_to_olap,
	provide_context = True,
	dag = initial_load
)

et_initial_impressions_oltp = PythonOperator(
	task_id = 'et_initial_impressions_oltp',
	python_callable = extract_transform_initial_impressions_from_oltp,
	provide_context = True,
	dag = initial_load
)

l_initial_impressions_olap = PythonOperator(
	task_id = 'l_initial_impressions_olap',
	python_callable = load_initial_impressions_to_olap,
	provide_context = True,
	dag = initial_load
)

et_initial_clicks_oltp = PythonOperator(
	task_id = 'et_initial_clicks_oltp',
	python_callable = extract_transform_initial_clicks_from_oltp,
	provide_context = True,
	dag = initial_load
)

l_initial_clicks_olap = PythonOperator(
	task_id = 'l_initial_clicks_olap',
	python_callable = load_initial_clicks_to_olap,
	provide_context = True,
	dag = initial_load
)

create_campaign_totals_report = PythonOperator(
	task_id = 'create_campaign_totals_report',
	python_callable = generate_totals_report_olap,
	provide_context = True,
	dag = initial_load
)

create_campaign_daily_ctr_report = PythonOperator(
	task_id = 'create_campaign_daily_ctr_report',
	python_callable = generate_daily_ctr_report_olap,
	provide_context = True,
	dag = initial_load
)

check_initial_load >> [generate_initial_load, create_olap_schema]
generate_initial_load >> create_olap_schema
create_olap_schema >> et_initial_totals_oltp >> l_initial_totals_olap >> et_initial_impressions_oltp >> l_initial_impressions_olap >> et_initial_clicks_oltp >> l_initial_clicks_olap >> create_campaign_totals_report >> create_campaign_daily_ctr_report