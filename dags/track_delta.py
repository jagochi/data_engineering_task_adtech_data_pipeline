from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client #from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from datetime import date, datetime, timedelta

## Set Properties

## Python Task Helper Functions
def set_datime_no_tz(datetime_value):
	
	if datetime_value is None:

		return datetime.utcnow()

	if isinstance(datetime_value, datetime):
		
		if datetime_value.tzinfo is not None:
			datetime_value = datetime_value.replace(tzinfo = None)
		
		return datetime_value

	elif isinstance(datetime_value, date):

		return datetime(datetime_value.year, datetime_value.month, datetime_value.day, 0, 0, 0, 0)

	return datetime_value

## Python Task Functions
def get_delta(**kwargs):

	oltp_hook = PostgresHook(
		postgres_conn_id = 'postgres_default',
		host = 'postgres',
		schema = 'postgres',
		login = 'postgres',
		password = 'postgres',
		port = 5432
	)

	oltp_delta_table = '''
		WITH Campaigns AS
		(
			SELECT
				advertiser.id AS rtm_advertiser_id,
				advertiser.updated_at AS rtm_advertiser_gmt_modified,
				campaign.id AS rtm_campaign_id,
				campaign.updated_at AS rtm_campaign_modified
			FROM campaign
			LEFT JOIN advertiser ON advertiser.id = campaign.advertiser_id
		),
		Impressions AS
		(
			SELECT
				impressions.campaign_id,
				MAX(impressions.created_at) AS rtm_impressions_gmt_created
			FROM impressions
			GROUP BY campaign_id
		),
		Clicks AS
		(
			SELECT
				clicks.campaign_id,
				MAX(clicks.created_at) AS rtm_clicks_gmt_created
			FROM clicks
			GROUP BY campaign_id
		),
		RTMDeltas AS
		(
			SELECT
				Campaigns.rtm_advertiser_id,
				Campaigns.rtm_advertiser_gmt_modified,
				Campaigns.rtm_campaign_id,
				Campaigns.rtm_campaign_modified,
				Impressions.rtm_impressions_gmt_created,
				Clicks.rtm_clicks_gmt_created,
				GREATEST(Campaigns.rtm_advertiser_gmt_modified, Campaigns.rtm_campaign_modified, Impressions.rtm_impressions_gmt_created, Clicks.rtm_clicks_gmt_created) AS rtm_delta
			FROM Campaigns
			LEFT JOIN Impressions ON Impressions.campaign_id = Campaigns.rtm_campaign_id
			LEFT JOIN Clicks ON Clicks.campaign_id = Campaigns.rtm_campaign_id
		)
		SELECT
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_delta
		FROM RTMDeltas
	'''

	#oltp_delta_table_records = oltp_hook.get_records(oltp_delta_table)

	oltp_connection = oltp_hook.get_conn()
	oltp_cursor = oltp_connection.cursor()
	oltp_cursor.execute(oltp_delta_table)
	oltp_delta_table_records = oltp_cursor.fetchall()
	oltp_delta_table_records = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_delta_table_records]

	oltp_columns_positions = [column_description[0] for column_description in oltp_cursor.description]
	oltp_rtm_advertiser_id_position = oltp_columns_positions.index('rtm_advertiser_id')
	oltp_rtm_campaign_id_position = oltp_columns_positions.index('rtm_campaign_id')
	oltp_rtm_delta_position = oltp_columns_positions.index('rtm_delta')

	oltp_delta_dict = {(row[oltp_rtm_advertiser_id_position], row[oltp_rtm_campaign_id_position], row[oltp_rtm_delta_position]) for row in oltp_delta_table_records}

	olap_hook = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000 # 9000
	)

	olap_delta_table = '''
		SELECT
			clickhouse.advertiser_campaigns.rtm_advertiser_id,
			clickhouse.advertiser_campaigns.rtm_campaign_id,
			clickhouse.advertiser_campaigns.rtm_delta
		FROM clickhouse.advertiser_campaigns
	'''
	olap_delta_table_records = olap_hook.execute(olap_delta_table)

	olap_delta_dict = set(olap_delta_table_records)

	delta_dict = oltp_delta_dict - olap_delta_dict

	if delta_dict:

		sql_value_placeholders = ','.join(['(%s,%s,%s)'] * len(delta_dict))
		sql_values = [set_datime_no_tz(column_value) for row_tuple in delta_dict for column_value in row_tuple]

		oltp_get_upsert_table = f'''
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
					COALESCE(ImpressionsTotal.campaign_impressions_count_total, 0) AS campaign_impressions_count_total,
					COALESCE(ClicksTotal.campaign_clicks_count_total, 0) AS campaign_clicks_count_total,
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
			),
			Delta (rtm_advertiser_id, rtm_campaign_id, rtm_delta) AS
			(
				VALUES {sql_value_placeholders}
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
			INNER JOIN Delta USING (rtm_advertiser_id, rtm_campaign_id, rtm_delta)
		'''
		oltp_upsert_data = oltp_hook.get_records(oltp_get_upsert_table, sql_values)
		oltp_upsert_data = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_upsert_data]

		olap_upsert_query = '''
			INSERT INTO clickhouse.advertiser_campaigns (advertiser_name, campaign_name, campaign_bid, campaign_budget, campaign_impressions_count_total, campaign_clicks_count_total, campaign_start_date, campaign_end_date, rtm_advertiser_id, rtm_advertiser_gmt_created, rtm_advertiser_gmt_modified, rtm_campaign_id, rtm_campaign_gmt_created, rtm_campaign_modified, rtm_impressions_gmt_created, rtm_clicks_gmt_created, rtm_delta)
			VALUES
		'''
		olap_hook.execute(olap_upsert_query, oltp_upsert_data)

def get_delta_impressions(**kwargs):

	oltp_hook = PostgresHook(
		postgres_conn_id = 'postgres_default',
		host = 'postgres',
		schema = 'postgres',
		login = 'postgres',
		password = 'postgres',
		port = 5432
	)

	oltp_delta_table = '''
		WITH ImpressionsDaily AS
		(
			SELECT
				campaign.advertiser_id AS rtm_advertiser_id,
				impressions.campaign_id AS rtm_campaign_id,
				MAX(impressions.created_at) AS rtm_delta
			FROM impressions
			LEFT JOIN campaign ON campaign.id = impressions.campaign_id
			GROUP BY campaign.advertiser_id, impressions.campaign_id, CAST(impressions.created_at AS DATE)
		)
		SELECT
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_delta
		FROM ImpressionsDaily
		ORDER BY rtm_advertiser_id, rtm_campaign_id
	'''

	oltp_connection = oltp_hook.get_conn()
	oltp_cursor = oltp_connection.cursor()
	oltp_cursor.execute(oltp_delta_table)
	oltp_delta_table_records = oltp_cursor.fetchall()
	oltp_delta_table_records = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_delta_table_records]

	oltp_columns_positions = [column_description[0] for column_description in oltp_cursor.description]
	oltp_rtm_advertiser_id_position = oltp_columns_positions.index('rtm_advertiser_id')
	oltp_rtm_campaign_id_position = oltp_columns_positions.index('rtm_campaign_id')
	oltp_rtm_delta_position = oltp_columns_positions.index('rtm_delta')

	oltp_delta_dict = {(row[oltp_rtm_advertiser_id_position], row[oltp_rtm_campaign_id_position], row[oltp_rtm_delta_position]) for row in oltp_delta_table_records}

	olap_hook = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000 # 9000
	)

	olap_delta_table = '''
		SELECT
			clickhouse.advertiser_campaigns_impressions.rtm_advertiser_id,
			clickhouse.advertiser_campaigns_impressions.rtm_campaign_id,
			clickhouse.advertiser_campaigns_impressions.rtm_delta
		FROM clickhouse.advertiser_campaigns_impressions
	'''
	olap_delta_table_records = olap_hook.execute(olap_delta_table)

	olap_delta_dict = set(olap_delta_table_records)

	delta_dict = oltp_delta_dict - olap_delta_dict

	sql_value_placeholders = ','.join(['(%s,%s,%s)'] * len(delta_dict))
	sql_values = [set_datime_no_tz(column_value) for row_tuple in delta_dict for column_value in row_tuple]

	oltp_get_upsert_table = f'''
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
		),
		Delta (rtm_advertiser_id, rtm_campaign_id, rtm_delta) AS
		(
			VALUES {sql_value_placeholders}
		)
		SELECT
			campaign_impressions_count_daily,
			campaign_impressions_date,
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_delta
		FROM ImpressionsDaily
		INNER JOIN Delta USING (rtm_advertiser_id, rtm_campaign_id, rtm_delta)
		ORDER BY rtm_advertiser_id, rtm_campaign_id, campaign_impressions_date
	'''
	oltp_upsert_data = oltp_hook.get_records(oltp_get_upsert_table, sql_values)
	oltp_upsert_data = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_upsert_data]

	olap_upsert_query = '''
		INSERT INTO clickhouse.advertiser_campaigns_impressions (campaign_impressions_count_daily, campaign_impressions_date, rtm_advertiser_id, rtm_campaign_id, rtm_delta)
		VALUES
	'''
	olap_hook.execute(olap_upsert_query, oltp_upsert_data)

def get_delta_clicks(**kwargs):

	oltp_hook = PostgresHook(
		postgres_conn_id = 'postgres_default',
		host = 'postgres',
		schema = 'postgres',
		login = 'postgres',
		password = 'postgres',
		port = 5432
	)

	oltp_delta_table = '''
		WITH ClicksDaily AS
		(
			SELECT
				campaign.advertiser_id AS rtm_advertiser_id,
				clicks.campaign_id AS rtm_campaign_id,
				MAX(clicks.created_at) AS rtm_delta
			FROM clicks
			LEFT JOIN campaign ON campaign.id = clicks.campaign_id
			GROUP BY campaign.advertiser_id, clicks.campaign_id, CAST(clicks.created_at AS DATE)
		)
		SELECT
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_delta
		FROM ClicksDaily
		ORDER BY rtm_advertiser_id, rtm_campaign_id
	'''

	oltp_connection = oltp_hook.get_conn()
	oltp_cursor = oltp_connection.cursor()
	oltp_cursor.execute(oltp_delta_table)
	oltp_delta_table_records = oltp_cursor.fetchall()
	oltp_delta_table_records = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_delta_table_records]

	oltp_columns_positions = [column_description[0] for column_description in oltp_cursor.description]
	oltp_rtm_advertiser_id_position = oltp_columns_positions.index('rtm_advertiser_id')
	oltp_rtm_campaign_id_position = oltp_columns_positions.index('rtm_campaign_id')
	oltp_rtm_delta_position = oltp_columns_positions.index('rtm_delta')

	oltp_delta_dict = {(row[oltp_rtm_advertiser_id_position], row[oltp_rtm_campaign_id_position], row[oltp_rtm_delta_position]) for row in oltp_delta_table_records}

	olap_hook = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000 # 9000
	)

	olap_delta_table = '''
		SELECT
			clickhouse.advertiser_campaigns_clicks.rtm_advertiser_id,
			clickhouse.advertiser_campaigns_clicks.rtm_campaign_id,
			clickhouse.advertiser_campaigns_clicks.rtm_delta
		FROM clickhouse.advertiser_campaigns_clicks
	'''
	olap_delta_table_records = olap_hook.execute(olap_delta_table)

	olap_delta_dict = set(olap_delta_table_records)

	delta_dict = oltp_delta_dict - olap_delta_dict

	sql_value_placeholders = ','.join(['(%s,%s,%s)'] * len(delta_dict))
	sql_values = [set_datime_no_tz(column_value) for row_tuple in delta_dict for column_value in row_tuple]

	oltp_get_upsert_table = f'''
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
		),
		Delta (rtm_advertiser_id, rtm_campaign_id, rtm_delta) AS
		(
			VALUES {sql_value_placeholders}
		)
		SELECT
			campaign_clicks_count_daily,
			campaign_clicks_date,
			rtm_advertiser_id,
			rtm_campaign_id,
			rtm_delta
		FROM ClicksDaily
		INNER JOIN Delta USING (rtm_advertiser_id, rtm_campaign_id, rtm_delta)
		ORDER BY rtm_advertiser_id, rtm_campaign_id, campaign_clicks_date
	'''
	oltp_upsert_data = oltp_hook.get_records(oltp_get_upsert_table, sql_values)
	oltp_upsert_data = [tuple(set_datime_no_tz(column) for column in row) for row in oltp_upsert_data]

	olap_upsert_query = '''
		INSERT INTO clickhouse.advertiser_campaigns_clicks (campaign_clicks_count_daily, campaign_clicks_date, rtm_advertiser_id, rtm_campaign_id, rtm_delta)
		VALUES
	'''
	olap_hook.execute(olap_upsert_query, oltp_upsert_data)

def generate_totals_report_olap(**kwargs):

	airflow_hook_olap = Client(
		host = 'clickhouse',
		user = 'clickhouse',
		password = 'clickhouse',
		port = 9000
	)

	airflow_hook_olap.execute('''DROP TABLE IF EXISTS clickhouse.advertiser_campaigns_totals_report;''')

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

	airflow_hook_olap.execute('''DROP TABLE IF EXISTS clickhouse.advertiser_campaigns_daily_ctr_report;''')

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
	'start_date': days_ago(1), # datetime(now.year, now.month, now.day, 0, 0)
	'retries': 1,
	'retry_delay': timedelta(seconds = 30)
}

## Set DAG
track_deltas = DAG(
	dag_id = 'track_deltas',
	default_args = def_args,
	description = 'Merges existing and inserts new data to main tables from OLTP to OLAP.',
	schedule_interval = timedelta(minutes = 5),
	catchup = False,
	is_paused_upon_creation = False
)

## Set Tasks
delta_advertiser_campaigns = PythonOperator(
	task_id = 'delta_advertiser_campaigns',
	python_callable = get_delta,
	provide_context = True,
	dag = track_deltas
)

delta_impressions = PythonOperator(
	task_id = 'delta_impressions',
	python_callable = get_delta_impressions,
	provide_context = True,
	dag = track_deltas
)

delta_clicks = PythonOperator(
	task_id = 'delta_clicks',
	python_callable = get_delta_clicks,
	provide_context = True,
	dag = track_deltas
)

create_campaign_totals_report = PythonOperator(
	task_id = 'create_campaign_totals_report',
	python_callable = generate_totals_report_olap,
	provide_context = True,
	dag = track_deltas
)

create_campaign_daily_ctr_report = PythonOperator(
	task_id = 'create_campaign_daily_ctr_report',
	python_callable = generate_daily_ctr_report_olap,
	provide_context = True,
	dag = track_deltas
)

delta_advertiser_campaigns >> delta_impressions >> delta_clicks
delta_clicks >> create_campaign_totals_report
delta_clicks >> create_campaign_daily_ctr_report