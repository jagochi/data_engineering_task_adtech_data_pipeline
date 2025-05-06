# Data Engineering Task: AdTech Data Pipeline

## Overview

The project is based on the Data Engineering Task: AdTech Data Pipeline from the following repository.
https://github.com/retailmediatools/hiring-data-engineer-task/

This is a demo project showing OLTP to OLAP ETL.

## Solution

The solution is based on the following technologies.
1. PostgreSQL database
- OLTP database receiving new transactions
2. FlaWay
- Generates the OLTP schema
3. ClickHouse database
- OLAP database holding reports based on OLTP data
4. Airflow server with DAG pipelines
- provides DAG monitoring
- generates the demo initial load in OLTP using provided shell functions
- generates new data in OLTP simulating new transactions
- generates initial load reports in OLAP database
- provides delta logic with ETL from OLTP to OLAP
5. PGAdmin
- access to PosgreSQL database
6. Tabix
- access to ClickHouse database

## Requirements

* [uv](https://docs.astral.sh/uv/getting-started/installation/)
* [docker](https://docs.docker.com/engine/install/)
* [compose](https://docs.docker.com/compose/install/)

## Setup & Environment

```bash
# Install dependencies
pip install uv
uv sync

# Start all services
docker-compose up --build # first time use
docker-compose up -d

# Wait a bit for services to initialize, before using web services to access query editors and monitoring.
```

## Data Models

OLTP

As per original project:

- **advertiser**: Information about companies running ad campaigns
- **campaign**: Ad campaigns configured with bid amounts and budgets  
- **impressions**: Records of ads being displayed
- **clicks**: Records of users clicking on ads

OLAP

-- **advertiser_campaigns**: Advertisers' campaigns inforamtion.
-- **advertiser_campaigns_impressions**: Impressions aggregated on date.
-- **advertiser_campaigns_clicks**: Clicks aggregated on date.

-- **advertiser_campaigns_totals_report**: Aggregated report. Advertisers' campaigns status information for campaign planning.
-- **advertiser_campaigns_daily_ctr_report**: Aggregated report. Impressions and clicks per day report for campaign monitoring.

## Data Generators

The following generators were included with the original project.
Here, the generators run in an Airflow DAG randomly generating data every few minutes by running one of the four example functions to generate the data.

As per original project:
```bash
# Generate a complete batch of test data
uv run python main.py batch --advertisers 5 --campaigns 3 --impressions 1000 --ctr 0.08
# Add a single advertiser
uv run python main.py advertisers --count 1
# Add campaigns for an advertiser
uv run python main.py campaigns --advertiser-id 1 --count 2
# Add impressions for a campaign
uv run python main.py impressions --campaign-id 1 --count 500
# Add clicks for a campaign (based on existing impressions)
uv run python main.py clicks --campaign-id 1 --ratio 0.12
# View current data statistics
uv run python main.py stats
# Reset all data (use with caution)
uv run python main.py reset
```

## Outputs

In regards to the original project.

1. **ClickHouse Schema**: SQL scripts to create your analytical tables
- part of 'dag/initial_load_generate_postgres_data.py' and 'dag/track_delta.py'
2. **Data Pipeline**: Code and configuration to move data from PostgreSQL to ClickHouse
- Airflow DAGs in the 'dag/' directory
- initialization scripts for the Airflow ('airflow_init', 'airflow_start_services.sh')
3. **KPI Queries**: SQL queries to calculate the following metrics:
- part of 'dag/initial_load_generate_postgres_data.py' and 'dag/track_delta.py'
4. **Documentation**: A README explaining your design decisions and how to run your solution
- the file you're reading

## Pipelines

Airflow contains 3 main DAGs consisting of following tasks.

1. initial_load (runs on Airflow startup)
- check_initial_load: Checks if the tables in OLTP are empty. If empty runs generate_initial_load.
- generate_initial_load: Uses provided shell script to generate the initial load in OLTP.
- create_olap_schema: Create the OLAP schema.
- et_initial_totals_oltp and l_initial_totals_olap: ETL - OLTP initial load to OLAP for totals report.
- et_initial_impressions_oltp and l_initial_impressions_olap: ETL - OLTP initial load to OLAP for daily CTR report.
- et_initial_clicks_oltp and l_initial_clicks_olap: ETL - OLTP initial load to OLAP for daily CTR report.
- create_campaign_totals_report: ETL - generates the totals report table in OLAP.
- create_campaign_daily_ctr_report: ETL - generates the daily CTR report table in OLAP.

2. generate_new_data (runs every 3 minutes and executes a task at random interval between 0-120 seconds)
- select_random_generate_task: Randomly selects one of the other four tasks to run a shell command and generate new data in OLTP.
- generate_delta_load_advertisers: Adds a new advertiser.
- generate_delta_load_campaigns: Adds a new campaign.
- generate_delta_load_impressions: Adds impressions to a campaign.
- generate_delta_load_clicks: Adds clicks to a campaign.

3. track_deltas (runs every 5 minutes)
- delta_advertiser_campaigns: Upserts the delta rows from OLTP to OLAP for totals report.
- delta_impressions: Upserts the delta rows from OLTP to OLAP for daly CTR report.
- delta_clicks: Upserts the delta rows from OLTP to OLAP for daly CTR report.
- create_campaign_totals_report: Recreates the totals report.
- create_campaign_daily_ctr_report: Recreates the totals report.

## OLAP Tables

Main entity tables:
- advertiser_campaigns
- advertiser_campaigns_impressions
- advertiser_campaigns_clicks

Aggreagated reports tables (for passing to a Dashboard):
- advertiser_campaigns_totals_report
- advertiser_campaigns_daily_ctr_report

## Monitoring

Error handling is possible via Airflow Logs and monitoring.
Airflow webservice is available on the following link.
web: http://localhost:8090
credentials: airflow:airflow

## OLTP/OLAP Interface

PostreSQL is accessible via PGAdmin on the following link.
web: http://localhost:5050
db: postgres
port: 5432
creadentials: postgres:postgres

ClickHouse is accessible via Tabis on the following link.
web: http://localhost:8081
server name: postgres
host: http://localhost:8123
credentials: clickhouse:clickhouse

## OTHER

1. airflow_init
- copies pyproject.toml and uv.lock to Airflow
- starts services
- installs dependencies
- sets Python 3.12 path

2. airflow_start_services.sh
- checks if PostgreSQL database is running
- created Airflow database in PostgreSQL
- creates users
- starts scheduler, weberver
- runs the initial_load DAG

## Setup Procedure

Following the original project with additions

1. Clone this repository
2. Install dependencies: `uv sync`
3. Start Docker containers: `docker-compose up -build`
4. Explore the sample data:
   - Command line: `uv run python main.py stats`
   - Web interfaces: pgAdmin and Tabix (see Database Access section)
5. Access Airflow web for monitoring
6. Access PGAdmin to run queries on OLTP
7. Access Tabis to run queries on OLAP

For local development:
- View container status: `docker-compose ps`
- View logs: `docker-compose logs`
- Run services: `docker-compose up -d` 
- Stop services: `docker-compose down -v`
