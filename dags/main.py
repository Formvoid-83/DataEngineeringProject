from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality

#Define localtime
local_tz = pendulum.timezone("America/La_Paz")

# Default Args

default_args ={
    "owner" : "dataengineers",
    "depends_of_past" : False,
    "email_on_failure" : False,
    "email_on_retry" : False,
    "email" : "data@engineers.com",
    "max_active_runs" : 1,
    "dagrun_timeout" : timedelta(hours=1),
    "start_date" : datetime(2026, 8, 14, tzinfo=local_tz),
}

# Variables
staging_schema = "staging"
core_schema = "core"

# 1st DAG: Produce the json
with DAG(
    dag_id= 'produce_json',
    default_args= default_args,
    description= 'DAG to process JSON file',
    schedule= '0 14 * * *',
    catchup= False

) as dag_produce:

    #Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id = "trigger_update_db",
        trigger_dag_id = "update_db"
    )

    #Define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task >> trigger_update_db

# 2nd DAG: Update the db
with DAG(
    dag_id= 'update_db',
    default_args= default_args,
    description= 'DAG to process JSON file and insert data into both staging and core schemas',
    #schedule= '0 15 * * *',
    catchup= False,
    schedule=None

) as dag_update:

    #Define tasks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id = "trigger_data_quality",
        trigger_dag_id = "data_quality"
    )

    #Define dependencies
    update_staging >> update_core >> trigger_data_quality

# 3rd DAG: Data quality
with DAG(
    dag_id= 'data_quality',
    default_args= default_args,
    description= 'DAG to check the data quality on both layers in the db',
    #schedule= '0 16 * * *',
    catchup= False,
    schedule= None
) as dag:

    #Define tasks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)



    #Define dependencies
    soda_validate_staging >> soda_validate_core

