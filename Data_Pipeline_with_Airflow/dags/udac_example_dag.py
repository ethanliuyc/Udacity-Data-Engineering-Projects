from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ethan_lyc',
    'start_date': datetime(2019, 1, 12),
    'email': ['ethan.liuyc@hotmail.com'],
    'email_on_retry': False,
    'retries': 10,
    'email_on_failure': False,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credential_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    region = "us-west-2",
    file_format = "JSON",
    json_path = "s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credential_id = "aws_credentials",
    table = "staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    region = "us-west-2",
    file_format = "JSON",
    json_path = "auto" 
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    load_sql="songplay_table_insert",
    append_data = False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    load_sql="user_table_insert",
    append_data = False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    load_sql="song_table_insert",
    append_data = False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    load_sql="artist_table_insert",
    append_data = False)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    load_sql="time_table_insert",
    append_data = False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["songplays","time","artists","songs","users"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift,stage_songs_to_redshift]

[stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table]

[load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator