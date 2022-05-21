from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    PostgresOperator,
)
from helpers import SqlQueries, test_cases


# Redshift configs
REDSHIFT_CONNECTION = "redshift"

# S3 configs
AWS_CONNECTION = "aws_credentials"
BUCKET_NAME = "udacity-dend"
REGION = "us-west-2"

# Other configs
CREATE_TABLES_SCRIPT = open("/home/workspace/airflow/create_tables.sql").read()

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    max_active_runs=1,
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_all_tables = PostgresOperator(
    task_id="create_all_tables",
    postgres_conn_id=REDSHIFT_CONNECTION,
    sql=CREATE_TABLES_SCRIPT,
    dag=dag,
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_conn_id=AWS_CONNECTION,
    redshift_conn_id=REDSHIFT_CONNECTION,
    output_table="staging_events",
    bucket_name=BUCKET_NAME,
    s3_key="log_data/{execution_date.year}/{execution_date.month}",
    region=REGION,
    provide_context=True,
    dag=dag,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_conn_id=AWS_CONNECTION,
    redshift_conn_id=REDSHIFT_CONNECTION,
    output_table="staging_songs",
    bucket_name=BUCKET_NAME,
    s3_key="song_data/A/A/A",
    region=REGION,
    provide_context=True,
    truncate=True,
    dag=dag,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id=REDSHIFT_CONNECTION,
    output_table="songplays",
    input_query=SqlQueries.songplay_table_insert,
    dag=dag,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id=REDSHIFT_CONNECTION,
    output_table="users",
    input_query=SqlQueries.user_table_insert,
    truncate=True,
    dag=dag,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id=REDSHIFT_CONNECTION,
    output_table="songs",
    input_query=SqlQueries.song_table_insert,
    truncate=True,
    dag=dag,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id=REDSHIFT_CONNECTION,
    output_table="artists",
    input_query=SqlQueries.artist_table_insert,
    truncate=True,
    dag=dag,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id=REDSHIFT_CONNECTION,
    output_table="time",
    input_query=SqlQueries.time_table_insert,
    truncate=True,
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id=REDSHIFT_CONNECTION,
    test_cases=test_cases,
    dag=dag,
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag,
)

start_operator >> create_all_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks >> end_operator
