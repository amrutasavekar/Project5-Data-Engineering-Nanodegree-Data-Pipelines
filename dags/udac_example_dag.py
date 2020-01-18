from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               DataQualityOperator,CreateTableOperator,
                               LoadDimensionOperator)
from helpers import SqlQueries
start_date = datetime.utcnow()

default_args = {
    'owner': 'Amruta Savekar',
    'start_date': datetime(2020, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag_name='Udacity_Airflow_DEND_Amruta'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
create_tables = CreateTableOperator(
    task_id='create_tables_in_redshift',
    dag=dag,
    redshift_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    filetype="JSON",
)
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    filetype="JSON",
)


load_songplays_table = LoadFactOperator(
    task_id='songplays_table_insert',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    provide_context=True,
    query=SqlQueries.songplay_table_insert,
    mode='append'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='users_table_insert',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    provide_context=True,
    query=SqlQueries.user_table_insert,
    mode="append"
)
load_song_dimension_table = LoadDimensionOperator(
    task_id='songs_table_insert',
    dag=dag, table='songs',
    redshift_conn_id="redshift",
    provide_context=True,
    query=SqlQueries.song_table_insert,
    mode='truncate'
)
load_artist_dimension_table = LoadDimensionOperator(
    task_id='artists_table_insert',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    provide_context=True,
    query=SqlQueries.artist_table_insert,
    mode='append'
)
load_time_dimension_table = LoadDimensionOperator(
    task_id='time_table_insert',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    provide_context=True,
    query=SqlQueries.time_table_insert,
    mode='append'
)

run_quality_checks = DataQualityOperator(
    task_id='data_quality_checks_on_tables',
    dag=dag,
    provide_context=True,
    tables=["users", "songs", "songplays", "artists", "time"])

stop_operator = DummyOperator(task_id='End_execution',  dag=dag)

start_operator >> create_tables 
create_tables >> stage_songs_to_redshift
create_tables >> stage_events_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift  >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> stop_operator
