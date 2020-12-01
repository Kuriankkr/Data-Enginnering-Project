from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'kurian',
    'start_date': datetime(2020,1,12),
    'depends_on_past':False,
    'retries':1,
    'catchup':False,
    'retry_delay':timedelta(seconds=300),
    'email_on_retry':False,  
    'max_active_runs':1
}
    
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#create_redshift_tables = CreateTablesOperator(
#    task_id='Create_tables',
#    dag=dag,
#    redshift_conn_id="redshift"
#)


create_tables_task = PostgresOperator(
task_id="create_tables",
dag=dag,
sql='create_tables.sql',
postgres_conn_id="redshift"
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context = True,
    table = "public.staging_events",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    region="us-west-2",
    json = "s3://udacity-dend/log_json_path.json" 
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = 'public.staging_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    region="us-west-2",
    json = "auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="public.songplays",
    truncate_table = True,
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = "public.users",
    truncate_table = True,
    query=SqlQueries.user_table_insert 
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = "public.songs",
    truncate_table = True,
    query=SqlQueries.song_table_insert 
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = "artists",
    truncate_table = True,
    query=SqlQueries.artist_table_insert 
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = "time",
    truncate_table = True,
    query=SqlQueries.time_table_insert 
)

dq_checks=[{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
           {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=dq_checks
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task >>  [stage_events_to_redshift, stage_songs_to_redshift] >> \
load_songplays_table>>[ load_user_dimension_table,
                       load_song_dimension_table,
                       load_artist_dimension_table,
                       load_time_dimension_table] \
>> run_quality_checks >> end_operator
