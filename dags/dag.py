
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.pull_weather_data import BigQueryToS3Operator
from operators.pull_crime_data import SocrataToS3Operator
from operators.data_quality import DataQualityOperator
from operators.s3_to_staging_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from helpers.sql_queries import SqlQueries
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2001, 1, 1),
    'depends_on_past': True,
    'email_on_retry': False,
    'catchup': True
}

dag_dag = DAG('udac_example_dag',
          default_args=default_args,
          max_active_runs= 3,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 1 * *'
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag_dag)


pull_crime_data = SocrataToS3Operator(
                 dag = dag_dag,
                 task_id = "pull_crime_data_task",
                 aws_credentials_id="aws_connection",
                 folder="crime",
                 provide_context=True
)

pull_weather_data = BigQueryToS3Operator(
                 dag = dag_dag,
                 task_id = "pull_weather_data_task",
                 aws_credentials_id="aws_connection",
                 folder="weather",
                 provide_context=True
)

s3_to_redshift_task_weather = StageToRedshiftOperator(
                 dag = dag_dag,
                 task_id = "s3_to_redshift_task_weather",
                 aws_credentials_id="aws_connection",
                 redshift_conn_id="redshift_conn",
                 source_key = "weather",
                 target_table="staging_weather",
                 s3_bucket="udacitycapstoneprojectbucket",

)
s3_to_redshift_task_crime = StageToRedshiftOperator(
                 dag=dag_dag,
                 task_id="s3_to_redshift_task_crime",
                 aws_credentials_id="aws_connection",
                 redshift_conn_id="redshift_conn",
                 source_key="crime",
                 target_table="staging_crimes",
                 s3_bucket="udacitycapstoneprojectbucket",
)

load_crime_weather_fact_table_task = LoadFactOperator(
    task_id='load_crime_weather_fact_table',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    target_table='fact_daily_crime_weather',
    sql=SqlQueries.fact_daily_crime_weather_insert
)

load_dimension_crime_table_task = LoadDimensionOperator(
    task_id='load_dimension_crime_table',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    target_table='crime',
    sql=SqlQueries.dim_table_crime_insert
)

load_dimension_crime_location_table_task = LoadDimensionOperator(
    task_id='load_dimension_crime_location_table',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    target_table='crime_location',
    sql=SqlQueries.dim_table_crime_location_insert
)
load_dimension_crime_arrest_table_task = LoadDimensionOperator(
    task_id='load_dimension_crime_arrest_table',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    target_table='crime_arrest',
    sql=SqlQueries.dim_table_crime_arrest_insert
)
load_dimension_crime_domestic_table_task = LoadDimensionOperator(
    task_id='load_dimension_crime_domestic_table',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    target_table='crime_domestic',
    sql=SqlQueries.dim_table_crime_domestic_insert
)

load_dimension_time_table_task = LoadDimensionOperator(
    task_id='load_dimension_crime_time_table',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    target_table='time',
    sql=SqlQueries.dim_table_time_insert
)

load_dimension_daily_weather_table_task = LoadDimensionOperator(
    task_id='load_dimension_daily_weather_table',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    target_table='daily_weather',
    sql=SqlQueries.dim_table_daily_weather_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag_dag,
    redshift_conn_id='redshift_conn',
    dq_checks=[
            {'check_sql': "SELECT COUNT(*) FROM crime WHERE primary_type IS NULL", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM crime_location WHERE block IS NULL", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM crime_arrest", 'expected_result': 2},
            {'check_sql': "SELECT COUNT(*) FROM crime_domestic", 'expected_result': 2},
            {'check_sql': "SELECT COUNT(*) FROM time WHERE crime_time IS NULL", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM daily_weather WHERE id IS NULL", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag_dag)


start_operator >> pull_weather_data
start_operator >> pull_crime_data

pull_weather_data >> s3_to_redshift_task_crime
pull_weather_data >> s3_to_redshift_task_weather
pull_crime_data >> s3_to_redshift_task_crime
pull_crime_data >> s3_to_redshift_task_weather

s3_to_redshift_task_crime >> load_crime_weather_fact_table_task
s3_to_redshift_task_weather >> load_crime_weather_fact_table_task

load_crime_weather_fact_table_task >> load_dimension_crime_table_task
load_crime_weather_fact_table_task >> load_dimension_crime_location_table_task
load_crime_weather_fact_table_task >> load_dimension_crime_arrest_table_task
load_crime_weather_fact_table_task >> load_dimension_crime_domestic_table_task
load_crime_weather_fact_table_task >> load_dimension_time_table_task
load_crime_weather_fact_table_task >> load_dimension_daily_weather_table_task

load_dimension_crime_table_task >> run_quality_checks
load_dimension_crime_location_table_task >> run_quality_checks
load_dimension_crime_arrest_table_task >> run_quality_checks
load_dimension_crime_domestic_table_task >> run_quality_checks
load_dimension_time_table_task >> run_quality_checks
load_dimension_daily_weather_table_task >> run_quality_checks

run_quality_checks >> end_operator