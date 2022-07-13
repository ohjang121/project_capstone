from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (PqStageToRedshiftOperator, DataQualityOperator)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# DAG default args

default_args = {
    'owner': 'joh',
    'start_date': datetime(2022, 7, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

# S3 Variables

S3_BUCKET = 'udacity-capstone-joh/production'
S3_PREFIX_IMMIGRATION = 'fact_immigration'
S3_PREFIX_IMMIGRANT = 'dim_immigrant'
S3_PREFIX_TEMPERATURE = 'dim_temperature'
S3_PREFIX_DEMOGRAPHICS = 'dim_demographics'

# Python script paths for BashOperators

SPARK_ETL_PATH = os.path.join(os.getcwd(), 'immigration_spark_etl.py')
AWS_SETUP_PATH = os.path.join(os.getcwd(), 'aws_setup.py')

# Data Quality Variables
TABLE_DICT = {'fact_immigration':'immigration_id',
        'dim_immigrant':'immigration_id',
        'dim_temperature':'temp_id',
        'dim_demographics':'demo_id'
        }

dag = DAG('immigration_dag',
          default_args=default_args,
          description='ETL data from S3 using Spark and Redshift on Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


## need operator for spark etl.py & redshift start

spark_etl = BashOperator(
    task_id='Spark_ETL',
    bash_command=f'python3 {SPARK_ETL_PATH}',
    dag=dag
)

aws_setup = BashOperator(
    task_id='AWS_redshift_setup',
    bash_command=f'python3 {AWS_SETUP_PATH}',
    dag=dag
)

create_tables = PostgresOperator(
    task_id='Create_tables',
    postgres_conn_id='redshift_capstone',
    sql='imm_create_tables.sql',
    dag=dag
)

load_fact_immigration_to_redshift = PqStageToRedshiftOperator(
    task_id='Load_fact_immigration',
    table='fact_immigration',
    s3_bucket=S3_BUCKET,
    s3_prefix=S3_PREFIX_IMMIGRATION,
    copy_options='FORMAT AS PARQUET',
    dag=dag
)

load_dim_immigration_to_redshift = PqStageToRedshiftOperator(
    task_id='Load_dim_immigrant',
    table='dim_immigrant',
    s3_bucket=S3_BUCKET,
    s3_prefix=S3_PREFIX_IMMIGRANT,
    copy_options='FORMAT AS PARQUET',
    dag=dag
)

load_dim_temperature_to_redshift = PqStageToRedshiftOperator(
    task_id='Load_dim_temperature',
    table='dim_temperature',
    s3_bucket=S3_BUCKET,
    s3_prefix=S3_PREFIX_TEMPERATURE,
    copy_options='FORMAT AS PARQUET',
    dag=dag
)

load_dim_demographics_to_redshift = PqStageToRedshiftOperator(
    task_id='Load_dim_demographics',
    table='dim_demographics',
    s3_bucket=S3_BUCKET,
    s3_prefix=S3_PREFIX_DEMOGRAPHICS,
    copy_options='FORMAT AS PARQUET',
    dag=dag
)

run_data_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    table_dict=TABLE_DICT,
    dag=dag
)

aws_terminate = BashOperator(
    task_id='AWS_redshift_terminate',
    bash_command=f'python3 {AWS_SETUP_PATH} --delete',
    dag=dag
)

# need operator for redshift quit


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# DAG Dependencies

start_operator >> spark_etl >> aws_setup >> create_tables

create_tables >> load_fact_immigration_to_redshift >> run_data_quality_checks
create_tables >> load_dim_immigration_to_redshift >> run_data_quality_checks
create_tables >> load_dim_temperature_to_redshift >> run_data_quality_checks
create_tables >> load_dim_demographics_to_redshift >> run_data_quality_checks

run_data_quality_checks >> aws_terminate >> end_operator
