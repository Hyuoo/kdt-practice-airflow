from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json

dag = DAG(
    dag_id = 'kdt_mysql_to_redshift',
    start_date = datetime(2022,4,24), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'owner': 'Hyuoo',
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

schema = "hopeace6"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table
# SqlToS3Operator > s3://grepp-data-engineering/hopeace6-nps
# S3ToRedshiftOperator > hopeace6.nps

mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3_nps',
    # 쿼리 결과를 파일로 저장.
    # query = "SELECT * FROM prod.nps",
    query = "SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')",
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    sql_conn_id = "mysql_conn_id",
    aws_conn_id = "aws_conn_id",
    verify = False,
    replace = True, # 덮어쓰기, 안덮쓰면 에러발생가능
    pd_kwargs={"index": False, "header": False}, # sql결과 저장 포맷.
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    schema = schema,
    table = table,
    copy_options=['csv'], # 대상파일
    redshift_conn_id = "redshift_dev_db",
    aws_conn_id = "aws_conn_id",
    # 메서드 REPLACE, APPEND, UPSERT
    # method = 'REPLACE',
    method = "UPSERT",
    # UPSERT일 경우 기준컬럼으로 있으면 REPLACE(UPDATE), 없으면 INSERT
    upsert_keys = ["id"],
    dag = dag
)

mysql_to_s3_nps >> s3_to_redshift_nps