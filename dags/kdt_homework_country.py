from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from pandas import Timestamp

import requests
import json
import logging
import psycopg2

def get_Redshift_connection(autocommit=True):
    # host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    # redshift_user = "***"  # 본인 ID 사용
    # redshift_pass = "***"  # 본인 Password 사용
    # port = 5439
    # dbname = "dev"
    # conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    # conn.set_session(autocommit=True)
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def get_country_info(url):
    res = requests.get(url)
    countries = json.loads(res.text)
    records = []
    for country in countries:
        records.append([country["name"]["official"].replace("'","''"), country["population"], country["area"]])
    
    logging.info("api-get .. OK")
    return records

@task
def load_to_redshift(schema, table, records):
    cur = get_Redshift_connection()
    try:
        logging.info("start load")
        
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        cur.execute(f"""
        CREATE TABLE {schema}.{table} (
            country varchar(100),
            population integer,
            area float
        );""")
        c=0
        for record in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{record[0]}', {record[1]}, {record[2]});"
            # print(sql)
            cur.execute(sql)
            c+=1
            # print(".. GOOD")
        cur.execute("COMMIT;")
        
        logging.info("end load .. successfully")
        logging.info(f"loaded data count : {c}")
    except Exception as e:
        print(e)
        cur.execute("ROLLBACK;")
        logging.info("end load .. failed")
        raise


with DAG(
    dag_id = 'kdt_country_hw',
    start_date = datetime(2023,6,10),
    catchup = False,
    tags = ["hopeace6"],
    schedule = "30 6 * * 6"
) as dag:

    url = "https://restcountries.com/v3.1/all"
    record = get_country_info(url)
    load_to_redshift("hopeace6", "country_hw", record)
