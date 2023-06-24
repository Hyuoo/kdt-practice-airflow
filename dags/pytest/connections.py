import psycopg2
def get_Redshift_connection(autocommit=True):
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "***"  # 본인 ID 사용
    redshift_pass = "***"  # 본인 Password 사용
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()


from airflow.providers.postgres.hooks.postgres import PostgresHook
def get_Redshift_connections(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


from airflow.providers.postgres.hooks.postgres import PostgresHook
def get_Redshift_connections_2():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

