from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
from google.cloud import bigquery
from google.cloud import storage
from elt_slack import on_failure_callback, on_success_callback
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['guddn806@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
}

## GCP information
project_id = "job-posting-api-388303"
staging_dataset = 'staging'
dwh_dataset = 'external'
# bucket detail information
gs_bucket_recruit = 'hale-posting-bucket/recruit'
gs_bucket_google_trend = 'hale-posting-bucket/google_trend'
gs_bucket_certification = 'hale-posting-bucket/certification'

# define DAG
with DAG(
        dag_id='GCS_to_Bigquery',
        start_date=datetime(2023, 6, 29),
        catchup=False,
        default_args=default_args,
        description='Data load from GCS to Bigquery',
        schedule_interval='0 */3 * * *'
) as dag:
    gcs_hook = GCSHook(gcp_conn_id='gcp_airflow_conn_id')
    bigquery_hook = BigQueryHook(gcp_conn_id='gcp_airflow_conn_id')

    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )
    # Loading data from GCS to Bigquery
    load_recruit = GoogleCloudStorageToBigQueryOperator(
        dag=dag,
        task_id='load_recruit',
        gcp_conn_id=bigquery_hook.gcp_conn_id,
        bucket=gs_bucket_recruit,
        source_objects=['recruit_info20*.csv'],
        destination_project_dataset_table=f'{project_id}:{staging_dataset}.recruit',
        schema_fields=[
            {"name": "position_title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_industry", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_location", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_job_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_job_mid_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_industry_keyword_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_job_code_keyword_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_experience_level_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_experience_level_min", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_experience_level_max", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_required_education_level", "type": "STRING", "mode": "NULLABLE"},
            {"name": "keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "salary", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "position_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "expiration_timestamp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "expiration_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "read_cnt", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "apply_cnt", "type": "INTEGER", "mode": "NULLABLE"}
        ],
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        field_delimiter=',',
        encoding='UTF-8',
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )


    def add_google_trend_schema(ds, **kwargs):
        gcp_conn_id = 'gcp_airflow_conn_id',
        gcs_uri = kwargs['params']['gcs_uri']
        dataset_id = kwargs['params']['dataset_id']
        table_id = kwargs['params']['table_id']

        gcs_hook = GoogleCloudStorageHook(gcp_conn_id=gcp_conn_id)
        storage_client = gcs_hook.get_conn()
        blob = storage_client.get_bucket('hale-posting-bucket/recruit').blob(gcs_uri)
        schema = blob.download_as_text()
        bigquery_client = bigquery.Client()

        table = bigquery_client.get_table(f"{dataset_id}.{table_id}")
        table.schema = []

        add_field = [
            {"name": "date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "info", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "ai", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "bigdata", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "boj", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "bootcamp", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "db", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dm", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "de", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "da", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "datearch", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "ds", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "devops", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "ETL", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "date lake", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "date warehouse", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Docker", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Kafka", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Spark", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "AWS", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "Kubernetes", "type": "INTEGER", "mode": "NULLABLE"},
        ]

        for field in add_field:
            if not any(existing_field['name'] == field['name'] for existing_field in schema):
                table.schema.append(bigquery.SchemaField(field['name'], field['type']))

        bigquery_client.update_table(table, ["schema"])


    google_trend_list = ['hale-posting-bucket/google_trend/result_2023-07*.csv']
    schema_update_finished = []
    for i, csv in enumerate(google_trend_list):
        task_id = f'update_schema_google_trend_{i}'
        update_google_trend_schema = PythonOperator(
            dag=dag,
            task_id='update_google_trend_schema',
            python_callable=add_google_trend_schema,
            provide_context=True,
            params={
                'gcs_uri': google_trend_list,
                'dataset_id': 'staging',
                'table_id': 'google_trend'
            }
        )
        schema_update_finished.append(update_google_trend_schema)

    load_google_trend = GoogleCloudStorageToBigQueryOperator(
        task_id='load_google_trend',
        bucket=gs_bucket_google_trend,
        gcp_conn_id='gcp_airflow_conn_id',
        dag=dag,
        source_objects=google_trend_list,
        destination_project_dataset_table=f'{project_id}:{staging_dataset}.google_trend',
        autodetect=False,
        schema_fields=[],
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        encoding='UTF-8',
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
    )

    # Check load data not null and data schema
    check_recruit = BigQueryCheckOperator(
        dag=dag,
        task_id='check_recruit',
        gcp_conn_id='gcp_airflow_conn_id',
        # schema detect
        use_legacy_sql=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        # checking rows_count in staging
        sql=f"SELECT COUNT(*) FROM job-posting-api-388303.staging.recruit"
    )

    check_google_trend = BigQueryCheckOperator(
        dag=dag,
        task_id='check_google_trend',
        gcp_conn_id='gcp_airflow_conn_id',
        use_legacy_sql=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        # checking rows_count in staging
        sql=f"SELECT COUNT(*) FROM job-posting-api-388303.staging.google_trend"
    )

    # load dimensions data from filed directly DW table
    load_certification = GoogleCloudStorageToBigQueryOperator(
        dag=dag,
        task_id='load_certification',
        gcp_conn_id=bigquery_hook.gcp_conn_id,
        bucket=gs_bucket_certification,
        source_objects=['certificate_info.csv'],
        destination_project_dataset_table=f'{project_id}:{dwh_dataset}.certification',
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        encoding='UTF-8',
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        schema_fields=[
            {'name': '시험구분', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': '검정연도', 'type': 'INT', 'mode': 'REQUIRED'},
            {'name': '자격등급', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': '응시자수', 'type': 'INT', 'mode': 'REQUIRED'},
            {'name': '취득자수', 'type': 'INT', 'mode': 'REQUIRED'}
        ]
    )

    loaded_data_to_staging = DummyOperator(
        task_id='loaded_data_to_staging'
    )

    # create & check fact data
    create_recruit = BigQueryOperator(
        dag=dag,
        task_id='create_recruit',
        gcp_conn_id=bigquery_hook.gcp_conn_id,
        use_legacy_sql=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        # CTAS 구문으로 external_recruit 생성
        sql='taejun3305/instance-1/ELT_GCP/sql/Dimension_recruit.sql'
    )
    create_google_trend = BigQueryOperator(
        dag=dag,
        task_id='create_google_trend',
        gcp_conn_id=bigquery_hook.gcp_conn_id,
        use_legacy_sql=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        # CTAS 구문으로 external_google_trend 생성
        sql='taejun3305/instance-1/ELT_GCP/sql/Dimension_google_trend.sql'
    )

    check_dim_google_trend = BigQueryCheckOperator(
        dag=dag,
        task_id='check_dim_google_trend',
        gcp_conn_id=bigquery_hook.gcp_conn_id,
        use_legacy_sql=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql=f'SELECT COUNT(*) FROM job-posting-api-388303.external.google_trend'
    )

    check_certification = BigQueryCheckOperator(
        dag=dag,
        task_id='check_certification',
        gcp_conn_id=bigquery_hook.gcp_conn_id,
        use_legacy_sql=False,
        on_failure_callback=on_failure_callback,
        on_success_callback=on_success_callback,
        params={
            'project_id': project_id,
            'staging_dataset': staging_dataset,
            'dwh_dataset': dwh_dataset
        },
        sql=f'SELECT COUNT(*) FROM `job-posting-api-388303.external.certification`'
    )

    finish_pipeline = DummyOperator(
        task_id='finish_pipeline'
    )

    # define task dependencies
    start_pipeline >> [update_google_trend_schema, load_recruit, load_certification]

    update_google_trend_schema >> load_google_trend >> check_google_trend
    load_recruit >> check_recruit

    [check_recruit, check_google_trend, load_certification] >> loaded_data_to_staging

    loaded_data_to_staging >> [create_recruit, create_google_trend, check_certification]

    create_recruit >> check_dim_recruit
    create_google_trend >> check_dim_google_trend

    [check_dim_recruit, check_dim_google_trend, check_certification] >> finish_pipeline