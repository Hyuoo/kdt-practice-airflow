from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import logging
"""
트리거되지 않더라도 1번 실행 될 수. >> schedule=None
이 경우 foo의 execution_date가 None이 나온다.
에러도 뿜뿜 근데 썩세스

활성화되지않으면 트리거 되는 만큼 queued
"""
def foo(**kwargs):
    logging.info(f'dag_run:{kwargs["dag_run"]}')
    logging.info(f'conf:{kwargs["dag_run"].conf.get("path")}')
    logging.info(f'ti:{kwargs["ti"]}')
    logging.info(f'data_interval_start:{kwargs["data_interval_start"]}')
    # 버전땜에 execution_date 말고 logical_date 쓰랜다.
    logging.info(f'logical_date:{kwargs["logical_date"]}')

dag = DAG(
    'TriggerDag_Target',
    schedule=None,
    start_date=datetime(2023, 6, 1),
)

task1 = BashOperator(
    task_id='task1',
    bash_command="""echo '{{ ds }}, {{ dag_run.conf.get("path", "none") }}' """,
    dag=dag
)
task2 = PythonOperator(
    task_id='task2',
    python_callable=foo,
    dag=dag,
)