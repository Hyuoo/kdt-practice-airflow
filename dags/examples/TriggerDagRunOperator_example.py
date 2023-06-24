from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

"""
airflow.operators.trigger_dagrun.TriggerDagRunOperator
(Explicit Trigger)

아래 conf의 경우 airflow.cfg
[core] dag_run_conf_overrides_params = True
로 설정되어있어야 함.
"""

dag = DAG(
    dag_id='TriggerDagRunOperator_example',
    start_date=datetime(2023, 6, 19),
    schedule='@daily'
)

trigger_task = TriggerDagRunOperator(
    task_id='trigger_task',
    trigger_dag_id='TriggerDag_Target',
    # jinja에서.. {{ dag_run.conf["path"] }}
    # **kwargs에서.. kwargs["dag_run"].conf.get("path")
    conf={'path': 'value1'},
    # trigger dag의 execution date 패스
    execution_date='{{ ds }}',
    # 해당 dag가 실행되었어도 재실행 여부
    reset_dag_run=True,
    # targer dag가 끝날 때 까지 기자릴 지 여부 (default:False)
    # wait_for_completion=True
    dag=dag
)
