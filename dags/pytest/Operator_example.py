from airflow import DAG
from airflow import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators...

from datetime import datetime, timedelta




from airflow.operators.trigeer_dagrun import TriggerDagRunOperator

trigger_DAG = TriggerDagRunOperator(
    task_id="trigger_DAG",
    trigger_dag_id="DAG_B",
    # airflow.cfg > dag_run_conf_overrides_params:True
    # kwargs["dag_run"].conf.get("path")
    conf={'path':'/opt/ml/conf'},
    execution_date="{{ ds }}",
    # 이미 실행된 dag가 있어도 재실행
    reset_dag_run=True,
    # 트리거된 dag_B가 끝날때까지 기다림
    wait_for_completion=True,
)



from airflow.sensors.external_task import ExternalTaskSensor

waiting_for_end_of_dag_a = ExternalTaskSensor(
    task_id='waiting_for_end_of_dag_a',
    external_dag_id='DAG',
    external_task_id='end',
    timeout=5*60,
    # worker 점유 방식 ( poke | reschedule )
    mode='reschedule',
)



from airflow.operators.python impoer BranchPythonOperator

def skip_or_cont_trigger():
    if Variable.get("mode","dev") == "dev":
        return []
    else:
        return ["trigger_b"]

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=skip_or_cont_trigger,
)



from airflow.operators.latest_only import LatestOnlyOperator

with DAG(
    dag_id='latest_only_example',
    schedule=timedelta(hours=48),
    start_date=datetime(2023,6,14),
    catchup=True,
) as dag:

    t1 = EmptyOperator(task_id='task1')
    # 과거 이력 백필 할 경우 t2는 실행 X
    t2 = LatestOnlyOperator(task_id='latest_only')
    t3 = EmptyOperator(task_id='task3')
    t4 = EmptyOperator(task_id='task4')

    t1 >> t2 >> [t3, t4]


from airflow.utils.trigger_rules import TriggerRule

tr = EmptyOperator(
    task_id='trg_rule',
    trigger_rule=TriggerRule.ALL_SUCCESS
)
