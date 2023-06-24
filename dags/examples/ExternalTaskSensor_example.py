from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

from datetime import datetime
"""
airflow.sensors.external_task.ExternalTaskSensor
(Reactive Trigger라고도 함.)

Sensor >> File, Http, Sql, Time, External
특정 조건을 wait
mode=(poke|reschedule)
    poke (default): worker 점유 (sleep) 
    reschedule: worker 릴리즈
    워커 리소스를 사용할 수 있는게 보장안되면,,

schedule_interval 동일
execution_date 동일 해야 함

실행주기 어긋나는 경우는
execution_delta=timedelta(minutes=5)
이런 식으로 싱크를 맞추면 되는데
주기 자체가 다른 경우는 ExternalTaskSensor 자체 사용이 불가. 
"""

with DAG(
    "ExternalTaskSensor_example",
    start_date=datetime(2023, 6, 19),
    schedule='@daily'
) as dag:

    waiting_for_end_of_dag = ExternalTaskSensor(
        task_id='ExternalTaskSensor',
        external_dag_id='TriggerDagRunOperator_example',
        external_task_id='trigger_task',
        timeout=5*60,
        mode='reschedule'
    )

    task = BashOperator(
        task_id='task',
        bash_command='''echo "ds: {{ ds }}"
        echo "ti: {{ task_instance }}, {{ ti }}"
        echo "dag: {{ dag }}, task: {{ task }}"
        echo "dag_run: {{ dag_run }}"
        '''
    )

    waiting_for_end_of_dag >> task