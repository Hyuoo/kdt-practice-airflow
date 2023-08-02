from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime
from datetime import timedelta

"""
airflow.operators.latest_only.LatestOnlyOperator

백필 시 불필요한 반복 태스크를 스킵. (현재스케줄의 태스크 한번만)

무조건 task2 (LatestOnly) 경우는 최근일 때,
즉 현재 슬롯에서 실행될 job이었을 경우에만 실행

DAG.following_schedule(time) 이런 식으로 다음 스케줄을 가져옴
left < now <= right
LatestOnly 자체는 Success로 기록 되고, 이후 태스크는 Skip

if not left_window < now <= right_window:
   self.log.info("Not latest execution, skipping downstream.")
   # we return an empty list, thus the parent BaseBranchOperator
   # won't exclude any downstream tasks from skipping.
   return []
else:
   self.log.info("Latest, allowing execution to proceed.")
   return list(context["task"].get_direct_relative_ids(upstream=False))
"""

with DAG(
   dag_id='LatestOnlyOperator_example',
   schedule=timedelta(hours=48),    # 매 48시간마다 실행되는 DAG로 설정
   start_date=datetime(2023, 6, 14),
   catchup=True
) as dag:

   t1 = EmptyOperator(task_id='task1')
   t2 = LatestOnlyOperator(task_id='latest_only')
   t3 = EmptyOperator(task_id='task3')
   t4 = EmptyOperator(task_id='task4')

   t1 >> t2 >> [t3, t4]