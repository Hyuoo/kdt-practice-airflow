from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

"""
airflow.operators.python.BranchPythonOperator

a >> b
a >> [c,d]
이럴 때 a branch c 하면 b,d스킵

a >> b
a >> c
이럴 때 a branch [b,c] 하면 둘다 실행

그냥 a >> [b,c,d,e,f] 다 넣어놓고 이중에 브랜치 하는 식으로 써도 될 듯.

XXX 의존성을 명시 안한 dangling_task도 정상적으로 동작하는걸 봐서는
XXX 의존성과 상관없이 리턴되는 task_id를 모두 실행하는 듯 함. 
의존성에 아예 안넣은 태스크는 그냥 실행되는거였음
의존성으로 등록안된 태스크는 리턴해도 실행 안됨.

에러는 안나는데 실행안됨. 그냥 무시되는 듯 함.

의존성을 나눠서 하는 경우는
그 이후 과정이 같지않을 경우.

branch같은경우 웹UI에서 태스크 확인 할 때 편의성을 위해서
네이밍을 통일시켜줘야 할 듯 함. 
"""

default_args = {
    'start_date': datetime(2023, 1, 1)
}

dag = DAG(
    'BranchPythonOperator_example',
    schedule='@daily',
    default_args=default_args)


def decide_branch(**context):
    current_hour = datetime.now().hour
    print(f"current_hour: {current_hour}")
    if current_hour < 12:
        return 'morning_task'
    else:
        return ['afternoon_task','task_a']


branching_operator = BranchPythonOperator(
    task_id='branching_task',
    python_callable=decide_branch,
    dag=dag
)


morning_task = EmptyOperator(
    task_id='morning_task',
    dag=dag
)


afternoon_task = EmptyOperator(
    task_id='afternoon_task',
    dag=dag
)
night_task = EmptyOperator(
    task_id='night_task',
    dag=dag
)
task_a = EmptyOperator(
    task_id='task_a',
    dag=dag
)
dangling_task = EmptyOperator(
    task_id='dangling_task',
    dag=dag
)

branching_operator >> morning_task >> dangling_task
branching_operator >> [afternoon_task,night_task]
branching_operator >> task_a
