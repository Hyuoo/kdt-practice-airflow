from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

"""
jinja template
presentation logic, application logic의 분리
장고에서 맨 처음 썼다가 진자로 만들어지고 플라스크가 엄청 쓴다고 함.

{{ ~~~ }} 변수
{{%  %}} 제어문

특정 operator, param에 대해서 적용 가능.

지원되는 파라미터는 reference에서 특정 파라미터의 설명 뒤에 (templated) 하고 써있다.

example
{{ ds }}
{{ ds_nodash }}
{{ ts }}
{{ dag }}
{{ task }}
{{ dag_run }}
{{ var.value }}: {{ var.value.get('my.var', 'fallback') }}
{{ var.json }}: {{ var.json.my_dict_var.key1 }}
{{ conn }}: {{ conn.my_conn_id.login }}, {{ conn.my_conn_id.password }}
"""

# DAG 정의
dag = DAG(
    'jinja_template_example',
    schedule='0 0 * * *',  # 매일 실행
    start_date=datetime(2023, 6, 1),
    catchup=False
)

# BashOperator를 사용하여 템플릿 작업 정의
task1 = BashOperator(
    task_id='task1',
    bash_command='''echo "ds: {{ ds }}"
    echo "ds_nodh: {{ ds_nodash }}"
    echo "dag: {{ dag }}, task: {{ task }}"
    echo "ti: {{ task_instance }}, {{ ti }}"
    echo "dag_run: {{ dag_run }}"
    ''',
    dag=dag
)

# 동적 매개변수가 있는 다른 템플릿 작업 정의
task2 = BashOperator(
    task_id='task2',
    bash_command='echo "안녕하세요, {{ params.name }}!__ {{ params.three.two }} __ {{ params.multi }}"',
    params={'name': 'John', 'three':{'two':'one'}, 'multi':{'a':95,'b':96}},  # 사용자 정의 가능한 매개변수
    dag=dag
)

task3 = BashOperator(
    task_id='task3',
    bash_command="""echo "{{ dag }}, {{ task }}, {{ var.value.get('csv_url') }}" """,
    dag=dag
)

task1 >> task2 >> task3


""" 
***************************** TASK1 *****************************
[2023-06-23 12:14:31,410] {dag.py:3634} INFO - *****************************************************
[2023-06-23 12:14:31,410] {dag.py:3638} INFO - Running task task1
[2023-06-23 12:14:31,877] {taskinstance.py:1509} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=Learn_Jinja
AIRFLOW_CTX_TASK_ID=task1
AIRFLOW_CTX_EXECUTION_DATE=2023-06-22T00:00:00+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2023-06-22T00:00:00+00:00
[2023-06-23 12:14:31,877] {taskinstance.py:1509} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=Learn_Jinja
AIRFLOW_CTX_TASK_ID=task1
AIRFLOW_CTX_EXECUTION_DATE=2023-06-22T00:00:00+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2023-06-22T00:00:00+00:00
[2023-06-23 12:14:31,880] {subprocess.py:63} INFO - Tmp dir root location: 
 /tmp
[2023-06-23 12:14:31,882] {subprocess.py:75} INFO - Running command: ['/bin/bash', '-c', 'echo "ds: 2023-06-22"\n    echo "ds_nodh: 20230622"\n    echo "dag: <DAG: Learn_Jinja>, task: <Task(BashOperator): task1>"\n    echo "ti: <TaskInstance: Learn_Jinja.task1 manual__2023-06-22T00:00:00+00:00 [None]>, <TaskInstance: Learn_Jinja.task1 manual__2023-06-22T00:00:00+00:00 [None]>"\n    echo "dag_run: <DagRun Learn_Jinja @ 2023-06-22T00:00:00+00:00: manual__2023-06-22T00:00:00+00:00, state:running, queued_at: None. externally triggered: False>"\n    ']
[2023-06-23 12:14:31,904] {subprocess.py:86} INFO - Output:
[2023-06-23 12:14:31,906] {subprocess.py:93} INFO - ds: 2023-06-22
[2023-06-23 12:14:31,906] {subprocess.py:93} INFO - ds_nodh: 20230622
[2023-06-23 12:14:31,906] {subprocess.py:93} INFO - dag: <DAG: Learn_Jinja>, task: <Task(BashOperator): task1>
[2023-06-23 12:14:31,906] {subprocess.py:93} INFO - ti: <TaskInstance: Learn_Jinja.task1 manual__2023-06-22T00:00:00+00:00 [None]>, <TaskInstance: Learn_Jinja.task1 manual__2023-06-22T00:00:00+00:00 [None]>
[2023-06-23 12:14:31,907] {subprocess.py:93} INFO - dag_run: <DagRun Learn_Jinja @ 2023-06-22T00:00:00+00:00: manual__2023-06-22T00:00:00+00:00, state:running, queued_at: None. externally triggered: False>
[2023-06-23 12:14:31,907] {subprocess.py:97} INFO - Command exited with return code 0
[2023-06-23 12:14:31,968] {taskinstance.py:1323} INFO - Marking task as SUCCESS. dag_id=Learn_Jinja, task_id=task1, execution_date=20230622T000000, start_date=, end_date=20230623T121431
[2023-06-23 12:14:31,968] {taskinstance.py:1323} INFO - Marking task as SUCCESS. dag_id=Learn_Jinja, task_id=task1, execution_date=20230622T000000, start_date=, end_date=20230623T121431
[2023-06-23 12:14:32,031] {dag.py:3642} INFO - task1 ran successfully!


***************************** TASK2 *****************************
[2023-06-23 12:14:32,047] {dag.py:3638} INFO - Running task task2
[2023-06-23 12:14:32,163] {taskinstance.py:1509} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=Learn_Jinja
AIRFLOW_CTX_TASK_ID=task2
AIRFLOW_CTX_EXECUTION_DATE=2023-06-22T00:00:00+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2023-06-22T00:00:00+00:00
[2023-06-23 12:14:32,163] {taskinstance.py:1509} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=Learn_Jinja
AIRFLOW_CTX_TASK_ID=task2
AIRFLOW_CTX_EXECUTION_DATE=2023-06-22T00:00:00+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2023-06-22T00:00:00+00:00
[2023-06-23 12:14:32,165] {subprocess.py:63} INFO - Tmp dir root location: 
 /tmp
[2023-06-23 12:14:32,172] {subprocess.py:75} INFO - Running command: ['/bin/bash', '-c', 'echo "안녕하세요, John!__ one __ {\'a\': 95, \'b\': 96}"']
[2023-06-23 12:14:32,195] {subprocess.py:86} INFO - Output:
[2023-06-23 12:14:32,202] {subprocess.py:93} INFO - 안녕하세요, John!__ one __ {'a': 95, 'b': 96}
[2023-06-23 12:14:32,203] {subprocess.py:97} INFO - Command exited with return code 0
[2023-06-23 12:14:32,263] {taskinstance.py:1323} INFO - Marking task as SUCCESS. dag_id=Learn_Jinja, task_id=task2, execution_date=20230622T000000, start_date=, end_date=20230623T121432
[2023-06-23 12:14:32,263] {taskinstance.py:1323} INFO - Marking task as SUCCESS. dag_id=Learn_Jinja, task_id=task2, execution_date=20230622T000000, start_date=, end_date=20230623T121432
[2023-06-23 12:14:32,306] {dag.py:3642} INFO - task2 ran successfully!


***************************** TASK3 *****************************
[2023-06-23 12:14:32,315] {dag.py:3638} INFO - Running task task3
[2023-06-23 12:14:32,455] {taskinstance.py:1509} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=Learn_Jinja
AIRFLOW_CTX_TASK_ID=task3
AIRFLOW_CTX_EXECUTION_DATE=2023-06-22T00:00:00+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2023-06-22T00:00:00+00:00
[2023-06-23 12:14:32,455] {taskinstance.py:1509} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=Learn_Jinja
AIRFLOW_CTX_TASK_ID=task3
AIRFLOW_CTX_EXECUTION_DATE=2023-06-22T00:00:00+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2023-06-22T00:00:00+00:00
[2023-06-23 12:14:32,456] {subprocess.py:63} INFO - Tmp dir root location: 
 /tmp
[2023-06-23 12:14:32,460] {subprocess.py:75} INFO - Running command: ['/bin/bash', '-c', 'echo "<DAG: Learn_Jinja>, <Task(BashOperator): task3>, https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv" ']
[2023-06-23 12:14:32,489] {subprocess.py:86} INFO - Output:
[2023-06-23 12:14:32,494] {subprocess.py:93} INFO - <DAG: Learn_Jinja>, <Task(BashOperator): task3>, https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv
[2023-06-23 12:14:32,494] {subprocess.py:97} INFO - Command exited with return code 0
[2023-06-23 12:14:32,618] {taskinstance.py:1323} INFO - Marking task as SUCCESS. dag_id=Learn_Jinja, task_id=task3, execution_date=20230622T000000, start_date=, end_date=20230623T121432
[2023-06-23 12:14:32,618] {taskinstance.py:1323} INFO - Marking task as SUCCESS. dag_id=Learn_Jinja, task_id=task3, execution_date=20230622T000000, start_date=, end_date=20230623T121432
[2023-06-23 12:14:32,714] {dag.py:3642} INFO - task3 ran successfully!
"""