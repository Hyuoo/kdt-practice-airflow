from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import pendulum

"""
airflow.utils.task_group.TaskGroup
with TaskGroup("group_name", tooltip="Task tooltip") as group_a:
    task_a
    task_b
    task_a >> task_b

의존성 설정부터 [A,B,C] >> [D,E] 이런식으로 list to list가 안됨.
옛날엔 subDAG.
"""

with DAG(dag_id="Learn_Task_Group", start_date=pendulum.today('UTC').add(days=-2), tags=["example"]) as dag:
    start = EmptyOperator(task_id="start")

    # Task Group #1
    with TaskGroup("Download", tooltip="Tasks for downloading data") as section_1:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
        task_3 = EmptyOperator(task_id="task_3", trigger_rule=TriggerRule.ALL_FAILED)

        task_1 >> [task_2, task_3]

    # Task Group #2
    with TaskGroup("Process", tooltip="Tasks for processing data") as section_2:
        task_1 = EmptyOperator(task_id="task_1")

        with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
            task_3 = EmptyOperator(task_id="task_3")
            task_4 = EmptyOperator(task_id="task_4")

            [task_2, task_3] >> task_4

    end = EmptyOperator(task_id='end')

    start >> section_1 >> section_2 >> end

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")
    t3 = EmptyOperator(task_id="t3")
    t4 = EmptyOperator(task_id="t4", trigger_rule=TriggerRule.ALL_FAILED)
    t5 = EmptyOperator(task_id="t5")

    start >> t1
    [t1, t2] >> t4
    [t3, t4] >> t5 >> end


