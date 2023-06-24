from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.latest_only import LatestOnlyOperator
from datetime import datetime, timedelta

"""
airflow.utls.trigger_rule.TriggerRule.(
    ALL_SUCCESS (default)
    ALL_FAILED
    ALL_DONE
    ONE_FAILED
    ONE_SUCCESS
    NONE_FAILED
    NONE_FAILED_MIN_ONE_SUCCESS
    ..
    ALL_SKIPPED
    ONE_DONE
    NONE_SKIPPED
    ALWAYS
)
이걸 Operator Param (trigger_rule=)

success인 애들은 skip도 안쳐준다.
>>> 디폴트가 모두 성공이기 때문에 앞에서 스킵이 일어나도 정상인 경우를 잘 처리해줘야.

*앞 모든 태스크 X 바로 앞단만.

ALL_SUCCESS, ALL_FAILED 둘 다 스킵 하나라도 있으면 안됨. 
"""

default_args = {
    'start_date': datetime(2023, 6, 20),
}

with DAG("TriggerRule_example", default_args=default_args, schedule=timedelta(1)) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date")
    t2 = BashOperator(task_id="sleep", bash_command="sleep 5")
    t3 = BashOperator(task_id="exit", bash_command="exit 1")
    t4 = BashOperator(
       task_id='final_task',
       bash_command='echo DONE!',
       trigger_rule=TriggerRule.ALL_DONE
    )

    [t1, t2, t3] >> t4

    loo = LatestOnlyOperator(task_id='latest_only')
    dummy = BashOperator(task_id='dummy',bash_command='')
    successed = BashOperator(task_id='success', bash_command='echo ALL_SUCCESS!', trigger_rule=TriggerRule.ALL_SUCCESS)
    failed = BashOperator(task_id='fail', bash_command='echo ALL_FAIL!', trigger_rule=TriggerRule.ALL_FAILED)
    nf_os = BashOperator(task_id='nonf_ones', bash_command='echo nfos!', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    loo >> dummy >> [successed, failed, nf_os]

    task_0 = EmptyOperator(task_id="task_0")
    task_1 = EmptyOperator(task_id="task_1", trigger_rule=TriggerRule.ALWAYS)
    task_2 = EmptyOperator(task_id="task_2", trigger_rule=TriggerRule.ALL_DONE)
    task_3 = EmptyOperator(task_id="task_3", trigger_rule=TriggerRule.ALL_FAILED)
    task_4 = EmptyOperator(task_id="task_4")
    task_5 = EmptyOperator(task_id="task_5", trigger_rule=TriggerRule.ALWAYS)
    task_6 = EmptyOperator(task_id="task_6")

    task_0 >> [task_1, task_2, task_3]
    [task_1, task_2, task_3] >> task_4
    [task_1, task_2, task_3] >> task_5
    task_5 >> task_6
