from airflow.models.dag import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_ep1",
    schedule="10 0 1 * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    #START_DATE : 전월말일 END_DATE : 1일전
    bash_task_1 = BashOperator(
        task_id = bash_task_1,
        env={'START_DATE':'{{ data_interval_start.in_timejone("Asia/Seoul") | ds }}',
             'END_DATE':'{{ (data_interval_end.in_timejone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )