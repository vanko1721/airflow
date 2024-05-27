from airflow.models.dag import DAG
from airflow.decorators import task
import pendulum
import datetime

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2024, 5, 20, tz="Asia/Seoul"),
    catchup=True,
):
    
    @task(task_id='python_task')
    def show_template(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    
    show_template()