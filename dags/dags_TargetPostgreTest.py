from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.TargetPostgre import TargetPostgre

with DAG(
        dag_id='dags_TargetPostgreTest',
        start_date=pendulum.datetime(2025, 1, 20, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    
    AA='conn-db-postgres-custom'
    BB='TbCorona19CountStatus_bulk2'
    CC=','

    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = TargetPostgre(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter={CC}, is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': {AA},
                   'tbl_nm':{BB},
                   'file_nm':'/home/vanko1721/files/TbCorona19CountStatus/TestCorona19Status.csv'}
    )