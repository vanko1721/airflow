from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_test_value_select',
    start_date=pendulum.datetime(2025,1,3, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:

    
    def value_select_conn(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2 # type: ignore
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                val1 = 'col2'
                val2 = '1'
                val3 = '1'
                sql = 'select ' + val1 + ' from value_test where col1 = ' + val2
                cursor.execute(sql)
                result = cursor.fetchall()
                sql1 = 'select 1+2 as c FROM value_test2 where col1 = ' + "'" + val3 + "'"
                result1 = cursor.execute(sql1,(result[0]))
                #result1 = cursor.fetchall()
                return result1

    python_value_test = PythonOperator(
        task_id='python_value_test',
        python_callable=value_select_conn,
        op_args=['172.28.0.3', '5432', 'vanko1721', 'vanko1721', 'vanko1721']
    )
        
    python_value_test