from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(dag_id='ok_sequential', start_date=datetime(2024,1,1), schedule=None, catchup=False) as dag:
    def _fn_extract():
        print('task extract')
    extract = PythonOperator(task_id='extract', python_callable=_fn_extract)
    def _fn_load():
        print('task load')
    load = PythonOperator(task_id='load', python_callable=_fn_load)

    extract >> load
