from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from socrata_check import main_func

DAG = DAG(
  dag_id='socrata_to_s3',
  start_date=datetime.now(),
  schedule_interval='@once'
)

def socrata_main_check():
    main_func() 

t1 = PythonOperator(
    task_id='socrata_main', 
    python_callable=socrata_main_check,
    dag=DAG)

t1