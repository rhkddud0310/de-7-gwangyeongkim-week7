from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def return_hello():
    # 파이썬 작업 1: 문자열 return (print 아님)
    return "Hello Airflow"

def return_goodbye():
    # 파이썬 작업 2: 문자열 return (print 아님)
    return "Goodbye Airflow"

with DAG(
    dag_id="sample_dag",
    default_args=default_args,
    description="Q3: Airflow Compose Sample DAG",
    schedule_interval="@daily",  # 매일 1회
    start_date=datetime(2026, 8, 1),
    catchup=False,  # 과거 구간 자동 실행 건너뛰기
    tags=["q3", "compose"],
) as dag:

    start = EmptyOperator(task_id="start")

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=return_hello,
    )

    goodbye_task = PythonOperator(
        task_id="goodbye_task",
        python_callable=return_goodbye,
    )

    end = EmptyOperator(task_id="end")

    # 시작 -> 파이썬 1 -> 파이썬 2 -> 종료 (4개 Task 순차 연결)
    start >> hello_task >> goodbye_task >> end