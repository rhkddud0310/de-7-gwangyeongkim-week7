from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'gwangyeong',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}

def count_lines_func(**context):
    ti = context['ti']
    # Airflow 2.x에서는 첫 시도 시 try_number가 1로 시작합니다.
    # 첫 시도에서 의도적으로 에러를 발생시켜 재시도를 유도합니다.
    if ti.try_number <= 1:
        logger.error(f"Intentional failure for retry demonstration (try_number: {ti.try_number})")
        raise RuntimeError("Simulated failure on first attempt to demonstrate retry behavior")
    
    # 2번째 시도(재시도)에서 정상적으로 값을 반환합니다.
    logger.info(f"Task succeeded on attempt {ti.try_number}")
    return 42

def use_value_func(**context):
    ti = context['ti']
    received = ti.xcom_pull(task_ids='count_lines')
    doubled = received * 2 if isinstance(received, int) else None
    
    logger.info(f"received from XCom: {received}")
    logger.info(f"doubled = {doubled}")

with DAG(
    dag_id='xcom_demo_gwangyeong',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['week7', 'q6']
) as dag:

    count_lines = PythonOperator(
        task_id='count_lines',
        python_callable=count_lines_func,
    )

    use_value = PythonOperator(
        task_id='use_value',
        python_callable=use_value_func,
    )

    count_lines >> use_value