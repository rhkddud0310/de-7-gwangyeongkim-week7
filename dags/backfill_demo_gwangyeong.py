from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

OUTPUT_DIR = "/opt/airflow/data/backfill"

with DAG(
    dag_id="backfill_demo_gwangyeong",
    # 작업일(2026-09-19) 기준 7일 전 고정 날짜
    start_date=datetime(2026, 9, 12),
    schedule="@daily",
    # 과거 누락된 실행 구간을 자동으로 채우는 핵심 설정
    catchup=True,
    max_active_runs=2,
    tags=["week7", "q7"]
) as dag:

    # 1. 파일 생성 태스크: 실행별 logical date(ds)를 파일명과 내용에 기록
    write_daily_file = BashOperator(
        task_id="write_daily_file",
        bash_command=f"""
        mkdir -p {OUTPUT_DIR}
        echo "Logical Date: {{{{ ds }}}}" > {OUTPUT_DIR}/daily_{{{{ ds }}}}.txt
        """,
    )

    # 2. 검증 태스크: 파일 생성 확인
    verify_file = BashOperator(
        task_id="verify_file",
        bash_command=f"ls -l {OUTPUT_DIR}/daily_{{{{ ds }}}}.txt",
    )

    write_daily_file >> verify_file