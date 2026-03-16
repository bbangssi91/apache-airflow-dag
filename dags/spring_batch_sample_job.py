from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from utils.spring_batch import trigger_spring_batch_job, check_batch_job_status, SPRING_BATCH_BASE_URL

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "spring_batch_sample_job",
    default_args=default_args,
    description="Spring Batch Job 2개를 순차적으로 실행하는 DAG",
    
    # =========================== Schedule 설정 가이드 ===========================
    # 1. timedelta 활용 (일정 간격으로 실행)
    #    - schedule=timedelta(days=1)     : 매일 (24시간 간격) 실행
    #    - schedule=timedelta(days=3)     : 3일마다 실행
    #    - schedule=timedelta(hours=1)    : 1시간마다 실행
    #    - schedule=timedelta(minutes=5)  : 5분마다 실행
    #
    # 2. Cron 표현식 활용 (특정 시점에 실행)
    #    - schedule="* * * * *"           : 매분 실행
    #    - schedule="0 * * * *"           : 매시간 정각에 실행
    #    - schedule="0 0 * * *"           : 매일 자정에 실행
    #    - schedule="0 0 * * 1"           : 매주 월요일 자정에 실행
    #
    # 3. Airflow 기본 Preset 활용
    #    - schedule="@hourly"             : 매시간 정각에 실행 ("0 * * * *")
    #    - schedule="@daily"              : 매일 자정에 실행 ("0 0 * * *")
    #    - schedule="@weekly"             : 매주 일요일 자정에 실행 ("0 0 * * 0")
    #    - schedule="@once"               : 단 1회만 실행
    # ========================================================================
    schedule=timedelta(minutes=5),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spring-batch", "example"],
) as dag:

    # 1. 첫 번째 Job (sample)
    trigger_batch_job_1 = PythonOperator(
        task_id="trigger_batch_job_1",
        python_callable=trigger_spring_batch_job,
        op_kwargs={"target_url": f"{SPRING_BATCH_BASE_URL}/sample"},
    )

    check_job_status_1 = PythonOperator(
        task_id="check_job_status_1",
        python_callable=check_batch_job_status,
        op_kwargs={"trigger_task_id": "trigger_batch_job_1"},
    )

    # 2. 두 번째 Job (sample2)
    trigger_batch_job_2 = PythonOperator(
        task_id="trigger_batch_job_2",
        python_callable=trigger_spring_batch_job,
        op_kwargs={"target_url": f"{SPRING_BATCH_BASE_URL}/sample2"},
    )

    check_job_status_2 = PythonOperator(
        task_id="check_job_status_2",
        python_callable=check_batch_job_status,
        op_kwargs={"trigger_task_id": "trigger_batch_job_2"},
    )

    # 3. 세 번째 Job (sample3 - Job1 실패 시 실행)
    trigger_batch_job_3 = PythonOperator(
        task_id="trigger_batch_job_3",
        python_callable=trigger_spring_batch_job,
        op_kwargs={"target_url": f"{SPRING_BATCH_BASE_URL}/sample3"},
        trigger_rule="all_failed", # 부모 태스크가 모두 실패했을 때만 실행
    )

    check_job_status_3 = PythonOperator(
        task_id="check_job_status_3",
        python_callable=check_batch_job_status,
        op_kwargs={"trigger_task_id": "trigger_batch_job_3"},
    )

    # 태스크 흐름 (Dependency) 정의
    # Job1 트리거 -> Job1 상태 확인
    trigger_batch_job_1 >> check_job_status_1
    
    # Job1 상태 확인 성공 시 -> Job2 실행
    check_job_status_1 >> trigger_batch_job_2 >> check_job_status_2
    
    # Job1 상태 확인 실패 시(Exception 발생으로 실패 처리됨) -> Job3 실행
    check_job_status_1 >> trigger_batch_job_3 >> check_job_status_3
