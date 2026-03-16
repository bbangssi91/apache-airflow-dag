import requests

# 환경 및 대상 URL 정의 (공통으로 사용)
SPRING_BATCH_BASE_URL = "http://host.docker.internal:8081/batch/jobs"

def trigger_spring_batch_job(target_url, **context):
    """Spring Batch Job을 지정된 URL로 트리거합니다."""
    execution_date = context.get("ds")

    payload = {
        "jobParameters": {
            "executionDate": execution_date,
        }
    }

    response = requests.post(
        target_url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=300,
    )

    response.raise_for_status()
    result = response.json()
    print(f"Response: {result}")

    if isinstance(result, int):
        job_execution_id = result
    else:
        job_execution_id = (
            result.get("jobExecutionId") if isinstance(result, dict) else result
        )

    print(f"Spring Batch Job started on {target_url}. Execution ID: {job_execution_id}")

    return job_execution_id


def check_batch_job_status(trigger_task_id, **context):
    """지정된 Trigger 태스크가 실행한 Spring Batch Job의 상태를 확인합니다."""
    ti = context["ti"]
    job_execution_id = ti.xcom_pull(task_ids=trigger_task_id)

    status_url = f"{SPRING_BATCH_BASE_URL}/{job_execution_id}/status"

    response = requests.get(status_url, timeout=30)
    response.raise_for_status()

    status = response.json().get("status")
    print(f"Job {job_execution_id} status: {status}")

    if status in ["FAILED", "STOPPED"]:
        raise Exception(f"Batch job failed with status: {status}")

    return status
