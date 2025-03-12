# dags/standard_dag_with_k8s_executor.py
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 파이썬 함수 정의
def extract_data(**kwargs):
    """데이터 추출 함수"""
    print("데이터 추출 작업을 수행합니다.")
    # 데이터 처리 로직
    return {"extracted_data": "sample_data"}

def transform_data(**kwargs):
    """데이터 변환 함수"""
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_data')
    print(f"추출된 데이터: {extracted_data}")
    print("데이터 변환 작업을 수행합니다.")
    # 변환 로직
    return {"transformed_data": "transformed_sample_data"}

def load_data(**kwargs):
    """데이터 적재 함수"""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')
    print(f"변환된 데이터: {transformed_data}")
    print("데이터 적재 작업을 수행합니다.")
    # 적재 로직
    return {"status": "success"}

# DAG 정의
with DAG(
    'standard_etl_k8s_executor',
    default_args=default_args,
    description='Kubernetes Executor를 사용하는 일반 ETL DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'k8s_executor'],
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    # 데이터 처리 태스크 그룹
    with TaskGroup(group_id='process_data') as process_data:
        extract_data_task = PythonOperator(
            task_id='extract_data',
            python_callable=extract_data,
            provide_context=True,
            # executor_config를 통해 K8s 리소스 요청 설정 가능
            executor_config={
                "KubernetesExecutor": {
                    "request_memory": "512Mi",
                    "request_cpu": "0.5",
                    "limit_memory": "1Gi",
                    "limit_cpu": "1",
                    "labels": {"purpose": "data-extraction"},
                    "annotations": {"sidecar.istio.io/inject": "false"},
                }
            }
        )

        transform_data_task = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data,
            provide_context=True,
            executor_config={
                "KubernetesExecutor": {
                    "request_memory": "1Gi",
                    "request_cpu": "1",
                    "limit_memory": "2Gi",
                    "limit_cpu": "2",
                }
            }
        )

        extract_data_task >> transform_data_task

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        executor_config={
            "KubernetesExecutor": {
                "request_memory": "512Mi",
                "request_cpu": "0.5",
                "limit_memory": "1Gi",
                "limit_cpu": "1",
                "namespace": "airflow-load",  # 다른 네임스페이스에도 배포 가능
            }
        }
    )

    # 로그 저장 태스크
    archive_logs = BashOperator(
        task_id='archive_logs',
        bash_command='echo "로그 아카이빙 완료: $(date)" > /tmp/log_archive_$(date +%Y%m%d).txt',
        executor_config={
            "KubernetesExecutor": {
                "volumes": [
                    {
                        "name": "log-volume",
                        "persistent_volume_claim": {"claim_name": "airflow-logs-pvc"}
                    },
                ],
                "volume_mounts": [
                    {
                        "name": "log-volume",
                        "mount_path": "/tmp",
                        "sub_path": "logs"
                    },
                ],
            }
        }
    )

    end = EmptyOperator(task_id='end')
    
    start >> process_data >> load_data_task >> archive_logs >> end