"""
Kubernetes Pod Operator를 사용한 데이터 처리 파이프라인 예제
- 데이터 수집
- 데이터 전처리
- 데이터 분석
- 결과 저장
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy import DummyOperator
from kubernetes.client import models as k8s

GIT_SYNC_PATH = '/opt/airflow/dags'
SCRIPT_PATH = f'{GIT_SYNC_PATH}/scripts'

# 기본 인자 설정
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# 공통으로 사용할 환경 변수
env_vars = [
    k8s.V1EnvVar(name='DATA_DIR', value='/data'),
    k8s.V1EnvVar(name='LOG_LEVEL', value='INFO'),
    k8s.V1EnvVar(name='EXECUTION_DATE', value='{{ ds }}'),
]

# 공통으로 사용할 볼륨 설정
# 데이터 볼륨
data_volume_config = k8s.V1Volume(
    name='data-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='data-processing-pvc'  # PVC 이름 (미리 생성 필요)
    )
)

data_volume_mount = k8s.V1VolumeMount(
    name='data-volume',
    mount_path='/data',
    sub_path=None,
    read_only=False
)

git_sync_volume_config = k8s.V1Volume(
    name='git-sync-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='airflow-dags'
    )
)

git_sync_volume_mount = k8s.V1VolumeMount(
    name = 'git-sync-volume',
    mount_path = '/opt/airflow/dags',
    sub_path = None,
    read_only = False
)


resources = k8s.V1ResourceRequirements(
    requests = {
        'cpu': '500m',
        'memory': '512Mi'
    },
    limits = {
        'cpu': '1000m',
        'memory': '1Gi'
    }
)

# DAG 정의
dag = DAG(
    'k8s_data_pipeline',
    default_args=default_args,
    description='Kubernetes Pod Operator를 사용한 데이터 파이프라인',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['kubernetes', 'data_processing'],
)

# 시작
start = DummyOperator(task_id='start_pipeline', dag=dag)

# 1. 데이터 수집 태스크
collect_data = KubernetesPodOperator(
    task_id='collect_data',
    name='data-collector',
    namespace='airflow',
    image='python:3.9-slim',
    cmds=["bash", "-c"],
    arguments=["pip install pandas && python /repo/scripts/collect_data.py"],
    env_vars=env_vars,
    volumes=[data_volume_config, git_sync_volume_config],
    volume_mounts=[data_volume_mount, git_sync_volume_mount],
    container_resources = resources,
    execution_timeout = timedelta(minutes=10),
    is_delete_operator_pod=True,
    in_cluster=True,
    get_logs=True,
    image_pull_policy='IfNotPresent',
    dag=dag,
)

# 2. 데이터 전처리 태스크
preprocess_data = KubernetesPodOperator(
    task_id='preprocess_data',
    name='data-preprocessor',
    namespace='airflow',
    image='python:3.9-slim',
    cmds=["bash", "-c"],
    arguments=["pip install pandas scikit-learn numpy && python /repo/scripts/preprocess_data.py"],
    env_vars=env_vars,
    volumes=[data_volume_config, git_sync_volume_config],
    volume_mounts=[data_volume_mount, git_sync_volume_mount],
    container_resources = resources,
    execution_timeout = timedelta(minutes=10),
    is_delete_operator_pod=True,
    in_cluster=True,
    get_logs=True,
    image_pull_policy='IfNotPresent',
    dag=dag,
)

# 3. 데이터 분석 태스크
analyze_data = KubernetesPodOperator(
    task_id='analyze_data',
    name='data-analyzer',
    namespace='airflow',
    image='python:3.9-slim',
    cmds=["bash", "-c"],
    arguments=["pip install pandas matplotlib seaborn numpy && python /repo/scripts/analyze_data.py"],
    env_vars=env_vars,
    volumes=[data_volume_config, git_sync_volume_config],
    volume_mounts=[data_volume_mount, git_sync_volume_mount],
    container_resources = resources,
    execution_timeout = timedelta(minutes=10),
    is_delete_operator_pod=True,
    in_cluster=True,
    get_logs=True,
    image_pull_policy='IfNotPresent',
    dag=dag,
)

# 4. 결과 저장 태스크
store_results = KubernetesPodOperator(
    task_id='store_results',
    name='result-storer',
    namespace='airflow',
    image='python:3.9-slim',
    cmds=["bash", "-c"],
    arguments=["pip install pandas && python /repo/scripts/store_results.py"],
    env_vars=env_vars,
    volumes=[data_volume_config, git_sync_volume_config],
    volume_mounts=[data_volume_mount, git_sync_volume_mount],
    container_resources = resources,
    execution_timeout = timedelta(minutes=10),
    is_delete_operator_pod=True,
    in_cluster=True,
    get_logs=True,
    image_pull_policy='IfNotPresent',
    dag=dag,
)

# 종료
end = DummyOperator(task_id='end_pipeline', dag=dag)

# 태스크 의존성 설정
start >> collect_data >> preprocess_data >> analyze_data >> store_results >> end