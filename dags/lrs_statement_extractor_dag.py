from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import json
import os
from pathlib import Path
from typing import List, Dict, Any

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

REQUIRED_COLUMNS = {'id', 'statement_id', 'full_statement', 'timestamp'}

@task
def execute_sql(sql: str, conn_id: str = 'lrs_connection', **context) -> List[Any]:
    hook = PostgresHook(postgres_conn_id=conn_id)
    
    # schema 정보 가져오기
    conn = hook.get_connection(conn_id)
    schema = conn.schema
    if not schema:
        # Variable에서 schema 정보 가져오기
        metadata = get_connection_metadata_from_variable()
        schema = metadata.get('schema')
        if not schema:
            raise ValueError("schema 정보를 찾을 수 없습니다.")
    
    # SQL 쿼리에 schema 적용
    sql = sql.format(schema=schema)
    
    return hook.get_records(sql)

def get_connection_metadata_from_variable() -> Dict:
    """Variable에서 metadata 정보를 가져옵니다."""
    metadata_json = Variable.get("lrs_connection_meta", default_var="{}")
    metadata = json.loads(metadata_json)
    if not metadata:
        raise ValueError("Variable 'lrs_connection_meta'에 정보가 없습니다.")
    return metadata

@dag(
    dag_id='lrs_statement_extractor',
    default_args=default_args,
    description='Extract and save LRS statements to JSON files',
    schedule_interval=None,
    catchup=False
)
def lrs_statement_extractor():
    
    # 저장 경로 가져오기
    save_folder_path = Variable.get(
        "SAVE_FOLDER_PATH", 
        default_var=str(Path.home())  # 기본값으로 홈 디렉토리 사용
    )

    # Variable에서 metadata 정보 가져오기
    connection_metadata = get_connection_metadata_from_variable()
    
    # 테이블 구조 확인
    get_table_info = execute_sql("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = 'lrs_statement';
    """)
    
    # Variable의 metadata에서 type 정보 가져오기
    @task
    def get_type_info(metadata: Dict, **context) -> List[Any]:
        sys_type = metadata.get('sys_type')
        if not sys_type:
            raise ValueError("Variable 'lrs_connection_meta'에 'sys_type' 정보가 없습니다.")
        print(f"Using sys_type: {sys_type}")
        return [[sys_type]]
    
    # Variable의 metadata에서 컬럼 정보 가져오기
    @task
    def get_column_info(metadata: Dict, table_info: List[Any], **context) -> List[Any]:
        # 테이블의 실제 컬럼 이름 확인
        column_names = [col[0] for col in table_info]
        print("Available columns:", column_names)
        
        # metadata에서 컬럼 정보 가져오기
        columns = metadata.get('column')
        if not columns:
            raise ValueError("Variable 'lrs_connection_meta'에 'column' 정보가 없습니다.")
        
        # 필수 컬럼이 테이블에 있는지 확인
        for col in REQUIRED_COLUMNS:
            if col not in column_names:
                raise ValueError(f"필수 컬럼 '{col}'이 테이블에 없습니다.")
        
        # 필수 컬럼이 포함되어 있는지 확인
        if not REQUIRED_COLUMNS.issubset(set(columns)):
            raise ValueError(f"필수 컬럼 {REQUIRED_COLUMNS}이 metadata의 'column' 정보에 포함되어 있지 않습니다.")
        
        print(f"Using columns: {columns}")
        return [[json.dumps(columns)]]

    @task
    def create_select_query(columns: List[str], **context) -> str:
        ti = context['ti']
        last_process_id = ti.xcom_pull(task_ids='update_last_processed_id', key='return_value') or 0
        
        column_list = ', '.join(columns)
        return f"""
            SELECT {column_list}
            FROM {{schema}}.lrs_statement
            WHERE id > {last_process_id}
            ORDER BY id ASC
            LIMIT 500;
        """

    @task
    def prepare_base_path(type_result: List[Any], **context) -> str:
        if not type_result:
            raise ValueError("타입 정보를 찾을 수 없습니다.")
            
        type_value = type_result[0][0]
        if not isinstance(type_value, str):
            raise ValueError("타입 정보가 올바른 형식이 아닙니다.")
            
        # LRS 저장 경로 설정
        return str(Path(save_folder_path) / type_value)
    
    @task
    def prepare_columns(columns_result: List[Any], **context) -> List[str]:
        if not columns_result:
            raise ValueError("컬럼 정보를 찾을 수 없습니다.")
            
        try:
            columns = json.loads(columns_result[0][0])
            if not isinstance(columns, list):
                raise ValueError("컬럼 정보가 올바른 형식이 아닙니다.")
        except json.JSONDecodeError:
            # 이미 리스트 형태로 반환된 경우
            columns = columns_result[0][0]
            if not isinstance(columns, list):
                raise ValueError("컬럼 정보가 올바른 형식이 아닙니다.")
            
        # 필수 컬럼이 포함되어 있는지 확인
        if not REQUIRED_COLUMNS.issubset(set(columns)):
            raise ValueError(f"필수 컬럼 {REQUIRED_COLUMNS}이 포함되어 있지 않습니다: {columns}")
            
        return columns
    
    @task
    def process_results(query_results: List[Any], columns: List[str], base_path: str, **context) -> int:
        if not query_results:
            return 0
        
        max_id = 0
        for row in query_results:
            row_dict = {
                col: row[idx] if not isinstance(row[idx], datetime) else row[idx]
                for idx, col in enumerate(columns)
            }
            
            # timestamp를 기준으로 디렉토리 구조 생성
            statement_timestamp = row_dict.get('timestamp')
            if not statement_timestamp:
                statement_timestamp = datetime.now()
                
            # JSON 데이터 준비 (timestamp를 ISO 형식 문자열로 변환)
            statement_data = row_dict.copy()
            statement_data['timestamp'] = statement_timestamp.isoformat() if isinstance(statement_timestamp, datetime) else statement_timestamp
            
            # 날짜별 디렉토리 구조 생성
            if isinstance(statement_timestamp, datetime):
                month = statement_timestamp.strftime('%m')
                day = statement_timestamp.strftime('%d')
            else:
                # timestamp가 문자열인 경우
                try:
                    dt = datetime.fromisoformat(statement_timestamp)
                    month = dt.strftime('%m')
                    day = dt.strftime('%d')
                except (ValueError, TypeError):
                    # 파싱할 수 없는 경우 현재 날짜 사용
                    now = datetime.now()
                    month = now.strftime('%m')
                    day = now.strftime('%d')
            
            output_dir = Path(base_path) / month / day
            output_dir.mkdir(parents=True, exist_ok=True)
            
            statement_id = statement_data.get('statement_id', f"unknown_{max_id}")
            file_path = output_dir / f"{statement_id}.json"
            with open(file_path, 'w') as f:
                json.dump(statement_data, f, indent=2)
            
            max_id = max(max_id, statement_data.get('id', 0))
        
        return max_id

    @task
    def update_last_processed_id(max_id: int, **context) -> int:
        if max_id and max_id > 0:
            return max_id
        return 0

    # Task 의존성 설정
    base_path = prepare_base_path(get_type_info(connection_metadata))
    columns = prepare_columns(get_column_info(connection_metadata, get_table_info))
    select_query = create_select_query(columns)
    
    extract_statements = execute_sql(select_query)
    
    max_id = process_results(extract_statements, columns, base_path)
    update_last_processed_id(max_id)

dag = lrs_statement_extractor()