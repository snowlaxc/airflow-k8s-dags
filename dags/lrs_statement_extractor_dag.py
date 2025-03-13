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
def execute_sql(sql: str, conn_id: str = 'lrs_connection') -> List[Any]:
    hook = PostgresHook(postgres_conn_id=conn_id)
    # Connection 정보에서 metadata 제외하여 psycopg2에 전달
    conn = hook.get_connection(conn_id)
    print(type(conn.extra))
    # extra_dict = json.loads(conn.extra) if conn.extra else {}
    # filtered_extra = {k: v for k, v in extra_dict.items() if k != 'metadata'}
    # conn.extra = json.dumps(filtered_extra) if filtered_extra else None
    # hook.connection = conn
    
    # # schema 정보 가져오기
    # schema = conn.schema
    # if not schema:
    #     raise ValueError("Connection에 schema 정보가 없습니다.")
    
    # # SQL 쿼리에 schema 적용
    # sql = sql.format(schema=schema)
    return True
    # return hook.get_records(sql)

@task
def get_connection_metadata(conn_id: str = 'lrs_connection') -> Dict:
    """Connection의 extra 필드에서 metadata 정보를 가져옵니다."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)
    extra_dict = json.loads(conn.extra) if conn.extra else {}
    metadata = extra_dict.get('metadata')
    if not metadata:
        raise ValueError("Connection의 extra 필드에 'metadata' 정보가 없습니다.")
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

    # Connection의 metadata 정보 가져오기
    connection_metadata = get_connection_metadata()
    
    # 테이블 구조 확인
    get_table_info = execute_sql("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = '{schema}' AND table_name = 'lrs_statement';
    """)
    
    # Connection의 metadata에서 type 정보 가져오기
    @task
    def get_type_info(metadata: Dict) -> List[Any]:
        sys_type = metadata.get('sys_type')
        if not sys_type:
            raise ValueError("Connection의 metadata에 'sys_type' 정보가 없습니다.")
        print(f"Using sys_type: {sys_type}")
        return [[sys_type]]
    
    # Connection의 metadata에서 컬럼 정보 가져오기
    @task
    def get_column_info(metadata: Dict, table_info: List[Any]) -> List[Any]:
        # 테이블의 실제 컬럼 이름 확인
        column_names = [col[0] for col in table_info]
        print("Available columns:", column_names)
        
        # metadata에서 컬럼 정보 가져오기
        columns = metadata.get('column')
        if not columns:
            raise ValueError("Connection의 metadata에 'column' 정보가 없습니다.")
        
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
    def create_select_query(columns: List[str]) -> str:
        column_list = ', '.join(columns)
        return f"""
            SELECT {column_list}
            FROM {{schema}}.lrs_statement
            WHERE id > {{{{ task_instance.xcom_pull(task_ids='update_last_processed_id', key='return_value') or 0 }}}}
            ORDER BY id ASC
            LIMIT 500;
        """

    @task
    def prepare_base_path(type_result: List[Any]) -> str:
        if not type_result:
            raise ValueError("No type information found in the table")
            
        type_value = type_result[0][0]
        if not isinstance(type_value, str):
            raise ValueError("Type information is not in the expected format")
            
        # LRS 저장 경로 설정
        return str(Path(save_folder_path) / type_value)
    
    @task
    def prepare_columns(columns_result: List[Any]) -> List[str]:
        if not columns_result:
            raise ValueError("No column information found in the table")
            
        try:
            columns = json.loads(columns_result[0][0])
            if not isinstance(columns, list):
                raise ValueError("Column information is not in the expected format")
        except json.JSONDecodeError:
            # 이미 리스트 형태로 반환된 경우
            columns = columns_result[0][0]
            if not isinstance(columns, list):
                raise ValueError("Column information is not in the expected format")
            
        # 필수 컬럼이 포함되어 있는지 확인
        if not REQUIRED_COLUMNS.issubset(set(columns)):
            raise ValueError(f"Required columns {REQUIRED_COLUMNS} are not present in {columns}")
            
        return columns
    
    @task
    def process_results(query_results: List[Any], columns: List[str], base_path: str) -> int:
        if not query_results:
            return 0
        
        max_id = 0
        for row in query_results:
            row_dict = {
                col: row[idx] if not isinstance(row[idx], datetime) else row[idx]
                for idx, col in enumerate(columns)
            }
            
            # timestamp를 기준으로 디렉토리 구조 생성
            statement_timestamp = row_dict['timestamp']
            if not statement_timestamp:
                statement_timestamp = datetime.now()
                
            # JSON 데이터 준비 (timestamp를 ISO 형식 문자열로 변환)
            statement_data = row_dict.copy()
            statement_data['timestamp'] = statement_timestamp.isoformat()
            
            # 날짜별 디렉토리 구조 생성
            output_dir = Path(base_path) / statement_timestamp.strftime('%m') / statement_timestamp.strftime('%d')
            output_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = output_dir / f"{statement_data['statement_id']}.json"
            with open(file_path, 'w') as f:
                json.dump(statement_data, f, indent=2)
            
            max_id = max(max_id, statement_data['id'])
        
        return max_id

    @task
    def update_last_processed_id(max_id: int) -> int:
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