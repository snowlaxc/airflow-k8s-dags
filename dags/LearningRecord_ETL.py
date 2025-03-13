from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import logging

"""

# Airflow 변수 설정 예시
1. save_folder_path = /data
2. lrs_connection_meta = {
    "sys_type": "lrs",
    "schema": "acid",
    "table": "lrs_statement",
    "column": [
      "id",
      "statement_id",
      "full_statement",
      "timestamp"
    ]
}

# Airflow Connection 설정 예시
conn_id = lrs_connection
conn_type = Postgres
host = 192.168.0.114
schema = acid
login = acid
password = test1234
port = 5432

"""

# 상수 정의
POSTGRES_CONN_ID = 'lrs_connection'
LRS_METADATA_VARIABLE = 'lrs_connection_meta'
SAVE_FOLDER_VARIABLE = 'save_folder_path'

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

def get_metadata_from_variable() -> Dict:
    """Variable에서 metadata 정보를 가져옵니다."""
    try:
        metadata_json = Variable.get(LRS_METADATA_VARIABLE)
    except Exception as e:
        raise ValueError(f"Variable '{LRS_METADATA_VARIABLE}'를 찾을 수 없습니다: {str(e)}")
    
    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Variable '{LRS_METADATA_VARIABLE}'의 JSON 형식이 올바르지 않습니다: {str(e)}")
    
    if not metadata:
        raise ValueError(f"Variable '{LRS_METADATA_VARIABLE}'에 정보가 없습니다.")
    
    # 필수 메타데이터 확인
    required_metadata = ['sys_type', 'schema', 'table', 'column']
    for field in required_metadata:
        if field not in metadata:
            raise ValueError(f"Variable '{LRS_METADATA_VARIABLE}'에 '{field}' 정보가 없습니다.")
    
    return metadata

@task
def execute_sql(sql: str, conn_id: str = POSTGRES_CONN_ID, **context) -> List[Any]:
    """SQL 쿼리를 실행하고 결과를 반환합니다."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    
    # Connection 정보 확인
    try:
        conn = hook.get_connection(conn_id)
    except Exception as e:
        raise ValueError(f"Connection '{conn_id}'를 찾을 수 없습니다: {str(e)}")
    
    # schema 정보 가져오기
    schema = conn.schema
    if not schema:
        metadata = get_metadata_from_variable()
        schema = metadata.get('schema')
        if not schema:
            raise ValueError("schema 정보를 찾을 수 없습니다.")
    
    sql = sql.format(schema=schema)
    return hook.get_records(sql)

@task
def get_connection_metadata_from_variable() -> Dict:
    """Variable에서 metadata 정보를 가져옵니다."""
    return get_metadata_from_variable()

@dag(
    dag_id='LearningRecord_ETL',
    default_args=default_args,
    description='Extract and save LRS statements to JSON files',
    schedule_interval=None,
    catchup=False
)
def lrs_statement_extractor():
    # 저장 경로 가져오기
    try:
        save_folder_path = Variable.get(SAVE_FOLDER_VARIABLE)
    except Exception as e:
        raise ValueError(f"Variable '{SAVE_FOLDER_VARIABLE}'를 찾을 수 없습니다: {str(e)}")

    # Variable에서 metadata 정보 가져오기
    metadata = get_metadata_from_variable()
    
    # 테이블 이름 가져오기
    table_name = metadata.get('table')
    
    @task
    def get_table_info(conn_id: str = POSTGRES_CONN_ID) -> List[Any]:
        """테이블 구조 정보를 가져옵니다."""
        sql = f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{{schema}}' AND table_name = '{table_name}';
        """
        return execute_sql(sql, conn_id=conn_id)
    
    @task
    def get_type_info(**context) -> str:
        """Variable의 metadata에서 type 정보를 가져옵니다."""
        metadata = get_metadata_from_variable()
        sys_type = metadata.get('sys_type')
        if not sys_type:
            raise ValueError(f"Variable '{LRS_METADATA_VARIABLE}'에 'sys_type' 정보가 없습니다.")
        logging.info(f"Using sys_type: {sys_type}")
        return sys_type
    
    @task
    def get_column_info(table_info: List[Any], **context) -> List[str]:
        """Variable의 metadata에서 컬럼 정보를 가져옵니다."""
        metadata = get_metadata_from_variable()
        
        # 테이블의 실제 컬럼 이름 확인
        column_names = [col[0] for col in table_info]
        logging.info(f"Available columns: {column_names}")
        
        # metadata에서 컬럼 정보 가져오기
        columns = metadata.get('column')
        if not columns:
            raise ValueError(f"Variable '{LRS_METADATA_VARIABLE}'에 'column' 정보가 없습니다.")
        
        # 필수 컬럼이 테이블에 있는지 확인
        for col in REQUIRED_COLUMNS:
            if col not in column_names:
                raise ValueError(f"필수 컬럼 '{col}'이 테이블에 없습니다.")
        
        # 필수 컬럼이 포함되어 있는지 확인
        if not REQUIRED_COLUMNS.issubset(set(columns)):
            raise ValueError(f"필수 컬럼 {REQUIRED_COLUMNS}이 metadata의 'column' 정보에 포함되어 있지 않습니다.")
        
        logging.info(f"Using columns: {columns}")
        return columns

    @task
    def get_last_processed_id(**context) -> int:
        """마지막으로 처리된 ID를 가져옵니다."""
        # Variable에서 마지막 처리된 ID를 가져오거나 기본값 0 사용
        try:
            last_id = Variable.get("lrs_last_processed_id", default_var=0)
            return int(last_id)
        except (ValueError, TypeError):
            return 0

    @task
    def prepare_base_path(type_value: str, **context) -> str:
        """저장 경로를 준비합니다."""
        if not type_value:
            raise ValueError("타입 정보를 찾을 수 없습니다.")
            
        # LRS 저장 경로 설정
        base_path = Path(save_folder_path) / type_value
        
        # 기본 디렉토리 생성
        base_path.mkdir(parents=True, exist_ok=True)
        
        return str(base_path)

    @task
    def process_statements(columns: List[str], base_path: str, last_id: int, conn_id: str = POSTGRES_CONN_ID, **context) -> Tuple[int, int, int]:
        """
        데이터베이스에서 직접 statement를 처리하고 JSON 파일로 저장합니다.
        XCom을 통해 대용량 데이터를 전달하지 않고 직접 처리합니다.
        
        Returns:
            Tuple[int, int, int]: (max_id, 성공 개수, 실패 개수)
        """
        hook = PostgresHook(postgres_conn_id=conn_id)
        
        # Connection 정보 확인
        try:
            conn = hook.get_connection(conn_id)
        except Exception as e:
            raise ValueError(f"Connection '{conn_id}'를 찾을 수 없습니다: {str(e)}")
        
        # schema 정보 가져오기
        schema = conn.schema
        if not schema:
            metadata = get_metadata_from_variable()
            schema = metadata.get('schema')
            if not schema:
                raise ValueError("schema 정보를 찾을 수 없습니다.")
        
        # 테이블 이름 가져오기
        metadata = get_metadata_from_variable()
        table_name = metadata.get('table')
        
        # 쿼리 생성
        column_list = ', '.join(columns)
        query = f"""
            SELECT {column_list}
            FROM {schema}.{table_name}
            WHERE id > {last_id}
            ORDER BY id ASC
            LIMIT 500;
        """
        
        # 카운터 초기화
        error_count = 0
        success_count = 0
        max_id = last_id
        
        # 데이터베이스에서 직접 데이터 가져와서 처리
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                
                # 컬럼 이름 가져오기
                column_names = [desc[0] for desc in cursor.description]
                
                # 결과 개수 확인
                results = cursor.fetchall()
                total_count = len(results)
                
                if not results:
                    logging.info("처리할 데이터가 없습니다.")
                    return last_id, 0, 0
                
                logging.info(f"총 {total_count}개의 데이터를 처리합니다.")
                
                # 결과 처리
                for i, row in enumerate(results):
                    try:
                        # 데이터 딕셔너리 생성
                        row_dict = {column_names[idx]: value for idx, value in enumerate(row)}
                        
                        # ID 값 업데이트
                        row_id = row_dict.get('id')
                        if row_id and isinstance(row_id, (int, float)) and int(row_id) > max_id:
                            max_id = int(row_id)
                        
                        # statement_id(uuid) 확인
                        statement_id = row_dict.get('statement_id')
                        if not statement_id:
                            logging.warning(f"statement_id가 없는 데이터 발견 (ID: {row_dict.get('id')})")
                            error_count += 1
                            continue
                        
                        # timestamp 처리
                        statement_timestamp = row_dict.get('timestamp')
                        if not statement_timestamp:
                            logging.warning(f"timestamp가 없는 데이터 발견 (ID: {row_dict.get('id')}, statement_id: {statement_id})")
                            error_count += 1
                            continue
                        
                        # timestamp 파싱 시도
                        try:
                            if isinstance(statement_timestamp, str):
                                datetime.fromisoformat(statement_timestamp.replace('Z', '+00:00'))
                            # datetime 객체는 이미 유효한 timestamp이므로 추가 검증 불필요
                        except (ValueError, TypeError):
                            logging.warning(f"timestamp 파싱 실패 (ID: {row_dict.get('id')}, statement_id: {statement_id}, timestamp: {statement_timestamp})")
                            error_count += 1
                            continue
                            
                        # JSON 데이터 준비
                        statement_data = row_dict.copy()
                        statement_data['timestamp'] = statement_timestamp.isoformat() if isinstance(statement_timestamp, datetime) else statement_timestamp
                        
                        # 날짜별 디렉토리 구조 생성
                        month, day = get_month_day_from_timestamp(statement_timestamp)
                        output_dir = Path(base_path) / month / day
                        output_dir.mkdir(parents=True, exist_ok=True)
                        
                        # 파일 저장
                        file_path = output_dir / f"{statement_id}.json"
                        with open(file_path, 'w') as f:
                            json.dump(statement_data, f, indent=2)
                            
                        # 성공 카운트 증가
                        success_count += 1
                    except Exception as e:
                        logging.error(f"데이터 처리 중 오류 발생: {e}", exc_info=True)
                        error_count += 1
                        continue
        
        logging.info(f"마지막 ID 값: {max_id}")
        logging.info(f"처리 결과: 총 {total_count}개 중 성공 {success_count}개, 실패 {error_count}개")
        
        return max_id, success_count, error_count

    def get_month_day_from_timestamp(timestamp) -> tuple:
        """timestamp에서 월과 일을 추출합니다."""
        if isinstance(timestamp, datetime):
            return timestamp.strftime('%m'), timestamp.strftime('%d')
        else:
            # timestamp가 문자열인 경우
            try:
                # ISO 형식의 'Z' 처리 (UTC 표시)
                if isinstance(timestamp, str) and 'Z' in timestamp:
                    timestamp = timestamp.replace('Z', '+00:00')
                dt = datetime.fromisoformat(timestamp)
                return dt.strftime('%m'), dt.strftime('%d')
            except (ValueError, TypeError):
                # 파싱할 수 없는 경우 현재 날짜 사용
                now = datetime.now()
                return now.strftime('%m'), now.strftime('%d')

    @task
    def save_last_processed_id(result_tuple: Tuple[int, int, int], **context) -> int:
        """
        처리된 결과 중 가장 큰 ID 값을 저장합니다.
        이 값은 다음 실행 시 이 ID보다 큰 데이터만 조회하는 데 사용됩니다.
        """
        max_id, success_count, error_count = result_tuple
        
        # 현재 저장된 마지막 ID 가져오기
        try:
            last_processed_id = int(Variable.get("lrs_last_processed_id", default_var=0))
        except (ValueError, TypeError):
            last_processed_id = 0
        
        if max_id > last_processed_id:
            logging.info(f"마지막으로 처리된 ID 업데이트: {last_processed_id} -> {max_id}")
            Variable.set("lrs_last_processed_id", str(max_id))
            return max_id
        return last_processed_id

    @task
    def log_processing_stats(result_tuple: Tuple[int, int, int], **context) -> None:
        """처리 통계를 로깅합니다."""
        max_id, success_count, error_count = result_tuple
        total_count = success_count + error_count
        
        separator = "=" * 50
        logging.info(separator)
        logging.info("처리 완료 요약")
        logging.info(separator)
        logging.info(f"마지막 ID: {max_id}")
        logging.info(f"총 처리 건수: {total_count}개")
        
        if total_count > 0:
            success_rate = (success_count / total_count) * 100
            error_rate = (error_count / total_count) * 100
            logging.info(f"성공 건수: {success_count}개 ({success_rate:.1f}% 성공)")
            logging.info(f"실패 건수: {error_count}개 ({error_rate:.1f}% 실패)")
        else:
            logging.info("성공 건수: 0개 (0.0% 성공)")
            logging.info("실패 건수: 0개 (0.0% 실패)")
            
        logging.info(separator)
        
        # 에러 건수가 있으면 경고 메시지 출력
        if error_count > 0:
            logging.warning(f"경고: {error_count}개의 statement가 처리되지 않았습니다.")
        
        return None

    # Task 인스턴스 생성
    table_info = get_table_info()
    type_value = get_type_info()
    base_path = prepare_base_path(type_value)
    
    columns = get_column_info(table_info)
    last_id = get_last_processed_id()
    
    # 데이터베이스에서 직접 처리
    process_result = process_statements(columns, base_path, last_id)
    save_id = save_last_processed_id(process_result)
    stats = log_processing_stats(process_result)
    
    # 타입 정보 의존성
    type_value >> base_path
    
    # 테이블 정보 의존성
    table_info >> columns
    
    # 쿼리 및 데이터 처리 의존성
    columns >> process_result
    last_id >> process_result
    base_path >> process_result
    process_result >> save_id
    process_result >> stats

dag = lrs_statement_extractor()