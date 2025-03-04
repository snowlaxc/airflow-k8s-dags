import os
import pandas as pd
import json
import shutil
from datetime import datetime

print('결과 저장 시작...')
execution_date = os.environ.get('EXECUTION_DATE')
data_dir = os.environ.get('DATA_DIR')
processed_path = f'{data_dir}/processed_data_{execution_date}.csv'
analysis_path = f'{data_dir}/analysis_results_{execution_date}.json'
archive_dir = f'{data_dir}/archive/{execution_date}'

# 결과 요약 작성
summary_path = f'{data_dir}/pipeline_summary_{execution_date}.txt'

# 처리된 데이터 정보
df = pd.read_csv(processed_path)
row_count = len(df)
col_count = len(df.columns)

# 분석 결과 정보
with open(analysis_path, 'r') as f:
    analysis_results = json.load(f)

# 요약 파일 작성
with open(summary_path, 'w') as f:
    f.write(f'데이터 파이프라인 실행 요약 - {execution_date}\n')
    f.write(f'===================================\n\n')
    f.write(f'처리된 레코드 수: {row_count}\n')
    f.write(f'특성 수: {col_count}\n\n')
    f.write(f'주요 통계:\n')
    
    for col, stats in analysis_results.get('numeric_stats', {}).items():
        f.write(f'  {col}:\n')
        for stat_name, stat_value in stats.items():
            f.write(f'    {stat_name}: {stat_value}\n')
        f.write('\n')
    
    f.write(f'\n파이프라인 완료 시간: {datetime.now()}\n')

# 결과 아카이브 (선택 사항)
os.makedirs(archive_dir, exist_ok=True)
for file_name in os.listdir(data_dir):
    if execution_date in file_name and os.path.isfile(os.path.join(data_dir, file_name)):
        shutil.copy(os.path.join(data_dir, file_name), os.path.join(archive_dir, file_name))

print(f'결과 저장 완료: 파이프라인 요약이 {summary_path}에 저장되었습니다.')
print(f'결과 파일이 {archive_dir}에 아카이브되었습니다.') 