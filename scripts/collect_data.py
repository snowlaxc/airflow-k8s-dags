import os
import pandas as pd
import random
from datetime import datetime

print("데이터 수집 시작...")
execution_date = os.environ.get('EXECUTION_DATE')
data_dir = os.environ.get('DATA_DIR')
output_path = f"{data_dir}/raw_data_{execution_date}.csv"

# 샘플 데이터 생성
num_records = 1000
data = {
    'id': list(range(1, num_records + 1)),
    'timestamp': [datetime.now().strftime("%Y-%m-%d %H:%M:%S") for _ in range(num_records)],
    'value1': [random.uniform(10, 100) for _ in range(num_records)],
    'value2': [random.uniform(100, 1000) for _ in range(num_records)],
    'category': [random.choice(['A', 'B', 'C', 'D']) for _ in range(num_records)]
}

# 데이터 저장
df = pd.DataFrame(data)
os.makedirs(data_dir, exist_ok=True)
df.to_csv(output_path, index=False)

print(f"데이터 수집 완료: {num_records}개 레코드가 {output_path}에 저장되었습니다.") 