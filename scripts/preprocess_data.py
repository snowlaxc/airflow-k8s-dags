import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import pickle

print('데이터 전처리 시작...')
execution_date = os.environ.get('EXECUTION_DATE')
data_dir = os.environ.get('DATA_DIR')
input_path = f'{data_dir}/raw_data_{execution_date}.csv'
output_path = f'{data_dir}/processed_data_{execution_date}.csv'
model_path = f'{data_dir}/scaler_{execution_date}.pkl'

# 데이터 로드
df = pd.read_csv(input_path)
print(f'로드된 데이터 크기: {df.shape}')

# 결측치 처리
df = df.fillna(df.mean(numeric_only=True))

# 이상치 처리 (IQR 방식)
Q1 = df['value1'].quantile(0.25)
Q3 = df['value1'].quantile(0.75)
IQR = Q3 - Q1
df = df[~((df['value1'] < (Q1 - 1.5 * IQR)) | (df['value1'] > (Q3 + 1.5 * IQR)))]

# 특성 정규화
numeric_features = ['value1', 'value2']
scaler = StandardScaler()
df[numeric_features] = scaler.fit_transform(df[numeric_features])

# 범주형 변수 원-핫 인코딩
df = pd.get_dummies(df, columns=['category'])

# 전처리된 데이터 저장
df.to_csv(output_path, index=False)
with open(model_path, 'wb') as f:
    pickle.dump(scaler, f)

print(f'데이터 전처리 완료: 처리된 데이터가 {output_path}에 저장되었습니다.')
print(f'스케일러 모델이 {model_path}에 저장되었습니다.') 