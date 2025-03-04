import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import json

print('데이터 분석 시작...')
execution_date = os.environ.get('EXECUTION_DATE')
data_dir = os.environ.get('DATA_DIR')
input_path = f'{data_dir}/processed_data_{execution_date}.csv'
output_path = f'{data_dir}/analysis_results_{execution_date}.json'
viz_path = f'{data_dir}/correlation_heatmap_{execution_date}.png'

# 데이터 로드
df = pd.read_csv(input_path)

# 기본 통계 계산
stats = {
    'record_count': len(df),
    'columns': list(df.columns),
    'numeric_stats': {}
}

numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
for col in numeric_cols:
    stats['numeric_stats'][col] = {
        'mean': df[col].mean(),
        'median': df[col].median(),
        'std': df[col].std(),
        'min': df[col].min(),
        'max': df[col].max()
    }

# 상관관계 분석
correlation_matrix = df.select_dtypes(include=['float64', 'int64']).corr().round(2)
plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
plt.title('Feature Correlation Heatmap')
plt.tight_layout()
plt.savefig(viz_path)

# 상관관계 결과 저장
stats['correlation_matrix'] = correlation_matrix.to_dict()
stats['visualization_path'] = viz_path

# 결과 저장
with open(output_path, 'w') as f:
    json.dump(stats, f, indent=2, default=str)

print(f'데이터 분석 완료: 분석 결과가 {output_path}에 저장되었습니다.')
print(f'시각화 결과가 {viz_path}에 저장되었습니다.') 