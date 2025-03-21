import pandas as pd

df = pd.read_parquet("output.parquet")#확인하고자 하는 parquet 파일 추가

# 컬럼명에서 공백 제거
df.columns = df.columns.str.strip()
print("컬럼 확인:", df.columns)

# 특정 컬럼 값 확인
print(df[['Column5']].dropna())
