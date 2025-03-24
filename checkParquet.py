import pandas as pd

# 출력 옵션 설정
pd.set_option('display.max_rows', None)  #모든 행 출력
pd.set_option('display.max_columns', None)  #모든 열 출력
pd.set_option('display.width', None)  #자동 줄바꿈 방지
pd.set_option('display.max_colwidth', None)  #컬럼 내용 최대 길이 제한 해제


df = pd.read_parquet("output.parquet")#확인하고자 하는 parquet 파일

# 컬럼명에서 공백 제거
df.columns = df.columns.str.strip()
print("컬럼:", df.columns)


#print(df[['Column5']].dropna())
print(df);