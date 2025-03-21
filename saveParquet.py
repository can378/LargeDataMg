import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

file_path = "./20250313_대용량 데이터 전송 테스트.txt"

# 모든 컬럼을 string으로 처리
df = pd.read_csv(file_path, sep="|", dtype=str)

# unnamed 컬럼 제거
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

df["seq"] = pd.to_numeric(df["seq"], errors="coerce")
df["Column2"] = pd.to_numeric(df["Column2"], errors="coerce")
df["Column3"] = pd.to_numeric(df["Column3"], errors="coerce")

# 컬럼명에서 공백 제거
df.columns = df.columns.str.strip()


# Parquet 저장
parquet_path = "./output.parquet"
pq.write_table(pa.Table.from_pandas(df), parquet_path)

print(f"Parquet 저장 완료: {parquet_path}")
