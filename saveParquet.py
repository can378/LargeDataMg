import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

file_path = "./20250313_대용량 데이터 전송 테스트.txt"

# 모든 컬럼을 string으로 읽기
df = pd.read_csv(file_path, sep="|", skiprows=[1],dtype=str)

# Unnamed 컬럼 제거
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# 컬럼명 공백 제거
df.columns = df.columns.str.strip()

# , 제거 및 숫자 확인. 알맞게 변환
for col in ["seq", "Column2", "Column3"]:
    df[col] = df[col].str.strip().str.replace(",", "")  # 공백 & 콤마 제거

    # 숫자 아닌 값들 출력해보기
    # non_numeric_seq = df[~df["seq"].str.match(r"^\d+$", na=False)]
    # print("숫자 아님 (seq 컬럼):")
    # print(non_numeric_seq["seq"].unique())

    if df[col].isnull().any():
        raise ValueError(f"'{col}' 컬럼에 결측치가 있다")
    
    if col == "seq":
        if not df[col].str.isnumeric().all():
            raise ValueError(f"'{col}' 컬럼에 숫자가 아닌 값이 있다")
        df[col] = df[col].astype(int)
    else:
        try:
            df[col] = df[col].astype(float)
        except ValueError:
            raise ValueError(f"'{col}' 컬럼에 변환 불가능한 값이 있다다")

# Parquet 저장
parquet_path = "./output.parquet"
pq.write_table(pa.Table.from_pandas(df), parquet_path)

print(f"Parquet 저장 완료: {parquet_path}")
