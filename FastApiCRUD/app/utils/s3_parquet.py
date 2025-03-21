import boto3
import io
import pandas as pd
import pyarrow.parquet as pq

S3_BUCKET = "hugehugebucket"
PARQUET_FILE_KEY = "parquet/output.parquet"

s3 = boto3.client("s3")

def get_parquet_df():
    """S3에서 Parquet 데이터를 가져와 Pandas DataFrame으로 변환"""
    response = s3.get_object(Bucket=S3_BUCKET, Key=PARQUET_FILE_KEY)
    body = response["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    
    df = table.to_pandas()
    df["seq"] = pd.to_numeric(df["seq"], errors="coerce").astype("Int64")

    return df

def save_parquet_df(df):
    """Pandas DataFrame을 Parquet 파일로 변환 후 S3에 저장"""
    # 🔥 seq 컬럼이 존재하면 int64로 변환 (float 저장 방지)
    if "seq" in df.columns:
        df["seq"] = pd.to_numeric(df["seq"], errors="coerce").fillna(0).astype("int64")

    # Parquet로 저장
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Body=buf, Bucket=S3_BUCKET, Key=PARQUET_FILE_KEY)
