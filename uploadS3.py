import boto3

# S3 설정
bucket_name = "hugehugebucket"
s3_key = "parquet/output.parquet"  #S3에 저장할 경로

# 업로드
s3 = boto3.client('s3')
s3.upload_file("output.parquet", bucket_name, s3_key)

print(f"S3 업로드 완료: s3://{bucket_name}/{s3_key}")
