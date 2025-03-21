import boto3
import time
from app.schemas.item import Item

ATHENA_DB = "hugedatabase"
ATHENA_OUTPUT = "s3://hugehugebucket/athena-results/"
athena_client = boto3.client("athena")

class ItemCRUD:
    @staticmethod
    def query_athena(sql_query):
        """Athena에서 SQL 쿼리를 실행하고 결과를 반환"""
        response = athena_client.start_query_execution(
            QueryString=sql_query,
            QueryExecutionContext={"Database": ATHENA_DB},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )
        query_execution_id = response["QueryExecutionId"]
        
        # 쿼리 실행 완료 대기
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = result["QueryExecution"]["Status"]["State"]
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)

        if state != "SUCCEEDED":
            raise Exception(f"Athena Query Failed: {state}")

        return query_execution_id  # INSERT/DELETE는 결과 필요 없음

    @staticmethod
    def get_all_items():
        """Athena에서 전체 아이템 조회"""
        sql = "SELECT * FROM large_data_table"
        return ItemCRUD.query_athena(sql)

    @staticmethod
    def add_item(item: Item):
        """Athena에 새로운 아이템 추가 (INSERT)"""
        sql = f"""
        INSERT INTO large_data_table (seq, column1, column2, column3, column4, column5)
        VALUES ({item.seq}, '{item.column1}', {item.column2}, {item.column3}, '{item.column4}', '{item.column5}')
        """
        return ItemCRUD.query_athena(sql)

    @staticmethod
    def delete_item(seq: float):
        """Athena 테이블에서 특정 아이템 삭제"""
        sql = f"DELETE FROM large_data_table WHERE seq = {seq}"  # Soft Delete가 필요할 수도 있음
        return ItemCRUD.query_athena(sql)
