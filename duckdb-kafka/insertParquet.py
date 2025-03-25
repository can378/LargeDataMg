import duckdb

#Parquet → DuckDB로 직접 insert
con = duckdb.connect("data.duckdb")
con.execute("DROP TABLE IF EXISTS TB_WEB_RACK_MST")
con.execute("""
CREATE TABLE TB_WEB_RACK_MST AS 
SELECT * FROM 'output.parquet'
""")

