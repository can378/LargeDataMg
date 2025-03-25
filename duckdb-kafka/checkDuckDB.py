import duckdb

con = duckdb.connect("data.duckdb")
results = con.execute("SELECT * FROM TB_WEB_RACK_MST").fetchall()

for row in results:
    print(row)
