from app.utils.duckdb_utils import get_connection
from app.schemas.item import Item
import pandas as pd
import numpy as np

def get_all_items():
    con = get_connection()
    df = con.execute("SELECT * FROM large_data_table").df()
    df = df.replace({np.nan: None})
    return df.to_dict(orient="records")

def add_item(item: Item):
    con = get_connection()
    con.execute(f"""
        INSERT INTO large_data_table VALUES (
            {item.seq}, '{item.column1}', {item.column2}, 
            {item.column3}, '{item.column4}', '{item.column5}'
        )
    """)
    return {"message": "Item inserted successfully."}

def update_item(seq: int, new_item: Item):
    con = get_connection()
    con.execute(f"DELETE FROM large_data_table WHERE seq = {seq}")
    return add_item(new_item)

def delete_item(seq: int):
    con = get_connection()
    con.execute(f"DELETE FROM large_data_table WHERE seq = {seq}")
    return {"message": f"Item {seq} deleted successfully."}
