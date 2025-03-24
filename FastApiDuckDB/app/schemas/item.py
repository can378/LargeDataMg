from pydantic import BaseModel

class Item(BaseModel):
    seq: int
    column1: str
    column2: int
    column3: int
    column4: str
    column5: str