from pydantic import BaseModel
from typing import Optional

class Item(BaseModel):
    seq: int
    column1: str
    column2: int
    column3: int
    column4: str
    column5: str
