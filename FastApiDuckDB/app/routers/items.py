from fastapi import APIRouter, HTTPException
from app.crud import item_crud
from app.schemas.item import Item

router = APIRouter()

@router.get("/")
def read_items():
    return item_crud.get_all_items()

@router.post("/")
def create_item(item: Item):
    return item_crud.add_item(item)

@router.put("/{seq}")
def update(seq: int, item: Item):
    return item_crud.update_item(seq, item)

@router.delete("/{seq}")
def delete(seq: int):
    return item_crud.delete_item(seq)
