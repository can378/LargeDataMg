from fastapi import APIRouter, HTTPException
from app.schemas.item import Item
from app.crud.item import ItemCRUD
from app.utils.s3_parquet import get_parquet_df, save_parquet_df
import pandas as pd
import time

router = APIRouter(prefix="/items", tags=["items"])

@router.get("/")
def read_items():
    start = time.time()  #시작 시간
    data = ItemCRUD.get_all_items()
    duration = time.time() - start  #처리 시간

    print(f"데이터 조회 시간: {duration:.3f}초")

    return {
        "duration": f"{duration:.3f}초",
        "count": len(data),
        "items": data
    }

@router.post("/")
def create_item(item: Item):
    """데이터터 추가 (Athena)"""
    return ItemCRUD.add_item(item)

@router.put("/{seq}")
def update_item(seq: int, new_item: Item):
    df = get_parquet_df()

    # seq 컬럼 int로 변환 (NaN 방지)
    df["seq"] = pd.to_numeric(df["seq"], errors="coerce").fillna(-1).astype(int)

    if not (df["seq"] == seq).any():
        raise HTTPException(status_code=404, detail="Item not found")

    df = df[df["seq"] != seq]
    df = pd.concat([df, pd.DataFrame([new_item.dict()])], ignore_index=True)
    save_parquet_df(df)

    return {"message": f"Item {seq} updated successfully"}


@router.delete("/{seq}")
def delete_item(seq: float):
    df = get_parquet_df()

    # 타입 맞추기
    seq = int(seq)
    df["seq"] = df["seq"].astype(int)

    if not (df["seq"] == seq).any():
        raise HTTPException(status_code=404, detail="Item not found")

    df = df[df["seq"] != seq]
    save_parquet_df(df)

    return {"message": f"Item {seq} deleted successfully"}
