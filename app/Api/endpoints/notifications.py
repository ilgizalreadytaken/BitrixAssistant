# api/endpoints/notifications.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def list_notifications():
    return {"notifications": []}

@router.post("/subscribe")
def subscribe(telegram_id: int):
    return {"message": "Subscribed", "telegram_id": telegram_id}

@router.post("/unsubscribe")
def unsubscribe(telegram_id: int):
    return {"message": "Unsubscribed", "telegram_id": telegram_id}
