# api/endpoints/users.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def list_users():
    return {"users": []}

@router.get("/{user_id}")
def get_user(user_id: int):
    return {"user_id": user_id, "name": "John Doe"}

@router.post("/")
def create_user(username: str):
    return {"message": "User created", "username": username}

@router.put("/{user_id}")
def update_user(user_id: int, username: str):
    return {"message": "User updated", "user_id": user_id}

@router.delete("/{user_id}")
def delete_user(user_id: int):
    return {"message": "User deleted", "user_id": user_id}
