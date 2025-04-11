# api/endpoints/tasks.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def list_tasks():
    return {"tasks": []}

@router.get("/{task_id}")
def get_task(task_id: int):
    return {"task_id": task_id, "title": "Sample Task"}

@router.post("/")
def create_task(title: str, description: str = None):
    return {"message": "Task created", "title": title}

@router.put("/{task_id}")
def update_task(task_id: int, title: str = None, description: str = None):
    return {"message": "Task updated", "task_id": task_id}

@router.delete("/{task_id}")
def delete_task(task_id: int):
    return {"message": "Task deleted", "task_id": task_id}
