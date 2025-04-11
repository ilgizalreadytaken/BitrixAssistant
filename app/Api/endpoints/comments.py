# api/endpoints/comments.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/{task_id}")
def get_comments(task_id: int):
    return {"task_id": task_id, "comments": []}

@router.post("/{task_id}")
def add_comment(task_id: int, comment: str):
    return {"message": "Comment added", "task_id": task_id, "comment": comment}
