# api/main.py
from fastapi import FastAPI
from .settings import settings
from .endpoints import auth, users, tasks, deals, comments, notifications

app = FastAPI(title=settings.PROJECT_NAME)

# Регистрация маршрутов с указанием префиксов и тегов для документации
app.include_router(auth.router, prefix="/auth", tags=["auth"])
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(tasks.router, prefix="/tasks", tags=["tasks"])
app.include_router(deals.router, prefix="/deals", tags=["deals"])
app.include_router(comments.router, prefix="/comments", tags=["comments"])
app.include_router(notifications.router, prefix="/notifications", tags=["notifications"])

@app.get("/")
def read_root():
    return {"message": "Welcome to BitrixAssistant API"}
