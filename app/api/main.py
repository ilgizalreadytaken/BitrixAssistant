import os
import json
import uvicorn
from fastapi import FastAPI, Request, Depends
from dotenv import load_dotenv
from aiogram import Bot
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine
)
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import (
    Column, Integer, String, Text,
    BigInteger, Boolean, DateTime, ForeignKey
)
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
from pyngrok import ngrok

# 1. Загрузка переменных окружения
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# 2. Инициализация Telegram бота
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# 3. Инициализация FastAPI
app = FastAPI(title="Bitrix24 Event API", version="1.0")

# 4. Настройки подключения к PostgreSQL через SQLAlchemy (асинхронно)
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


# ===================== MODELS =====================

# Таблица users
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(BigInteger, unique=True, index=True)  # Telegram chat/user ID
    bitrix_user_id = Column(String(255))
    first_name = Column(String(255))
    last_name = Column(String(255))
    access_token = Column(String(255))
    refresh_token = Column(String(255))

    # Связь с уведомлениями
    notifications = relationship("Notification", back_populates="user")
    # Связь с настройками уведомлений (1 к 1 или 1 ко многим, в зависимости от логики)
    notification_settings = relationship("UserNotificationSettings", back_populates="user",
                                         uselist=False)  # uselist=False для связи один-к-одному


# Таблица notifications
class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    event_type = Column(String(255))
    message = Column(Text)
    sent_at = Column(DateTime(timezone=True), server_default=func.now())

    # Связь с пользователем
    user = relationship("User", back_populates="notifications")


# Таблица user_notification_settings
class UserNotificationSettings(Base):
    __tablename__ = "user_notification_settings"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    notify_task_creation = Column(Boolean, default=True)
    notify_task_update = Column(Boolean, default=True)
    notify_task_comment = Column(Boolean, default=True)
    notify_task_assignment = Column(Boolean, default=True)

    # Связь с пользователем
    user = relationship("User", back_populates="notification_settings")


# Функция для инициализации базы данных (создание таблиц)
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# Событие старта приложения
@app.on_event("startup")
async def on_startup():
    await init_db()
    print("База данных инициализирована")


# Зависимость для получения сессии БД
async def get_db():
    async with SessionLocal() as session:
        yield session


# Функция отправки уведомления в Telegram
async def send_telegram_notification(user_id: int, message: str):
    """Пытаемся отправить сообщение в Телеграм по chat_id=user_id."""
    try:
        await bot.send_message(chat_id=user_id, text=message)
    except Exception as e:
        print(f"Ошибка при отправке сообщения пользователю {user_id}: {e}")


# Основной обработчик событий от Bitrix24
@app.post("/handler")
async def bitrix_event_handler(
        request: Request,
        db: AsyncSession = Depends(get_db)
):
    """
    Принимает POST-запросы от Bitrix24 в формате JSON.
    Пример JSON:
    {
      "event": "task_created",
      "data": {
         "TASK_ID": 123,
         "TITLE": "Новая задача",
         "DESCRIPTION": "Описание задачи",
         "USER_ID": 123456789   # Telegram ID
      }
    }
    """
    try:
        data = await request.json()
    except Exception as e:
        return {"status": "error", "message": "Неверный JSON: " + str(e)}

    print("Получено событие от Bitrix24:")
    print(json.dumps(data, indent=2, ensure_ascii=False))

    event_type = data.get("event", "unknown")
    task_data = data.get("data", {})

    # Ожидаем TELEGRAM chat_id в поле USER_ID
    user_id = task_data.get("USER_ID")
    if not user_id:
        return {"status": "error", "message": "USER_ID не указан"}

    # Формируем текст уведомления
    if event_type == "task_created":
        msg = (
            f"Создана новая задача:\n"
            f"ID: {task_data.get('TASK_ID')}\n"
            f"Название: {task_data.get('TITLE')}\n"
            f"Описание: {task_data.get('DESCRIPTION')}"
        )
    elif event_type == "task_updated":
        msg = (
            f"Обновлена задача:\n"
            f"ID: {task_data.get('TASK_ID')}\n"
            f"Новые данные: {json.dumps(task_data, ensure_ascii=False)}"
        )
    else:
        msg = f"Получено событие: {event_type} для задачи {task_data.get('TASK_ID')}"

    '''
    # Проверяем, есть ли уже такой пользователь в users
    # Если нет - нужно либо добавить, либо вернуть ошибку
    existing_user = await db.execute(
        # Используем текстовый запрос или ORM-запрос
        # ORM-вариант:
        # select(User).filter(User.telegram_id == user_id)
        # Для иллюстрации - запрос ORM:
        # result = await db.execute(select(User).where(User.telegram_id == user_id))
        # user_in_db = result.scalars().first()
        # Или query = text("SELECT * FROM users WHERE telegram_id = :uid")

        # Упрощённый ORM-вариант:
        """
        SELECT users
        """
    )
    # Чтобы не усложнять, предлагаем просто вставку "если нет", если это тестовый режим.

    # Вставляем уведомление
    new_notification = Notification(
        user_id=user_id,  # предполагается, что user_id - PK из users, но вы используете telegram_id
        event_type=event_type,
        message=msg
    )
    db.add(new_notification)
    try:
        await db.commit()
    except Exception as e:
        # Если произошла ошибка внешнего ключа - нужно добавить запись в users
        print("Ошибка при сохранении уведомления:", e)
        return {"status": "error", "message": str(e)}
    '''

    # Отправляем уведомление в Telegram
    await send_telegram_notification(user_id, msg)

    return {"status": "success", "message": "Событие обработано"}


if __name__ == "__main__":
    public_url = ngrok.connect(8000, bind_tls=True)
    print("Public URL:", public_url)

    uvicorn.run(app, host="0.0.0.0", port=8000)
