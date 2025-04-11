# api/settings.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "BitrixAssistant"
    BITRIX_API_URL: str = "https://api.bitrix24.com"
    TELEGRAM_BOT_TOKEN: str = "your_telegram_bot_token_here"
    DATABASE_URL: str = "postgresql://user:password@localhost:5432/dbname"

    class Config:
        env_file = ".env"

settings = Settings()