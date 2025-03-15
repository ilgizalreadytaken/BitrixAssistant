import asyncpg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

class Database:
    def __init__(self):
        self.dsn = self.create_dsn()  # Строка подключения
        self.conn = None

    def create_dsn(self):
        # Получаем параметры подключения из переменных окружения
        DB_HOST = os.getenv('DB_HOST')
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')
        DB_NAME = os.getenv('DB_NAME')

        # Формируем строку подключения
        return f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'

    async def connect(self):
        try:
            # Устанавливаем соединение с базой данных
            self.conn = await asyncpg.connect(self.dsn)
            print("Подключение к базе данных успешно установлено.")

            # Выполним простой запрос для проверки
            result = await self.conn.fetch('SELECT 1')
            if result:
                print("База данных успешно отвечает на запрос.")
            else:
                print("База данных не ответила на запрос.")

        except Exception as e:
            print(f"Ошибка при подключении к базе данных: {e}")

    async def close(self):
        # Закрываем соединение
        if self.conn:
            await self.conn.close()
            print("Соединение с базой данных закрыто.")

    async def fetch(self, query: str, *args):
        # Выполняем запрос на выборку данных
        return await self.conn.fetch(query, *args)

    async def execute(self, query: str, *args):
        # Выполняем запрос на изменение данных
        return await self.conn.execute(query, *args)