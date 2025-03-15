import asyncio
from connection import Database

async def test_connection():
    db = Database()  # Создаем экземпляр базы данных
    await db.connect()  # Подключаемся к базе данных
    await db.close()  # Закрываем соединение

if __name__ == '__main__':
    asyncio.run(test_connection())  # Запуск асинхронного подключения
