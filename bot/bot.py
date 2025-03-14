import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command

# Настройка логирования для отслеживания работы бота
logging.basicConfig(level=logging.INFO)

# Инициализация бота с вашим токеном
bot = Bot(token="8179379861:AAEoKsITnDaREJINuHJu4qXONwxTIlSncxc")
dp = Dispatcher()

# Хэндлер для команды /start
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Привет! Добро пожаловать в тестового бота на aiogram 3.18.0.")

# Хэндлер для команды /help
@dp.message(Command("help"))
async def help_handler(message: types.Message):
    help_text = (
        "Доступные команды:\n"
        "/start - Запуск бота\n"
        "/help - Справка\n"
        "Просто отправьте любое сообщение, и я повторю его!"
    )
    await message.answer(help_text)

# Хэндлер для любых текстовых сообщений (используем фильтр F.text)
@dp.message(F.text)
async def echo_handler(message: types.Message):
    await message.answer(f"Вы сказали: {message.text}")

# Основная асинхронная функция для запуска поллинга обновлений
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
