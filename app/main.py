import asyncio
import json
import logging
from urllib.parse import parse_qs
from time import time
from typing import Dict, Optional

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

# Конфигурация
BITRIX_CLIENT_ID = "local.68122d64ea29a1.85490975"
BITRIX_CLIENT_SECRET = "sFQq1zjJ2V4EAjAnP842GwOKKJT5Tb0WJ25btXtC3IR2VVg72d"
REDIRECT_URI = "https://mybitrixbot.ru/callback"
WEBHOOK_DOMAIN = "https://mybitrixbot.ru"
TELEGRAM_TOKEN = "8179379861:AAEoKsITnDaREJINuHJu4qXONwxTIlSncxc"
BITRIX_DOMAIN = "b24-eu9n9c.bitrix24.ru"

# Хранилища данных
tokens: Dict[str, Dict] = {}  # Хранение данных пользователя для протокола OAuth
member_map: Dict[str, str] = {}
notification_settings: Dict[str, Dict] = {}  # Настройки уведомлений пользователя

# Настройка модуля логирования
logging.basicConfig(level=logging.INFO)

# Инициализация
app = FastAPI()
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


# --- Вспомогательные функции ---
async def refresh_token(chat_id: str) -> bool:
    """Обновление access token, используя refresh token"""
    # Получение данных пользователя
    user_data = tokens.get(chat_id)
    if not user_data:
        return False

    # Процесс обновление access token
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://oauth.bitrix.info/oauth/token/",
                data={
                    "grant_type": "refresh_token",
                    "client_id": BITRIX_CLIENT_ID,
                    "client_secret": BITRIX_CLIENT_SECRET,
                    "refresh_token": user_data["refresh_token"]
                }
            )
            resp.raise_for_status()
            data = resp.json()

            user_data.update({
                "access_token": data["access_token"],
                "refresh_token": data["refresh_token"],
                "expires": int(time()) + int(data["expires_in"])
            })
            return True
    except httpx.HTTPStatusError as e:
        if "invalid_grant" in str(e):
            await bot.send_message(chat_id, "❌ Сессия истекла, выполните /start")
            tokens.pop(chat_id, None)
        return False


async def get_user_info(domain: str, access_token: str) -> Dict:
    """Получение информации о пользователе, включая роли"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://{domain}/rest/user.current.json",
            params={"auth": access_token}
        )
        data = resp.json()

        return {
            "id": data.get("result", {}).get("ID"),
            "is_admin": "ADMIN" in data.get("result", {}).get("ADMIN_LABELS", []),
            "email": data.get("result", {}).get("EMAIL")
        }


async def check_user_exists(domain: str, access_token: str, user_id: int) -> bool:
    """Проверка, существует ли пользователь на портале Битрикс24"""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"https://{domain}/rest/user.get.json",
            params={"auth": access_token, "ID": user_id}
        )
        data = resp.json()
        return data.get("result") is not None


async def register_webhooks(domain: str, access_token: str):
    """Регистрация обработчиков событий"""
    events = [  # все события, которые нужно обрабатывать
        "OnTaskAdd", "OnTaskUpdate", "OnTaskDelete", "OnTaskCommentAdd",
        "OnCrmDealAdd", "OnCrmDealUpdate", "OnCrmDealDelete"
    ]

    async with httpx.AsyncClient() as client:
        for event in events:
            try:
                resp = await client.post(
                    f"https://{domain}/rest/event.bind",
                    data={
                        "event": event,
                        "handler": f"{WEBHOOK_DOMAIN}/callback",
                        "auth": access_token
                    }
                )
                # logging.info(f"Webhook {event} response: {resp.status_code} {resp.text}")
            except Exception as e:
                logging.error(f"Webhook registration error for {event}: {e}")


def parse_form_data(form_data: dict) -> dict:
    """Парсинг полученных данных из Битрикса"""
    result = {}
    for key, value in form_data.items():
        parts = key.split('[')
        current = result
        for part in parts[:-1]:
            part = part.rstrip(']')
            if part not in current:
                current[part] = {}
            current = current[part]
        last_part = parts[-1].rstrip(']')
        current[last_part] = value
    return result


# --- API ---
@app.api_route("/callback", methods=["GET", "POST", "HEAD"])
async def unified_handler(request: Request):
    """Обработка запросов Битрикс"""
    if request.method == "GET":  # регистрация
        return await handle_oauth_callback(request)
    elif request.method == "POST":  # обработка событий
        return await handle_webhook_event(request)
    return JSONResponse({"status": "ok"})


async def handle_oauth_callback(request: Request):
    """Авторизация OAuth 2.0"""
    params = dict(request.query_params)
    # logging.info(f"OAuth callback params: {params}")

    try:
        required = ["code", "state", "domain"]
        if missing := [key for key in required if key not in params]:
            raise HTTPException(400, f"Missing params: {missing}")

        chat_id = int(params["state"])

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://{params['domain']}/oauth/token/",
                data={
                    "grant_type": "authorization_code",
                    "code": params["code"],
                    "client_id": BITRIX_CLIENT_ID,
                    "client_secret": BITRIX_CLIENT_SECRET,
                    "redirect_uri": REDIRECT_URI
                }
            )
            token_data = resp.json()

        # Псоле авторизации регистрируем обработчики
        try:
            await register_webhooks(
                domain=params['domain'],
                access_token=token_data['access_token']
            )
        except Exception as e:
            logging.error(f"Webhook registration failed: {str(e)}")

        member_id = params.get("member_id")
        if member_id:
            member_map[member_id] = str(chat_id)
        user_info = await get_user_info(params['domain'], token_data['access_token'])

        # Сохранение данных пользователя
        tokens[str(chat_id)] = {
            "access_token": token_data["access_token"],
            "refresh_token": token_data["refresh_token"],
            "expires": int(time()) + int(token_data["expires_in"]),
            "domain": params["domain"],
            "member_id": params.get("member_id", ""),
            "user_id": user_info["id"],
            "is_admin": user_info["is_admin"]
        }

        await bot.send_message(chat_id, "✅ Авторизация успешна!")
        return HTMLResponse("""
            <html><head><meta charset='utf-8'><title>Авторизация</title>
            <style>
            body { display:flex; justify-content:center; align-items:center; height:100vh; background:#f0f0f0; font-family:Arial,sans-serif; }
            .card { background:white; padding:2em; border-radius:8px; box-shadow:0 2px  6px rgba(0,0,0,0.2); text-align:center; }
            h1 { color:#4caf50; }
            </style>
            </head><body><div class='card'>
            <h1>✅ Авторизация успешна!</h1>
            <p>Закройте это окно и вернитесь в Telegram.</p>
            </div></body></html>
            """)

    except Exception as e:
        logging.error(f"OAuth error: {str(e)}")
        raise HTTPException(500, "Internal error")


async def handle_webhook_event(request: Request):
    """Обработка событий"""
    try:
        # Получение данных
        form_data = await request.form()
        parsed_data = parse_form_data(dict(form_data))
        # logging.info(f"Parsed webhook data: {json.dumps(parsed_data, indent=2)}")

        auth_data = parsed_data.get('auth', {})
        event = parsed_data.get('event', '').lower()
        member_id = auth_data.get('member_id')

        # Обработка ошибок при некорректных данных
        if not member_id:
            return JSONResponse({"status": "invalid_member_id"}, status_code=400)

        chat_id = member_map.get(member_id)
        if not chat_id:
            logging.error(f"Member ID {member_id} not mapped to any chat")
            return JSONResponse({"status": "member_not_found"}, status_code=404)

        user_data = tokens.get(chat_id)
        if not user_data:
            logging.error(f"User data not found for chat {chat_id}")
            return JSONResponse({"status": "unauthorized"}, status_code=401)

        # Обновление токена, если нужно
        if time() > user_data["expires"] and not await refresh_token(chat_id):
            return JSONResponse({"status": "token_expired"}, status_code=401)

        # Срабатывание событий
        if event.startswith("ontask"):  # задачи
            await process_task_event(event, parsed_data, user_data, chat_id)
        elif event.startswith("oncrmdeal"):  # сделки
            await process_deal_event(event, parsed_data, user_data, chat_id)

        return JSONResponse({"status": "ok"})

    except Exception as e:
        logging.error(f"Webhook handler error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


async def process_task_event(event: str, data: dict, user_data: dict, chat_id: str):
    """Получение уведомлений о задачах из Битрикса"""
    try:
        task_id = data.get('data', {}).get('FIELDS_AFTER', {}).get('ID')
        if not task_id:
            logging.error("No task ID in webhook data")
            return

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://{user_data['domain']}/rest/tasks.task.get",
                params={
                    "taskId": task_id,
                    "auth": user_data["access_token"]
                }
            )
            resp.raise_for_status()
            task_data = resp.json()

            if 'error' in task_data:
                logging.error(f"Bitrix API error: {task_data['error_description']}")
                return

            task = task_data.get('result', {}).get('task', {})
            logging.info(f"Task data: {task}")

            message = ""
            if event == "ontaskadd":
                message = (
                    f"🆕 Новая задача: {task.get('title', 'Без названия')}\n"
                    f"Описание: {task.get('description', 'Отсутствует')}\n"
                    f"Приоритет: {task.get('priority')}\n"
                    f"Дедлайн: {task.get('deadline')}\n"
                    f"ID: {task_id}"
                )
            elif event == "ontaskupdate":
                message = (
                    f"🆕 Обновлена задача: {task.get('title', 'Без названия')}\n"
                    f"Описание: {task.get('description', 'Отсутствует')}\n"
                    f"Приоритет: {task.get('priority')}\n"
                    f"Дедлайн: {task.get('deadline')}\n"
                    f"ID: {task_id}"
                )

            if message != "":
                await bot.send_message(chat_id, message)
    except httpx.HTTPStatusError as e:
        logging.error(f"API request failed: {e.response.text}")
    except Exception as e:
        logging.error(f"Task processing error: {e}")


async def process_deal_event(event: str, data: dict, user_data: dict, chat_id: str):
    """Получение уведомлений о сделках из Битрикса"""
    try:
        deal_id = data.get('data', {}).get('FIELDS', {}).get('ID')
        if not deal_id:
            return

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://{user_data['domain']}/rest/crm.deal.get",
                params={"id": deal_id, "auth": user_data["access_token"]}
            )
            deal = resp.json().get("result", {})
            # logging.info(f"Deal data: {deal}")

            message = ""
            if event == "oncrmdealadd":
                message = (
                    f"🆕 Новая сделка: {deal.get('title', 'Без названия')}\n"
                    f"Стадия: {deal.get('stage_name', 'Неизвестна')}")
            elif event == "oncrmdealupdate":
                message = (
                    f"🔄 Обновлена сделка: {deal.get('title', 'Без названия')}\n"
                    f"Новая стадия: {deal.get('stage_name', 'Неизвестна')}")

            if message != "":
                await bot.send_message(chat_id, message)
    except Exception as e:
        logging.error(f"Deal processing error: {e}")


# --- Telegram Bot ---
@dp.message(Command("start"))
async def cmd_start(m: Message):
    """Запуск бота, формирование ссылки для авторизации"""
    auth_url = (
        f"https://{BITRIX_DOMAIN}/oauth/authorize/"
        f"?client_id={BITRIX_CLIENT_ID}"
        f"&response_type=code"
        f"&state={m.from_user.id}"
        f"&redirect_uri={REDIRECT_URI}"
    )
    await m.answer(f"🔑 [Авторизация]({auth_url})", parse_mode="Markdown")


@dp.message(Command("task"))
async def cmd_task(m: Message):
    """Создание задачи"""
    user_data = tokens.get(str(m.from_user.id))
    if not user_data:
        return await m.answer("❗ Сначала авторизуйтесь: /start")

    try:
        parts = m.text.split(maxsplit=1)[1].split('|')
        parts = [p.strip() for p in parts]

        title = parts[0]
        description = parts[1] if len(parts) > 1 else ""
        responsible_id = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else user_data["user_id"]
        priority = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 2
        deadline = parts[4] if len(parts) > 4 else None

        if priority not in (1, 2, 3):
            raise ValueError("Приоритет должен быть 1, 2 или 3")

        if not await check_user_exists(user_data["domain"], user_data["access_token"], responsible_id):
            raise ValueError("Пользователь не найден")

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://{user_data['domain']}/rest/tasks.task.add.json",
                params={"auth": user_data["access_token"]},
                json={
                    "fields": {
                        "TITLE": title,
                        "DESCRIPTION": description,
                        "PRIORITY": priority,
                        "RESPONSIBLE_ID": responsible_id,
                        "DEADLINE": deadline
                    }
                }
            )

            # Расширенная обработка ответа
            try:
                data = resp.json()
            except json.JSONDecodeError as e:
                error_text = resp.text[:200]  # Первые 200 символов ответа
                logging.error(f"JSON decode error. Response: {error_text}")
                raise ValueError("Некорректный ответ от сервера Bitrix")

            # Проверка типа данных
            if not isinstance(data, dict):
                logging.error(f"Unexpected response type: {type(data)}. Content: {data}")
                raise ValueError("Ошибка формата ответа")

            # Обработка ошибок API
            if data.get('error'):
                error_msg = data.get('error_description', 'Неизвестная ошибка Bitrix')
                logging.error(f"Bitrix API Error: {error_msg}")
                raise ValueError(error_msg)

            # Получение ID задачи с проверкой структуры
            try:
                task_id = data['result']['task']['id']
            except KeyError:
                logging.error(f"Invalid response structure: {data}")
                raise ValueError("Некорректная структура ответа")

            await m.answer(f"✅ Задача создана! ID: {task_id}")

    except (IndexError, ValueError) as e:
        await m.answer(
            f"❌ Ошибка: {str(e)}\nФормат: /task Название | Описание | [ID_исполнителя] | [Приоритет] | [Дедлайн]")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}", exc_info=True)
        await m.answer(f"⚠️ Системная ошибка: {str(e)}")


@dp.message(Command("help"))
async def cmd_help(m: Message):
    """Справка о командах бота"""
    help_text = """
📚 Доступные команды:
/start - Авторизация в Bitrix24
/task - Создать задачу (Формат: Название | Описание | [ID_исполнителя] | [Приоритет] | [Дедлайн])
/deal - Создать сделку (только для админов)
/comment - Добавить комментарий к задаче
/help - Эта справка
"""
    await m.answer(help_text)


# --- Main ---
async def main():
    import uvicorn
    config = uvicorn.Config(app=app, host="0.0.0.0", port=5000, log_level="info")
    server = uvicorn.Server(config)
    await asyncio.gather(server.serve(), dp.start_polling(bot))


if __name__ == "__main__":
    asyncio.run(main())