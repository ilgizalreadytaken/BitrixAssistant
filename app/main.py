import asyncio
import json
import logging
from urllib.parse import parse_qs
from time import time
from typing import Dict, Optional
import datetime


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
BITRIX_CLIENT_ID = "local.68187191a08683.25172914"
BITRIX_CLIENT_SECRET = "46wPWoUU1YLv5d86ozDh7FbhODOi2L2mlmNBWweaA6jNxV2xX1"
REDIRECT_URI = "https://mybitrixbot.ru/callback"
WEBHOOK_DOMAIN = "https://mybitrixbot.ru"
TELEGRAM_TOKEN = "8179379861:AAEoKsITnDaREJINuHJu4qXONwxTIlSncxc"
BITRIX_DOMAIN = "b24-rqyyhh.bitrix24.ru"

# Хранилища данных
tokens: Dict[str, Dict] = {}  # Хранение данных пользователя для протокола OAuth
member_map: Dict[str, str] = {}  # Связка пользователей (Формат: {member_id (Битрикс24): chat_id (Telegram)})
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
            f"https://{domain}/rest/profile.json",
            params={"auth": access_token}
        )
        data = resp.json()
        data = data.get("result", {})

        # logging.info(f"Data User Get: {data}") # Логи

        return {
            "id": data.get("ID"),
            "is_admin": data.get("ADMIN"),
            "name": f"{data.get("NAME")} {data.get("LAST_NAME")}".strip(),
        }


async def get_user_name(domain: str, access_token: str, user_id: int) -> str:
    """Получение имени пользователя по ID"""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://{domain}/rest/user.get.json",
                params={
                    "auth": access_token,
                    "ID": user_id
                }
            )
            user_data = resp.json().get('result', [{}])[0]
            return f"{user_data.get('NAME', '')} {user_data.get('LAST_NAME', '')}".strip() or "Неизвестный"
    except Exception as e:
        logging.error(f"Error getting user name: {e}")
        return "Неизвестный"


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

    # все события, которые нужно обрабатывать
    events = [
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
                # logging.info(f"Webhook {event} response: {resp.status_code} {resp.text}") # Логи
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
    # logging.info(f"OAuth callback params: {params}") # Логи

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

        logging.info(f"User_info: {user_info}")  # Логи

        # Сохранение данных пользователя
        tokens[str(chat_id)] = {
            "access_token": token_data["access_token"],
            "refresh_token": token_data["refresh_token"],
            "expires": int(time()) + int(token_data["expires_in"]),
            "domain": params["domain"],
            "member_id": params.get("member_id", ""),
            "user_id": user_info["id"],
            "user_name": user_info["name"],
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

        # logging.info(f"Parsed webhook data: {json.dumps(parsed_data, indent=2)}") # Логи

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
        if event.startswith("ontaskcomment"):  # комментарии к задачам
            await process_comment_event(event, parsed_data, user_data, chat_id)
        elif event.startswith("ontask"):  # задачи
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
        task_id = None
        logging.info(f"data: {data}")
        if event != "ontaskdelete":
            task_id = data.get('data', {}).get('FIELDS_AFTER', {}).get('ID')
            if not task_id and event:
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

                # logging.info(f"Task data: {task}") # Логи

        message = ""
        responsible_id = None

        status_map = {
            '1': "🆕 Новая",
            '2': "🔄 В работе",
            '3': "⏳ Ожидает контроля",
            '4': "✅ Завершена",
            '5': "⏸ Отложена",
            '6': "❌ Отклонена"
        }

        title = task.get('title', 'Без названия')
        description = task.get('description', 'Отсутствует')
        priority = task.get('priority')
        status_code = task.get('status')
        status = status_map.get(status_code, f"Неизвестный статус ({status_code})")
        responsible_id = task.get('responsibleId')
        creator_name = task.get('creator').get('name')
        responsible_name = task.get('responsible').get('name')
        deadline = task.get('deadline')

        if event == "ontaskadd":
            message = (
                f"Задача [ID: {task_id}] - 🆕Создана🆕\n"
                f"📌Название: {title}\n"
                f"📝Описание: {description}\n"
                f"🚨Приоритет: {priority}\n"
                f"📊Cтатус: {status}\n"
                f"⏰Срок исполнения: {deadline}\n"
                f"👤Постановщик: {creator_name}\n"
                f"👤Исполнитель: {responsible_name}"
            )
        elif event == "ontaskupdate":
            changed_by_id = task.get('changedBy')

            changed_by_name = await get_user_name(
                domain=user_data['domain'],
                access_token=user_data["access_token"],
                user_id=changed_by_id
            )

            message = (
                f"Задача [ID: {task_id}] - 🔄Изменена🔄\n"
                f"📌Название: {title}\n"
                f"📝Описание: {description}\n"
                f"🚨Приоритет: {priority}\n"
                f"📊Cтатус: {status}\n"
                f"⏰Срок исполнения: {deadline}\n"
                f"👤Постановщик: {creator_name}\n"
                f"👤Исполнитель: {responsible_name}\n"
                f"👤Кто изменил: {changed_by_name}"
            )
        if responsible_id:
            if str(user_data.get('user_id')) == str(responsible_id) or user_data.get('is_admin'):
                await bot.send_message(chat_id, message)
    except httpx.HTTPStatusError as e:
        logging.error(f"API request failed: {e.response.text}")
    except Exception as e:
        logging.error(f"Task processing error: {e}")


# Пока в процессе доработки
async def process_deal_event(event: str, data: dict, user_data: dict, chat_id: str):
    """Получение уведомлений о сделках из Битрикса"""
    try:
        responsible_id = None
        message = ""
        deal = {}

        # Обработка разных типов событий
        if event != "oncrmdealdelete":
            deal_id = data.get('data', {}).get('FIELDS', {}).get('ID')
            if not deal_id:
                return

            name = ""

            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://{user_data['domain']}/rest/crm.deal.get",
                    params={
                        "id": deal_id,
                        "auth": user_data["access_token"]
                    }
                )
                deal = resp.json().get("result", {})
                responsible_id = deal.get('ASSIGNED_BY_ID')

                if responsible_id:
                    name = await get_user_name(
                        domain=user_data['domain'],
                        access_token=user_data["access_token"],
                        user_id=responsible_id
                    )

            # logging.info(f"deal data: {deal}") # Логи

            if event == "oncrmdealadd":
                message = (
                    f"🆕 Новая сделка\n"
                    f"ID: {deal_id}\n"
                    f"Название ЖК: {deal.get('TITLE')}\n"
                    f"Адрес: {deal.get('COMMENTS', 'Не указано')}\n"
                    f"Стадия: {deal.get('STAGE_ID')}\n"
                    f"Ответственный: {name}"
                )
            elif event == "oncrmdealupdate":
                message = (
                    f"🔄 Изменена сделка\n"
                    f"ID: {deal_id}\n"
                    f"Название ЖК: {deal.get('TITLE')}\n"
                    f"Адрес: {deal.get('COMMENTS', 'Не указано')}\n"
                    f"Стадия: {deal.get('STAGE_ID')}\n"
                    f"Ответственный: {name}"
                )

        if responsible_id:
            if str(user_data.get('user_id')) == str(responsible_id) or user_data.get('is_admin'):
                await bot.send_message(chat_id, message)

    except Exception as e:
        logging.error(f"Ошибка обработки сделки: {e}")


async def process_comment_event(event: str, data: dict, user_data: dict, chat_id: str):
    """Обработка комментариев к задачам из Битрикса"""
    try:
        comment_data = data.get('data', {}).get('FIELDS_AFTER')
        # logging.info(f"Comment data: {comment_data}") # Логи

        comment_id = comment_data.get('ID')
        task_id = comment_data.get('TASK_ID')
        message = ""
        responsible_id = None

        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://{user_data['domain']}/rest/task.commentitem.get",
                params={
                    "taskId": task_id,
                    "itemId": comment_id,
                    "auth": user_data["access_token"]
                }
            )
            comment = resp.json().get('result', {})
            logging.info(f"Comment data: {comment}")

            author_name = comment.get('AUTHOR_NAME')
            comment_text = comment.get('POST_MESSAGE', '')[:1000]  # Обрезаем длинные сообщения
            comment_date = comment.get('POST_DATE', '')
            message = (
                f"💬 Новый комментарий к задаче [ID: {task_id}]\n"
                f"Автор: {author_name}\n"
                f"Текст: {comment_text}\n"
                f"Дата: {comment_date}\n"
            )

        # Добавляем ответственного
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://{user_data['domain']}/rest/tasks.task.get",
                params={
                    "taskId": task_id,
                    "auth": user_data["access_token"]
                }
            )
            task = resp.json().get('result', {}).get('task', {})
            responsible_id = task.get('responsibleId')

        if responsible_id:
            if str(user_data.get('user_id')) == str(responsible_id) or user_data.get('is_admin'):
                await bot.send_message(chat_id, message)

    except Exception as e:
        logging.error(f"Ошибка обработки комментария: {e}")


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

    message_to_user = (f"Добро пожаловать!\n🔑 Для начала работы BitrixAssistant пройдите авторизацию: {auth_url}")
    await m.answer(message_to_user)


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
        priority = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 1
        deadline = parts[4] if len(parts) > 4 else None

        if priority not in (0, 1, 2):
            raise ValueError("Приоритет должен быть 0, 1 или 2")

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
            f"❌ Ошибка: {str(e)}\nФормат: /task Название | Описание | [ID_исполнителя] | [Приоритет] | [Срок исполнения]")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}", exc_info=True)
        await m.answer(f"⚠️ Системная ошибка: {str(e)}")


# В процессе доработки
@dp.message(Command("deal"))
async def cmd_deal(m: Message):
    """Создание сделки: /deal Название ЖК | Адрес | Стадия_ID"""
    user_data = tokens.get(str(m.from_user.id))
    if not user_data or not user_data.get("is_admin"):
        return await m.answer("❗ Требуются права администратора. Авторизуйтесь через /start")

    try:
        parts = m.text.split(maxsplit=1)[1].split('|')
        parts = [p.strip() for p in parts]

        if len(parts) < 3:
            raise ValueError("Недостаточно параметров. Формат: /deal Название ЖК | Адрес | ID_стадии")

        title, address, stage_id = parts[0], parts[1], parts[2]

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://{user_data['domain']}/rest/crm.deal.add.json",
                params={"auth": user_data["access_token"]},
                json={
                    "fields": {
                        "TITLE": title,
                        "COMMENTS": address,
                        "STAGE_ID": stage_id,
                        "ASSIGNED_BY_ID": user_data["user_id"]
                    }
                }
            )
            data = resp.json()

            if data.get('error'):
                error_msg = data.get('error_description', 'Неизвестная ошибка Bitrix')
                raise ValueError(f"Bitrix API: {error_msg}")

            deal_id = data.get('result')
            await m.answer(f"✅ Сделка создана! ID: {deal_id}")

    except (IndexError, ValueError) as e:
        await m.answer(f"❌ Ошибка: {str(e)}\nФормат: /deal Название ЖК | Адрес | ID_стадии")
    except Exception as e:
        logging.error(f"Ошибка создания сделки: {str(e)}", exc_info=True)
        await m.answer(f"⚠️ Ошибка: {str(e)}")


@dp.message(Command("comment"))
async def cmd_comment(m: Message):
    """Добавить комментарий к задаче: /comment [ID задачи] | Комментарий"""
    user_data = tokens.get(str(m.from_user.id))
    if not user_data:
        return await m.answer("❗ Сначала авторизуйтесь через /start")

    try:
        # Парсим аргументы
        parts = m.text.split(maxsplit=1)[1].split('|', 1)
        if len(parts) < 2:
            raise ValueError("Неверный формат команды")

        task_id = parts[0].strip()
        comment_text = parts[1].strip()

        # Проверяем доступ к задаче
        async with httpx.AsyncClient() as client:
            # Проверка существования задачи
            task_resp = await client.get(
                f"https://{user_data['domain']}/rest/tasks.task.get.json",
                params={
                    "taskId": task_id,
                    "auth": user_data["access_token"]
                }
            )
            task_data = task_resp.json()
            if 'error' in task_data:
                raise ValueError("Задача не найдена или нет доступа")

            # Отправка комментария
            comment_resp = await client.post(
                f"https://{user_data['domain']}/rest/task.commentitem.add.json",
                params={"auth": user_data["access_token"]},
                json={
                    "TASK_ID": task_id,
                    "fields": {
                        "AUTHOR_ID": user_data["user_id"],
                        "POST_MESSAGE": comment_text
                    }
                }
            )
            comment_data = comment_resp.json()

            if 'error' in comment_data:
                error_msg = comment_data.get('error_description', 'Ошибка добавления комментария')
                raise ValueError(error_msg)

            await m.answer(f"💬 Комментарий добавлен к задаче {task_id}")

    except (IndexError, ValueError) as e:
        await m.answer(f"❌ Ошибка: {str(e)}\nФормат: /comment [ID задачи] | [Текст комментария]")
    except Exception as e:
        logging.error(f"Comment error: {str(e)}", exc_info=True)
        await m.answer(f"⚠️ Ошибка: {str(e)}")


@dp.message(Command("stages"))
async def cmd_stages(m: Message):
    """Получить список стадий сделок"""
    user_data = tokens.get(str(m.from_user.id))
    if not user_data:
        return await m.answer("❗ Сначала авторизуйтесь: /start")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://{user_data['domain']}/rest/crm.dealcategory.stage.list",
                params={"auth": user_data["access_token"]}
            )
            stages = resp.json().get('result', [])

            message = "Доступные стадии:\n"
            for stage in stages:
                message += f"{stage['NAME']} (ID: {stage['STATUS_ID']})\n"

            await m.answer(message)

    except Exception as e:
        await m.answer(f"⚠️ Ошибка: {str(e)}")


@dp.message(Command("employees"))
async def cmd_employees(m: Message):
    """Получить список сотрудников Bitrix24"""
    user_data = tokens.get(str(m.from_user.id))
    if not user_data:
        return await m.answer("❗ Сначала авторизуйтесь через /start")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://{user_data['domain']}/rest/user.get.json",
                params={
                    "auth": user_data["access_token"],
                    "FILTER": {"USER_TYPE": "employee"},
                    "SELECT": ["ID", "NAME", "LAST_NAME"]
                }
            )
            data = resp.json()

            if 'error' in data:
                error_msg = data.get('error_description', 'Неизвестная ошибка')
                return await m.answer(f"❌ Ошибка Bitrix: {error_msg}")

            users = data.get('result', [])
            if not users:
                return await m.answer("🤷 На портале нет сотрудников")

            # Формируем список
            user_list = []
            for user in users:
                user_id = user.get('ID', 'N/A')
                name = f"{user.get('NAME', '')} {user.get('LAST_NAME', '')}".strip()
                user_list.append(f"👤 {name} (ID: {user_id})")

            # Разбиваем на сообщения по 20 пользователей
            chunk_size = 20
            for i in range(0, len(user_list), chunk_size):
                chunk = user_list[i:i + chunk_size]
                await m.answer(
                    "Список сотрудников:\n\n" + "\n".join(chunk),
                    parse_mode="HTML"
                )

    except Exception as e:
        logging.error(f"Employees error: {str(e)}", exc_info=True)
        await m.answer(f"⚠️ Ошибка при получении списка: {str(e)}")


@dp.message(Command("tasks"))
async def cmd_tasks(m: Message):
    """Показать список задач пользователя"""
    user_data = tokens.get(str(m.chat.id))
    if not user_data:
        return await m.answer("❗ Сначала авторизуйтесь через /start")

    try:
        user_id = user_data['user_id']

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://{user_data['domain']}/rest/tasks.task.list.json",
                params={"auth": user_data["access_token"]},
                json={
                    "order": {"CREATED_DATE": "DESC"},
                    "select": ["ID", "TITLE", "RESPONSIBLE_ID", "CREATED_BY", "STATUS", "DEADLINE"]
                }
            )
            resp.raise_for_status()
            data = resp.json()

            if 'error' in data:
                error_msg = data.get('error_description', 'Неизвестная ошибка')
                raise ValueError(f"Bitrix API: {error_msg}")

            tasks = data.get('result', {}).get('tasks', [])
            if not tasks:
                await m.answer("📭 У вас нет задач.")
                return

            status_map = {
                '1': "🆕 Новая",
                '2': "🔄 В работе",
                '3': "⏳ Ожидает контроля",
                '4': "✅ Завершена",
                '5': "⏸ Отложена",
                '6': "❌ Отклонена"
            }

            message = ["📋 Список задач:"]
            for task in tasks:
                task_id = task.get('id')
                title = task.get('title', 'Без названия')
                status_code = task.get('status')
                status = status_map.get(status_code, f"Неизвестный статус ({status_code})")
                responsible_id = task.get('responsibleId')
                creator_name = task.get('creator').get('name')
                responsible_name = task.get('responsible').get('name')
                deadline = task.get('deadline')

                deadline_str = "Не указан"
                if deadline:
                    try:
                        deadline_date = datetime.strptime(deadline, "%Y-%m-%d %H:%M:%S")
                        deadline_str = deadline_date.strftime("%d.%m.%Y %H:%M")
                    except Exception as e:
                        deadline_str = deadline

                task_info = (
                    f"\n🆔 ID: {task_id}",
                    f"📌 Название: {title}",
                    f"📊 Статус: {status}",
                    f"👤 Исполнитель: {responsible_name}",
                    f"👤 Постановщик: {creator_name}",
                    f"⏰ Срок: {deadline_str}",
                    "―――――――――――――――――――――"
                )
                message.extend(task_info)
            message.append(f"\nПоказано {len(tasks)} задач.")

            await m.answer("\n".join(message))

    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error: {e.response.text}")
        await m.answer("❌ Ошибка подключения к Bitrix24.")
    except ValueError as e:
        await m.answer(f"❌ {str(e)}")
    except Exception as e:
        logging.error(f"Ошибка в /tasks: {str(e)}", exc_info=True)
        await m.answer("⚠️ Ошибка при получении задач.")


@dp.message(Command("help"))
async def cmd_help(m: Message):
    """Справка о командах бота"""
    help_text = ("""
📚 Доступные команды:
/start - Авторизация в Bitrix24
/tasks - Вывести список задач
/task - Создать задачу (Формат: Название | Описание | [ID_исполнителя] | [Приоритет] | [Срок исполнения])
/comment - Добавить комментарий к задаче (Формат: [ID_задачи] | Комментарий)
/deal - Создать сделку (Формат: Название ЖК | Адрес | [ID_стадии]) ❗Только для админов❗
/employees - Получить список сотрудников
/stages - Получить список доступных стадий для сделок

/help - Справка о командах
    """)

    await m.answer(help_text)


# --- Main ---
async def main():
    import uvicorn
    config = uvicorn.Config(app=app, host="0.0.0.0", port=5000, log_level="info")
    server = uvicorn.Server(config)
    await asyncio.gather(server.serve(), dp.start_polling(bot))


if __name__ == "__main__":
    asyncio.run(main())