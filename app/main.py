import asyncio
import json
from urllib.parse import parse_qs
from time import time
import httpx

from typing import Dict
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

BITRIX_CLIENT_ID = "local.68122d64ea29a1.85490975"
BITRIX_CLIENT_SECRET = "sFQq1zjJ2V4EAjAnP842GwOKKJT5Tb0WJ25btXtC3IR2VVg72d"
REDIRECT_URI = "https://mybitrixbot.ru/callback"
WEBHOOK_DOMAIN = "https://mybitrixbot.ru"
TELEGRAM_TOKEN = "8179379861:AAEoKsITnDaREJINuHJu4qXONwxTIlSncxc"

tokens: Dict[str, Dict[str, str]] = {}
member_map: Dict[str, str] = {}

app = FastAPI()
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

async def refresh_token(chat_id: str) -> bool:
    user_data = tokens.get(chat_id)
    if not user_data:
        return False

    async with httpx.AsyncClient() as client:
        try:
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

            tokens[chat_id].update({
                "access_token": data["access_token"],
                "refresh_token": data["refresh_token"],
                "expires": str(int(time()) + int(data["expires_in"]))
            })
            return True
        except Exception as e:
            print(f"Token refresh failed: {str(e)}")
            return False


def flatten_parsed_data(parsed_data: dict) -> dict:
    return {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in parsed_data.items()}


# --- Обработчики маршрутов ---
@app.get("/callback")
async def oauth_callback(request: Request):
    params = request.query_params
    if not all(key in params for key in ["code", "state", "domain", "member_id"]):
        raise HTTPException(400, "Missing required parameters")

    async with httpx.AsyncClient() as client:
        try:
            # Обмен кода на токены
            token_resp = await client.post(
                "https://oauth.bitrix.info/oauth/token/",
                data={
                    "grant_type": "authorization_code",
                    "code": params["code"],
                    "client_id": BITRIX_CLIENT_ID,
                    "client_secret": BITRIX_CLIENT_SECRET,
                    "redirect_uri": REDIRECT_URI
                }
            )
            token_resp.raise_for_status()
            token_data = token_resp.json()

            # Получение данных пользователя
            user_resp = await client.get(
                f"https://{params['domain']}/rest/user.current.json",
                params={"auth": token_data["access_token"]}
            )
            user_resp.raise_for_status()
            user_data = user_resp.json().get("result", {})
            bitrix_user_id = user_data.get("ID")

            # Сохранение данных
            tokens[params["state"]] = {
                "access_token": token_data["access_token"],
                "refresh_token": token_data["refresh_token"],
                "domain": params["domain"],
                "bitrix_user_id": str(bitrix_user_id),
                "expires": str(int(time()) + int(token_data["expires_in"]))
            }
            member_map[params["member_id"]] = params["state"]

            # Привязка вебхука
            bind_resp = await client.post(
                f"https://{params['domain']}/rest/event.bind.json",
                json={
                    "event": "ONTASKADD",
                    "handler": f"{WEBHOOK_DOMAIN}/handler",
                    "auth": token_data["access_token"]
                }
            )
            bind_data = bind_resp.json()

            return JSONResponse({"status": "ok", "bound": bind_data})

        except Exception as e:
            raise HTTPException(500, f"OAuth error: {str(e)}")


@app.api_route("/handler", methods=["GET", "POST", "HEAD"])
async def bitrix_handler(request: Request):
    if request.method in ["GET", "HEAD"]:
        return JSONResponse({"status": "ok"})

    try:
        raw_body = await request.body()
        parsed_data = parse_qs(raw_body.decode())
        flat_data = flatten_parsed_data(parsed_data)

        # Логирование
        print("Received Bitrix data:", flat_data)
        await bot.send_message(
            chat_id=858016468,
            text=f"📩 Bitrix webhook:\n<pre>{json.dumps(flat_data, indent=2)}</pre>",
            parse_mode=ParseMode.HTML
        )

        # Извлечение параметров
        event = flat_data.get("event")
        member_id = flat_data.get("auth[member_id]")
        domain = flat_data.get("auth[domain]")
        task_id = flat_data.get("data[FIELDS_AFTER][ID]")
        expires = flat_data.get("auth[expires]", "0")

        if not all([event, member_id, domain]):
            return JSONResponse({"status": "error", "message": "Missing required fields"}, 400)

        chat_id = member_map.get(member_id)
        if not chat_id:
            return JSONResponse({"status": "error", "message": "Member not registered"}, 404)

        user_data = tokens.get(chat_id)
        if not user_data:
            return JSONResponse({"status": "error", "message": "User not authenticated"}, 401)

        # Обновление токена при необходимости
        if int(expires) < time():
            if not await refresh_token(chat_id):
                await bot.send_message(chat_id, "❌ Требуется повторная авторизация! /start")
                return JSONResponse({"status": "error", "message": "Token expired"}, 401)

        if event == "ONTASKADD" and task_id:
            task_url = f"https://{domain}/company/personal/user/{user_data['bitrix_user_id']}/tasks/task/view/{task_id}/"
            await bot.send_message(
                chat_id=chat_id,
                text=f"🆕 Новая задача в Bitrix24\n🔗 {task_url}\n📌 ID: #{task_id}"
            )

        return JSONResponse({"status": "ok"})

    except Exception as e:
        print(f"Handler error: {str(e)}")
        return JSONResponse({"status": "error", "message": str(e)}, 500)


# --- Telegram Bot ---
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()


# --- Команды бота ---
@dp.message(Command("start"))
async def cmd_start(m: Message):
    state = str(m.from_user.id)
    auth_url = (
        f"https://oauth.bitrix.info/oauth/authorize/"
        f"?client_id={BITRIX_CLIENT_ID}"
        f"&response_type=code"
        f"&state={state}"
        f"&redirect_uri={REDIRECT_URI}"
    )
    await m.answer(f"🔑 Для подключения Bitrix24 перейдите по ссылке:\n{auth_url}")


@dp.message(Command("task"))
async def cmd_task(m: Message):
    state = str(m.from_user.id)
    user_data = tokens.get(state)

    if not user_data:
        await m.answer("❗ Сначала выполните авторизацию через /start")
        return

    if int(time()) > int(user_data.get("expires", 0)):
        if not await refresh_token(state):
            await m.answer("❌ Требуется повторная авторизация! /start")
            return

    parts = (m.text or "").strip().split(maxsplit=1)
    title = parts[1] if len(parts) > 1 else "Задача из бота"

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(
                f"https://{user_data['domain']}/rest/tasks.task.add",
                json={
                    "fields": {
                        "TITLE": title,
                        "CREATED_BY": user_data["bitrix_user_id"],
                        "RESPONSIBLE_ID": user_data["bitrix_user_id"]
                    },
                    "auth": user_data["access_token"]
                }
            )
            resp.raise_for_status()
            result = resp.json()

            if task_info := result.get("result", {}).get("task"):
                await m.answer(f"✅ Задача создана!\nID: {task_info['id']}")
            else:
                await m.answer(f"❌ Ошибка: {result.get('error', 'Unknown error')}")

        except Exception as e:
            await m.answer(f"🚫 Ошибка при создании задачи: {str(e)}")


# --- Запуск приложения ---
async def main():
    import uvicorn
    server = uvicorn.Server(
        uvicorn.Config(
            app=app,
            host="0.0.0.0",
            port=5000,
            log_level="info"
        )
    )

    await asyncio.gather(
        server.serve(),
        dp.start_polling(bot)
    )


if __name__ == "__main__":
    asyncio.run(main())