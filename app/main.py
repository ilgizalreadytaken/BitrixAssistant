import asyncio
import json
from urllib.parse import parse_qs
from time import time
import httpx

from typing import Dict
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

# Настройки приложения
BITRIX_CLIENT_ID = "local.68122d64ea29a1.85490975"
BITRIX_CLIENT_SECRET = "sFQq1zjJ2V4EAjAnP842GwOKKJT5Tb0WJ25btXtC3IR2VVg72d"
REDIRECT_URI = "https://mybitrixbot.ru/callback"
WEBHOOK_DOMAIN = "https://mybitrixbot.ru"
TELEGRAM_TOKEN = "8179379861:AAEoKsITnDaREJINuHJu4qXONwxTIlSncxc"

# Хранилища токенов и соответствий
tokens: Dict[str, Dict[str, str]] = {}
member_map: Dict[str, str] = {}

# Инициализация FastAPI и Telegram-бота
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
            user_data.update({
                "access_token": data["access_token"],
                "refresh_token": data["refresh_token"],
                "expires": str(int(time()) + int(data["expires_in"]))
            })
            return True
        except Exception as e:
            print(f"Token refresh failed: {e}")
            return False


def flatten_parsed_data(parsed_data: dict) -> dict:
    return {k: (v[0] if isinstance(v, list) and len(v) == 1 else v)
            for k, v in parsed_data.items()}

# --- OAuth Callback ---
@app.get("/callback")
async def oauth_callback(request: Request):
    params = request.query_params
    required = ["code", "state", "domain", "member_id"]
    if not all(key in params for key in required):
        raise HTTPException(400, "Missing required parameters")

    state = params["state"]
    async with httpx.AsyncClient() as client:
        try:
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

            user_resp = await client.get(
                f"https://{params['domain']}/rest/user.current.json",
                params={"auth": token_data["access_token"]}
            )
            user_resp.raise_for_status()
            bitrix_user_id = str(user_resp.json().get("result", {}).get("ID"))

            tokens[state] = {
                "access_token": token_data["access_token"],
                "refresh_token": token_data["refresh_token"],
                "domain": params["domain"],
                "bitrix_user_id": bitrix_user_id,
                "expires": str(int(time()) + int(token_data["expires_in"]))
            }
            member_map[params["member_id"]] = state

            # Регистрация вебхука (не критичная при ошибке)
            try:
                bind_resp = await client.post(
                    f"https://{params['domain']}/rest/event.bind.json",
                    json={
                        "event": "ONTASKADD",
                        "handler": f"{WEBHOOK_DOMAIN}/handler",
                        "auth": token_data["access_token"]
                    }
                )
                if bind_resp.status_code != 200:
                    print(f"Warning: event.bind failed {bind_resp.status_code}")
            except Exception as e:
                print(f"Warning: event.bind exception: {e}")

            await bot.send_message(
                chat_id=int(state),
                text="✅ Авторизация прошла успешно! Теперь вы получите уведомления о задачах."
            )

            html = """
<html><head><meta charset='utf-8'><title>Авторизация</title>
<style>
  body { display:flex; justify-content:center; align-items:center; height:100vh; background:#f0f0f0; font-family:Arial,sans-serif; }
  .card { background:white; padding:2em; border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.2); text-align:center; }
  h1 { color:#4caf50; }
</style>
</head><body><div class='card'>
  <h1>✅ Авторизация успешна!</h1>
  <p>Закройте это окно и вернитесь в Telegram.</p>
</div></body></html>
"""
            return HTMLResponse(content=html)

        except Exception as e:
            print(f"OAuth error: {e}")
            raise HTTPException(500, f"OAuth error: {e}")

# --- Обработчик вебхуков ---
@app.api_route("/handler", methods=["GET","POST","HEAD"])
async def bitrix_handler(request: Request):
    if request.method in ["GET","HEAD"]:
        return JSONResponse({"status": "ok"})
    try:
        flat = flatten_parsed_data(parse_qs((await request.body()).decode()))
        task_id = flat.get("data[FIELDS_AFTER][ID]")
        member_id = flat.get("auth[member_id]")
        domain = flat.get("auth[domain]")
        expires = int(flat.get("auth[expires]", 0))

        if not all([task_id, member_id, domain]):
            return JSONResponse({"status": "error", "message": "Missing fields"}, status_code=400)
        chat = member_map.get(member_id)
        user_data = tokens.get(chat)
        if not user_data:
            return JSONResponse({"status": "error", "message": "User not authenticated"}, status_code=401)

        # Обновление токена если просрочен
        if expires < time():
            if not await refresh_token(chat):
                await bot.send_message(int(chat), "❌ Токен просрочен, выполните /start")
                return JSONResponse({"status": "error", "message": "Token expired"}, status_code=401)

        # Получаем полные данные о задаче через API
        async with httpx.AsyncClient() as client:
            task_resp = await client.get(
                f"https://{domain}/rest/tasks.task.get.json",
                params={"auth": user_data["access_token"], "taskId": task_id}
            )
            task_resp.raise_for_status()
            task_info = task_resp.json().get("result", {}).get("task", {})

        title = task_info.get("title") or flat.get("data[FIELDS_AFTER][TITLE]")
        deadline = task_info.get("deadline") or flat.get("data[FIELDS_AFTER][DEADLINE]")

        # Формирование сообщения
        msg = f"🆕 Новая задача: {title or 'ID '+task_id}"
        msg += f"\n🔗 https://{domain}/company/personal/user/{user_data['bitrix_user_id']}/tasks/task/view/{task_id}/"
        if deadline:
            msg += f"\n⏰ Срок: {deadline}"

        await bot.send_message(int(chat), msg)
        return JSONResponse({"status": "ok"})

    except Exception as e:
        print(f"Handler error: {e}")
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)

# --- Команды Telegram ---
@dp.message(Command("start"))
async def cmd_start(m: Message):
    state = str(m.from_user.id)
    auth_url = (
        f"https://oauth.bitrix.info/oauth/authorize/?client_id={BITRIX_CLIENT_ID}"
        f"&response_type=code&state={state}&redirect_uri={REDIRECT_URI}"
    )
    await m.answer(f"🔑 Для подключения Bitrix24 перейдите по ссылке:\n{auth_url}")

@dp.message(Command("task"))
async def cmd_task(m: Message):
    """Создаёт задачу. Синтаксис: /task Заголовок | YYYY-MM-DD"""
    state = str(m.from_user.id)
    ud = tokens.get(state)
    if not ud:
        await m.answer("❗ Сначала авторизуйтесь: /start")
        return
    if int(time()) > int(ud.get("expires", 0)):
        if not await refresh_token(state):
            await m.answer("❌ Переавторизуйтесь: /start")
            return

    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await m.answer("❗ Используйте: /task Заголовок | YYYY-MM-DD")
        return
    body = parts[1]
    if '|' in body:
        title, due = [p.strip() for p in body.split('|', 1)]
    else:
        title = body.strip()
        due = None

    payload = {"fields": {"TITLE": title,
                             "CREATED_BY": ud["bitrix_user_id"],
                             "RESPONSIBLE_ID": ud["bitrix_user_id"]},
               "auth": ud["access_token"]}
    if due:
        payload["fields"]["DEADLINE"] = due

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(f"https://{ud['domain']}/rest/tasks.task.add.json", json=payload)
            resp.raise_for_status()
            res = resp.json()
            task = res.get("result", {}).get("task")
            if task:
                msg = f"✅ Задача создана! ID: {task['id']}"
                if due:
                    msg += f"; срок: {due}"
                await m.answer(msg)
            else:
                await m.answer(f"❌ Ошибка создания: {res.get('error_description', res.get('error', 'Unknown error'))}")
        except Exception as e:
            await m.answer(f"🚫 Ошибка: {e}")

# --- Запуск приложения ---
async def main():
    import uvicorn
    server = uvicorn.Server(uvicorn.Config(app=app, host="0.0.0.0", port=5000, log_level="info"))
    await asyncio.gather(server.serve(), dp.start_polling(bot))

if __name__ == "__main__":
    asyncio.run(main())
