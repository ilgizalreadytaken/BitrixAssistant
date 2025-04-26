import os
import threading
import requests
from typing import Dict
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

# Configuration
CLIENT_ID      = os.getenv("BITRIX_CLIENT_ID")
CLIENT_SECRET  = os.getenv("BITRIX_CLIENT_SECRET")
REDIRECT_URI   = os.getenv("REDIRECT_URI")        # https://callback.mybitrixbot.ru
WEBHOOK_DOMAIN = os.getenv("WEBHOOK_DOMAIN")      # https://handler.mybitrixbot.ru
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

# In-memory storage
tokens: Dict[str, Dict[str, str]] = {}       # chat_id -> { access_token, refresh_token, domain }
member_map: Dict[str, str] = {}                # bitrix member_id -> chat_id

# FastAPI and Bot setup
app = FastAPI()
bot = Bot(token=TELEGRAM_TOKEN, parse_mode="HTML")
dp = Dispatcher()

# --- FastAPI endpoints ---
@app.get("/callback")
async def oauth_callback(request: Request):
    # Step 1: receive code, state, domain, member_id from Bitrix24
    code      = request.query_params.get("code")
    state     = request.query_params.get("state")      # telegram chat_id
    domain    = request.query_params.get("domain")
    member_id = request.query_params.get("member_id")
    if not code or not state or not domain or not member_id:
        raise HTTPException(400, "Missing code/state/domain/member_id")

    # Step 2: exchange code for tokens
    resp = requests.post(
        "https://oauth.bitrix24.com/oauth/token/",
        data={
            "grant_type":    "authorization_code",
            "code":          code,
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri":  REDIRECT_URI
        }
    )
    data = resp.json()
    if "access_token" not in data:
        raise HTTPException(400, data)

    # Step 3: store tokens keyed by chat_id
    tokens[state] = {
        "access_token":  data["access_token"],
        "refresh_token": data["refresh_token"],
        "domain":        domain
    }
    # map bitrix member_id to telegram chat
    member_map[member_id] = state

    # Step 4: bind ONTASKADD event automatically
    bind_resp = requests.post(
        f"https://{domain}/rest/event.bind",
        data={
            "event":   "ONTASKADD",
            "handler": f"{WEBHOOK_DOMAIN}/handler",
            "auth":    data["access_token"]
        }
    )

    return JSONResponse({
        "status": "ok",
        "bound": bind_resp.json()
    })

@app.post("/handler")
async def bitrix_handler(request: Request):
    # receive incoming webhook
    body = await request.json()
    event = body.get("event")
    auth  = body.get("auth", {})
    data  = body.get("data", [])
    member_id = auth.get("member_id")
    if not member_id or member_id not in member_map:
        return JSONResponse({"status": "unknown_member"}, status_code=403)

    chat_id = member_map[member_id]
    user = tokens.get(chat_id)
    if not user:
        return JSONResponse({"status": "no_token"}, status_code=403)

    # handle task addition event
    if event == "ONTASKADD" and data:
        # parse task id from FIELDS_AFTER
        record = data[0]
        after  = record.get("FIELDS_AFTER", {})
        task_id = after.get("ID")
        if task_id:
            await bot.send_message(
                chat_id=chat_id,
                text=f"🆕 В Bitrix24 создана новая задача #{task_id}"
            )
    return {"status": "ok"}

# --- Aiogram handlers ---
@dp.message(Command("start"))
async def cmd_start(m: Message):
    state   = str(m.from_user.id)
    auth_url = (
        f"https://oauth.bitrix24.com/oauth/authorize/"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&state={state}"
        f"&redirect_uri={REDIRECT_URI}/callback"
    )
    await m.answer(
        f"Привет! Для подключения Bitrix24 перейди по ссылке:\n{auth_url}"
    )

@dp.message(Command("task"))
async def cmd_task(m: Message):
    state = str(m.from_user.id)
    user = tokens.get(state)
    if not user:
        await m.answer("❗ Сначала отправьте /start и завершите авторизацию.")
        return
    # get title from message text
    title = m.get_args() or "Задача из бота"
    at     = user["access_token"]
    domain = user["domain"]

    # call tasks.task.add
    resp = requests.post(
        f"https://{domain}/rest/tasks.task.add",
        json={
            "fields": {"TITLE": title},
            "auth": at
        }
    )
    result = resp.json()
    if "taskId" in result:
        await m.answer(f"✅ Задача создана, её ID: {result['taskId']}")
    else:
        await m.answer(f"❌ Ошибка создания задачи: {result}")

# --- Run both services ---
def start_services():
    # run FastAPI in a thread
    threading.Thread(
        target=lambda: __import__('uvicorn').run("main:app", host="0.0.0.0", port=8000),
        daemon=True
    ).start()
    # run Telegram polling
    dp.run_polling(bot)

if __name__ == "__main__":
    start_services()
