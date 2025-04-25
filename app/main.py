from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/handler")
async def handler(request: Request):
    data = await request.json()
    print("Handler got data:", data)
    return {"status": "ok"}

@app.get("/callback")
async def callback(code: str):
    print("Callback got code:", code)
    return {"status": "received", "code": code}
