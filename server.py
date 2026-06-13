import asyncio
import aiohttp
import io
import os
from contextlib import asynccontextmanager
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

PASSWORD = "@ZikoB0SSXCT"

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, key: str):
        await websocket.accept()
        if key not in self.active_connections:
            self.active_connections[key] = set()
        self.active_connections[key].add(websocket)

    def disconnect(self, websocket: WebSocket, key: str):
        if key in self.active_connections:
            self.active_connections[key].discard(websocket)
            if not self.active_connections[key]:
                del self.active_connections[key]

    async def broadcast(self, key: str, message: dict):
        if key in self.active_connections:
            for conn in list(self.active_connections[key]):
                try:
                    await conn.send_json(message)
                except:
                    self.disconnect(conn, key)

manager = ConnectionManager()
bots_data: Dict[str, dict] = {}
chats_messages: Dict[str, dict] = {}
chats_lock = asyncio.Lock()
bots_lock = asyncio.Lock()
http_session: aiohttp.ClientSession = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_session
    http_session = aiohttp.ClientSession()
    yield
    await http_session.close()
    for token in list(bots_data.keys()):
        await stop_bot(token)

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def tg_api(method: str, token: str, **kwargs):
    url = f"https://api.telegram.org/bot{token}/{method}"
    async with http_session.post(url, json=kwargs, timeout=30) as resp:
        return await resp.json()

async def tg_api_file(token: str, method: str, files: dict, data: dict):
    url = f"https://api.telegram.org/bot{token}/{method}"
    async with http_session.post(url, data=data, files=files, timeout=60) as resp:
        return await resp.json()

async def get_file_url(token: str, file_id: str):
    res = await tg_api("getFile", token, file_id=file_id)
    if res.get("ok"):
        return f"https://api.telegram.org/file/bot{token}/{res['result']['file_path']}"
    return None

async def fetch_chat_info(token: str, chat_id: str):
    try:
        res = await tg_api("getChat", token, chat_id=chat_id)
        if res.get("ok"):
            chat = res["result"]
            title = chat.get("title") or f"{chat.get('first_name','')} {chat.get('last_name','')}".strip() or str(chat_id)
            photo_url = None
            if chat.get("photo"):
                file_id = chat["photo"]["big_file_id"]
                photo_url = await get_file_url(token, file_id)
            return {"title": title, "photo": photo_url, "type": chat["type"]}
    except:
        pass
    return {"title": str(chat_id), "photo": None, "type": "private"}

async def fetch_user_info(token: str, user_id: int):
    try:
        res = await tg_api("getChat", token, chat_id=user_id)
        if res.get("ok"):
            user = res["result"]
            name = f"{user.get('first_name','')} {user.get('last_name','')}".strip()
            photo_url = None
            photos = await tg_api("getUserProfilePhotos", token, user_id=user_id, limit=1)
            if photos.get("ok") and photos["result"]["photos"]:
                file_id = photos["result"]["photos"][0][-1]["file_id"]
                photo_url = await get_file_url(token, file_id)
            return {"name": name or str(user_id), "photo": photo_url}
    except:
        pass
    return {"name": str(user_id), "photo": None}

async def store_message(token: str, msg: dict, chat_id: str):
    text = msg.get("text") or msg.get("caption") or ""
    media_url = None
    media_type = None
    for mtype in ["photo", "video", "document", "audio", "sticker"]:
        if mtype in msg:
            file_id = msg[mtype][-1]["file_id"] if mtype == "photo" else msg[mtype]["file_id"]
            media_url = await get_file_url(token, file_id)
            media_type = mtype
            break
    sender_id = msg.get("from", {}).get("id") if msg.get("from") else None
    sender_info = await fetch_user_info(token, sender_id) if sender_id else {"name": "القناة", "photo": None}
    async with chats_lock:
        if chat_id not in chats_messages:
            info = await fetch_chat_info(token, chat_id)
            chats_messages[chat_id] = {"title": info["title"], "photo": info["photo"], "type": info["type"], "messages": []}
        chat = chats_messages[chat_id]
        chat["messages"].append({
            "from": "user" if msg.get("from") else "channel",
            "text": text,
            "media_url": media_url,
            "media_type": media_type,
            "sender_name": sender_info["name"],
            "sender_photo": sender_info["photo"],
            "timestamp": msg["date"],
            "message_id": msg["message_id"],
            "reply_to_message_id": msg.get("reply_to_message", {}).get("message_id") if msg.get("reply_to_message") else None
        })
        if len(chat["messages"]) > 200:
            chat["messages"] = chat["messages"][-200:]
    key = f"{token}:{chat_id}"
    await manager.broadcast(key, {"type": "new_message", "message": chat["messages"][-1]})
    return True

async def poll_bot(token: str):
    offset = 0
    while bots_data.get(token, {}).get("running", False):
        try:
            url = f"https://api.telegram.org/bot{token}/getUpdates"
            params = {"offset": offset, "timeout": 30}
            async with http_session.get(url, params=params, timeout=35) as resp:
                data = await resp.json()
                if data.get("ok"):
                    for upd in data["result"]:
                        offset = upd["update_id"] + 1
                        msg = None
                        chat_id = None
                        if "message" in upd:
                            msg = upd["message"]
                            chat_id = str(msg["chat"]["id"])
                        elif "channel_post" in upd:
                            msg = upd["channel_post"]
                            chat_id = str(msg["chat"]["id"])
                        if msg and chat_id:
                            asyncio.create_task(store_message(token, msg, chat_id))
                else:
                    await asyncio.sleep(1)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"Poll error for {token}: {e}")
            await asyncio.sleep(3)
    async with bots_lock:
        if token in bots_data:
            bots_data[token]["running"] = False

async def start_bot(token: str):
    async with bots_lock:
        if token in bots_data and bots_data[token]["running"]:
            return
        bots_data[token] = {"offset": 0, "running": True, "info": {}}
    me = await tg_api("getMe", token)
    if me.get("ok"):
        info = bots_data[token]["info"]
        info["username"] = me["result"]["username"]
        info["name"] = me["result"]["first_name"]
        photos = await tg_api("getUserProfilePhotos", token, user_id=me["result"]["id"], limit=1)
        if photos.get("ok") and photos["result"]["photos"]:
            file_id = photos["result"]["photos"][0][-1]["file_id"]
            photo_url = await get_file_url(token, file_id)
            info["photo_url"] = photo_url
    asyncio.create_task(poll_bot(token))

async def stop_bot(token: str):
    async with bots_lock:
        if token in bots_data:
            bots_data[token]["running"] = False

@app.post("/check_password")
async def check_password(data: dict):
    if data.get("password") == PASSWORD:
        return {"status": "ok"}
    raise HTTPException(status_code=403, detail="Wrong password")

@app.post("/add_bot")
async def add_bot(data: dict):
    token = data.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="Token missing")
    me = await tg_api("getMe", token)
    if not me.get("ok"):
        raise HTTPException(status_code=400, detail="Invalid token")
    await start_bot(token)
    return {"status": "ok", "username": me["result"]["username"]}

@app.get("/get_bots")
async def get_bots():
    bots = []
    async with bots_lock:
        for token, data in bots_data.items():
            if data["running"]:
                info = data.get("info", {})
                bots.append({
                    "token": token,
                    "username": info.get("username", ""),
                    "name": info.get("name", ""),
                    "photo_url": info.get("photo_url")
                })
    return {"bots": bots}

@app.delete("/delete_bot")
async def delete_bot(data: dict):
    token = data.get("token")
    await stop_bot(token)
    async with bots_lock:
        bots_data.pop(token, None)
    return {"status": "ok"}

@app.get("/get_chats")
async def get_chats(token: str):
    result = []
    async with chats_lock:
        for cid, chat in chats_messages.items():
            last_msg = chat["messages"][-1] if chat["messages"] else None
            last_text = last_msg["text"][:50] if last_msg and last_msg["text"] else ("[وسائط]" if last_msg else "")
            result.append({
                "id": cid,
                "title": chat["title"],
                "photo": chat["photo"],
                "type": chat.get("type", "private"),
                "last_message": last_text
            })
    return {"chats": result}

@app.get("/get_messages/{chat_id}")
async def get_messages(chat_id: str, limit: int = 50):
    async with chats_lock:
        chat = chats_messages.get(chat_id)
        if not chat:
            return {"info": None, "messages": []}
        return {
            "info": {"title": chat["title"], "photo": chat["photo"], "type": chat["type"]},
            "messages": chat["messages"][-limit:]
        }

@app.post("/send_message")
async def send_message(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    text = data.get("text")
    reply_to_msg_id = data.get("reply_to_message_id")
    if not all([token, chat_id, text]):
        raise HTTPException(status_code=400)
    payload = {"chat_id": chat_id, "text": text}
    if reply_to_msg_id:
        payload["reply_to_message_id"] = reply_to_msg_id
    res = await tg_api("sendMessage", token, **payload)
    return {"ok": res.get("ok")}

@app.post("/send_photo")
async def send_photo(token: str = Form(...), chat_id: str = Form(...), photo: UploadFile = File(...)):
    try:
        content = await photo.read()
        files = {"photo": (photo.filename, io.BytesIO(content), photo.content_type)}
        res = await tg_api_file(token, "sendPhoto", files, {"chat_id": chat_id})
        return {"ok": res.get("ok")}
    except Exception as e:
        print(f"Send photo error: {e}")
        return {"ok": False}

@app.post("/send_video")
async def send_video(token: str = Form(...), chat_id: str = Form(...), video: UploadFile = File(...)):
    try:
        content = await video.read()
        files = {"video": (video.filename, io.BytesIO(content), video.content_type)}
        res = await tg_api_file(token, "sendVideo", files, {"chat_id": chat_id})
        return {"ok": res.get("ok")}
    except Exception as e:
        print(f"Send video error: {e}")
        return {"ok": False}

@app.post("/send_document")
async def send_document(token: str = Form(...), chat_id: str = Form(...), document: UploadFile = File(...)):
    try:
        content = await document.read()
        files = {"document": (document.filename, io.BytesIO(content), document.content_type)}
        res = await tg_api_file(token, "sendDocument", files, {"chat_id": chat_id})
        return {"ok": res.get("ok")}
    except Exception as e:
        print(f"Send document error: {e}")
        return {"ok": False}

@app.post("/send_reaction")
async def send_reaction(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    emoji = data.get("emoji")
    if not all([token, chat_id, message_id, emoji]):
        raise HTTPException(status_code=400)
    res = await tg_api("setMessageReaction", token, chat_id=chat_id, message_id=message_id,
                       reaction=[{"type": "emoji", "emoji": emoji}])
    return {"ok": res.get("ok")}

@app.post("/leave_chat")
async def leave_chat(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    res = await tg_api("leaveChat", token, chat_id=chat_id)
    async with chats_lock:
        chats_messages.pop(chat_id, None)
    return {"ok": res.get("ok")}

@app.post("/join_chat")
async def join_chat(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    if not token or not chat_id:
        raise HTTPException(status_code=400)
    info = await fetch_chat_info(token, chat_id)
    if not info.get("type"):
        raise HTTPException(status_code=404, detail="Chat not found")
    async with chats_lock:
        if chat_id not in chats_messages:
            chats_messages[chat_id] = {"title": info["title"], "photo": info["photo"], "type": info["type"], "messages": []}
    return {"status": "ok"}

@app.post("/pin_message")
async def pin_message(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not all([token, chat_id, message_id]):
        raise HTTPException(status_code=400)
    res = await tg_api("pinChatMessage", token, chat_id=chat_id, message_id=message_id)
    return {"ok": res.get("ok")}

@app.post("/delete_message")
async def delete_message(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not all([token, chat_id, message_id]):
        raise HTTPException(status_code=400)
    res = await tg_api("deleteMessage", token, chat_id=chat_id, message_id=message_id)
    return {"ok": res.get("ok")}

@app.post("/restrict_user")
async def restrict_user(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    user_id = data.get("user_id")
    until_date = data.get("until_date", 2147483647)
    if not all([token, chat_id, user_id]):
        raise HTTPException(status_code=400)
    permissions = {
        "can_send_messages": False,
        "can_send_media_messages": False,
        "can_send_polls": False,
        "can_send_other_messages": False,
        "can_add_web_page_previews": False
    }
    res = await tg_api("restrictChatMember", token, chat_id=chat_id, user_id=user_id,
                       permissions=permissions, until_date=until_date)
    return {"ok": res.get("ok")}

@app.post("/ban_user")
async def ban_user(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    user_id = data.get("user_id")
    if not all([token, chat_id, user_id]):
        raise HTTPException(status_code=400)
    res = await tg_api("banChatMember", token, chat_id=chat_id, user_id=user_id)
    return {"ok": res.get("ok")}

@app.post("/unban_user")
async def unban_user(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    user_id = data.get("user_id")
    if not all([token, chat_id, user_id]):
        raise HTTPException(status_code=400)
    res = await tg_api("unbanChatMember", token, chat_id=chat_id, user_id=user_id)
    return {"ok": res.get("ok")}

@app.post("/set_bot_name")
async def set_bot_name(data: dict):
    token = data.get("token")
    name = data.get("name")
    if not token or not name:
        raise HTTPException(status_code=400)
    res = await tg_api("setMyName", token, name=name)
    if res.get("ok"):
        async with bots_lock:
            if token in bots_data:
                bots_data[token]["info"]["name"] = name
        return {"status": "ok"}
    return {"status": "error"}

@app.post("/set_bot_description")
async def set_bot_description(data: dict):
    token = data.get("token")
    description = data.get("description")
    if not token or description is None:
        raise HTTPException(status_code=400)
    res = await tg_api("setMyDescription", token, description=description)
    return {"ok": res.get("ok")}

@app.post("/set_bot_photo")
async def set_bot_photo(token: str = Form(...), photo: UploadFile = File(...)):
    try:
        content = await photo.read()
        files = {"photo": (photo.filename, io.BytesIO(content), photo.content_type)}
        res = await tg_api_file(token, "setMyPhoto", files, {})
        if res.get("ok"):
            me = await tg_api("getMe", token)
            if me.get("ok"):
                photos = await tg_api("getUserProfilePhotos", token, user_id=me["result"]["id"], limit=1)
                if photos.get("ok") and photos["result"]["photos"]:
                    file_id = photos["result"]["photos"][0][-1]["file_id"]
                    photo_url = await get_file_url(token, file_id)
                    async with bots_lock:
                        if token in bots_data:
                            bots_data[token]["info"]["photo_url"] = photo_url
            return {"status": "ok"}
        return {"status": "error"}
    except:
        return {"status": "error"}

@app.post("/get_permissions")
async def get_permissions(data: dict):
    token = data.get("token")
    chat_id = data.get("chat_id")
    if not token or not chat_id:
        raise HTTPException(status_code=400)
    me = await tg_api("getMe", token)
    if not me.get("ok"):
        return {"status": "error", "is_admin": False}
    bot_id = me["result"]["id"]
    res = await tg_api("getChatMember", token, chat_id=chat_id, user_id=bot_id)
    if res.get("ok") and res["result"]["status"] in ["administrator", "creator"]:
        return {"status": "ok", "is_admin": True, "permissions": res["result"]}
    return {"status": "ok", "is_admin": False, "permissions": None}

@app.websocket("/ws/{token}/{chat_id}")
async def websocket_endpoint(websocket: WebSocket, token: str, chat_id: str):
    key = f"{token}:{chat_id}"
    await manager.connect(websocket, key)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, key)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)