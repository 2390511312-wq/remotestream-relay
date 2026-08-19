#!/usr/bin/env python3
"""
RemoteStream Relay v3 — WebSocket(PC) + 批量轮询(手机)
Render 免费实例长连接不稳定, 使用短 HTTP 批量拉帧
"""

import asyncio, json, os, time, threading, base64, struct
from collections import deque
from aiohttp import web

# ============================================================
class Room:
    def __init__(self):
        self.pc_ws = None
        self.frames: deque = deque(maxlen=10)  # 最多缓存10帧
        self.last_seen: dict[str, float] = {}

rooms: dict[str, Room] = {}
rooms_lock = threading.Lock()

def get_room(did: str) -> Room:
    with rooms_lock:
        if did not in rooms:
            rooms[did] = Room()
        return rooms[did]

def cleanup():
    now = time.time()
    with rooms_lock:
        stale = [d for d, r in rooms.items()
                 if now - max(r.last_seen.values(), default=0) > 300]
        for d in stale: del rooms[d]

# ============================================================
#  WebSocket: PC
# ============================================================

async def handle_ws(request):
    did = request.match_info.get("device_id", "")
    if not did or len(did) < 8:
        return web.Response(status=400)

    ws = web.WebSocketResponse(max_msg_size=256*1024)
    await ws.prepare(request)
    room = get_room(did)
    room.pc_ws = ws
    room.last_seen["pc"] = time.time()

    try:
        await ws.send_str(json.dumps({"type": "connected"}))
        async for msg in ws:
            room.last_seen["pc"] = time.time()
            if msg.type == web.WSMsgType.BINARY:
                room.frames.append(msg.data)
    except Exception:
        pass
    finally:
        room.pc_ws = None
    return ws

# ============================================================
#  批量轮询: 手机 (替代 SSE)
# ============================================================

async def handle_poll(request):
    """返回所有缓存帧, 用长度前缀打包"""
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    room.last_seen["phone"] = time.time()

    if not room.frames:
        # 等最多 600ms, 有新帧立即返回
        await asyncio.sleep(0.6)

    frames = []
    while room.frames:
        frames.append(room.frames.popleft())

    if not frames:
        return web.Response(status=204)

    # 打包: [4字节帧数][每帧: 4字节长度+数据]
    buf = bytearray()
    buf.extend(struct.pack(">I", len(frames)))
    for f in frames:
        buf.extend(struct.pack(">I", len(f)))
        buf.extend(f)

    return web.Response(body=bytes(buf), content_type="application/octet-stream")

# ============================================================
#  HTTP POST: 手机音频
# ============================================================

async def handle_send(request):
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    room.last_seen["phone"] = time.time()
    data = await request.read()
    if room.pc_ws and not room.pc_ws.closed:
        await room.pc_ws.send_bytes(data)
    return web.Response(text="ok")

async def handle_status(request):
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    now = time.time()
    phone_online = now - room.last_seen.get("phone", 0) < 5  # 5秒内轮询视为在线
    return web.json_response({
        "pc_connected": room.pc_ws is not None and not room.pc_ws.closed,
        "frames_queued": len(room.frames),
        "phone_online": phone_online
    })

async def index(request):
    return web.json_response({"service": "RemoteStream Relay v3"})

# ============================================================

async def periodic_cleanup():
    while True:
        await asyncio.sleep(60)
        cleanup()

async def main():
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws/{device_id}", handle_ws)
    app.router.add_get("/poll/{device_id}/camera", handle_poll)
    app.router.add_post("/send/{device_id}/audio", handle_send)
    app.router.add_get("/status/{device_id}", handle_status)
    asyncio.create_task(periodic_cleanup())
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Relay v3 on :{port}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
