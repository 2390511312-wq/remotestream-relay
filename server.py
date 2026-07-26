#!/usr/bin/env python3
"""
RemoteStream 中继 — WebSocket + HTTP 轮询
部署: Render / Railway 免费实例, 一键启动
"""

import asyncio, json, os, signal, time, threading
from datetime import datetime
from urllib.parse import urlparse

# ============================================================
#  房间管理
# ============================================================

class Room:
    def __init__(self):
        self.pc_ws = None       # PC WebSocket
        self.phone_ws = None    # 手机 WebSocket (备用)
        self.pc_pending: list[bytes] = []  # PC → Phone 待转发帧
        self.phone_pending: list[bytes] = []  # Phone → PC 待转发音频
        self.last_seen: dict[str, float] = {}  # role -> timestamp

rooms: dict[str, Room] = {}
rooms_lock = threading.Lock()

def get_room(did: str) -> Room:
    with rooms_lock:
        if did not in rooms:
            rooms[did] = Room()
        return rooms[did]

def cleanup_rooms():
    """清理超过 5 分钟无活动的房间"""
    now = time.time()
    with rooms_lock:
        stale = [did for did, r in rooms.items()
                 if now - max(r.last_seen.values(), default=0) > 300]
        for did in stale:
            del rooms[did]

# ============================================================
#  HTTP + WebSocket 服务器
# ============================================================

from aiohttp import web
import aiohttp

async def handle_ws(request):
    """WebSocket: PC 客户端连接"""
    did = request.match_info.get("device_id", "")
    if not did or len(did) < 8:
        return web.Response(status=400, text="bad device_id")

    ws = web.WebSocketResponse(max_msg_size=256*1024)
    await ws.prepare(request)

    room = get_room(did)
    role = "pc"
    room.pc_ws = ws
    room.last_seen[role] = time.time()

    try:
        # 发送连接确认
        await ws.send_str(json.dumps({"type": "connected", "role": role}))

        # 处理消息
        async for msg in ws:
            room.last_seen[role] = time.time()
            if msg.type == aiohttp.WSMsgType.BINARY:
                # PC 发来的加密帧, 缓存给手机
                room.pc_pending.append(msg.data)
                # 只保留最新一帧 (丢弃旧帧减少延迟)
                if len(room.pc_pending) > 1:
                    room.pc_pending = room.pc_pending[-1:]
            elif msg.type == aiohttp.WSMsgType.TEXT:
                pass  # 控制消息
    except Exception:
        pass
    finally:
        room.pc_ws = None

    return ws

async def handle_http_poll(request):
    """HTTP 轮询: 手机获取摄像头帧"""
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    room.last_seen["phone"] = time.time()

    # 等待新帧 (最多 1.5 秒, 避免阻塞过久)
    waited = 0
    while not room.pc_pending and waited < 15:
        await asyncio.sleep(0.1)
        waited += 1

    if room.pc_pending:
        data = room.pc_pending.pop(0)
        return web.Response(body=data, content_type="application/octet-stream")
    else:
        return web.Response(status=204)  # No Content

async def handle_http_send(request):
    """HTTP POST: 手机发送加密音频"""
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    room.last_seen["phone"] = time.time()

    data = await request.read()
    if room.pc_ws and not room.pc_ws.closed:
        await room.pc_ws.send_bytes(data)
    return web.Response(text="ok")

async def handle_status(request):
    """状态查询"""
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    return web.json_response({
        "pc_connected": room.pc_ws is not None and not room.pc_ws.closed,
        "frames_pending": len(room.pc_pending)
    })

async def index(request):
    return web.json_response({"service": "RemoteStream Relay", "rooms": len(rooms)})

# ============================================================
#  启动
# ============================================================

async def periodic_cleanup():
    while True:
        await asyncio.sleep(60)
        cleanup_rooms()

async def main():
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws/{device_id}", handle_ws)
    app.router.add_get("/poll/{device_id}/camera", handle_http_poll)
    app.router.add_post("/send/{device_id}/audio", handle_http_send)
    app.router.add_get("/status/{device_id}", handle_status)

    asyncio.create_task(periodic_cleanup())

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Relay running on port {port}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
