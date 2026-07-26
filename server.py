#!/usr/bin/env python3
"""
RemoteStream Relay v2 — WebSocket(PC) + SSE推送(手机)
帧到达即推送, 无需轮询
"""

import asyncio, json, os, time, threading
from aiohttp import web

# ============================================================
class Room:
    def __init__(self):
        self.pc_ws = None
        self.phone_queue: asyncio.Queue | None = None  # SSE 推送队列
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
#  WebSocket: PC 连接
# ============================================================

async def handle_ws(request):
    did = request.match_info.get("device_id", "")
    if not did or len(did) < 8:
        return web.Response(status=400, text="bad id")

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
                # PC 发来的加密帧 → 立即推给手机 SSE
                if room.phone_queue:
                    await room.phone_queue.put(msg.data)
            elif msg.type == web.WSMsgType.TEXT:
                pass
    except Exception:
        pass
    finally:
        room.pc_ws = None
    return ws

# ============================================================
#  SSE: 手机推送流 (替代轮询)
# ============================================================

async def handle_sse(request):
    """Server-Sent Events — 帧到达即推送"""
    did = request.match_info.get("device_id", "")
    if not did or len(did) < 8:
        return web.Response(status=400)

    room = get_room(did)
    room.phone_queue = asyncio.Queue(maxsize=8)
    room.last_seen["phone"] = time.time()

    resp = web.StreamResponse()
    resp.headers["Content-Type"] = "text/event-stream"
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["Connection"] = "keep-alive"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    await resp.prepare(request)

    try:
        # 发送初始连接确认
        await resp.write(b"event: connected\ndata: ok\n\n")

        while True:
            data = await asyncio.wait_for(room.phone_queue.get(), timeout=30)
            room.last_seen["phone"] = time.time()

            # SSE 格式: 二进制帧用 base64 编码
            import base64 as b64
            encoded = b64.b64encode(data).decode()
            # 分块发送 (每块最多 64KB, 防止 SSE 行过长)
            chunk_size = 48000  # 64KB base64 ≈ 48K chars
            for i in range(0, len(encoded), chunk_size):
                chunk = encoded[i:i+chunk_size]
                msg = f"data: {chunk}\n\n"
                await resp.write(msg.encode())

            # 帧结束标记
            await resp.write(b"event: frame_end\ndata: \n\n")
    except (asyncio.TimeoutError, ConnectionResetError, Exception):
        pass
    finally:
        room.phone_queue = None

    return resp

# ============================================================
#  HTTP POST: 手机发送加密音频
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
    return web.json_response({
        "pc_connected": room.pc_ws is not None and not room.pc_ws.closed,
        "phone_connected": room.phone_queue is not None
    })

async def index(request):
    return web.json_response({"service": "RemoteStream Relay v2"})

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
    app.router.add_get("/stream/{device_id}/camera", handle_sse)
    app.router.add_post("/send/{device_id}/audio", handle_send)
    app.router.add_get("/status/{device_id}", handle_status)

    asyncio.create_task(periodic_cleanup())

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Relay v2 on :{port}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
