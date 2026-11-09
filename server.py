#!/usr/bin/env python3
"""
RemoteStream Relay v4 — 多设备 + 长连接流式
- PC: WebSocket 上行（二进制帧）
- 手机: /stream 长连接持续接收（多台手机各自独立广播）
- 兼容 v3: /poll 批量轮询
- 可选鉴权: PC 连接后发送 SHA256(secret|device_id)，手机带 token
"""

import asyncio, json, os, time, threading, struct, hmac
from collections import deque
from aiohttp import web

# ============================================================
class Room:
    def __init__(self):
        self.pc_ws = None
        self.frames: deque = deque(maxlen=10)   # 兼容 v3 轮询
        self.last_seen: dict[str, float] = {}
        self.mode: str = 'camera'
        self.ratio: float = 0.5
        self.auth_token: str = ''
        self.streams: set = set()               # 每台手机一个 asyncio.Queue
        self.stream_sent: int = 0               # 已成功写入手机连接的帧数（诊断用）

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
                 if not r.streams and now - max(r.last_seen.values(), default=0) > 300]
        for d in stale:
            del rooms[d]

def push_frame(room: Room, data: bytes):
    """PC 收到一帧后：写入兼容队列，并广播给所有流式手机。"""
    room.frames.append(data)
    for q in list(room.streams):
        try:
            q.put_nowait(data)
        except asyncio.QueueFull:
            # 慢客户端：丢掉最旧的一帧，保低延迟
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass

def auth_ok(room: Room, request) -> bool:
    if not room.auth_token:
        return True
    tok = request.query.get('token', '') or request.headers.get('X-RS-Token', '')
    return hmac.compare_digest(tok, room.auth_token)

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
                push_frame(room, msg.data)

            elif msg.type == web.WSMsgType.TEXT:
                # 可选鉴权：{"type":"auth","token":"<sha256>"}
                try:
                    obj = json.loads(msg.data)
                    if obj.get("type") == "auth":
                        tok = str(obj.get("token", ""))
                        if len(tok) == 64:
                            room.auth_token = tok
                            await ws.send_str(json.dumps({"type": "auth_ok"}))
                except Exception:
                    pass
    except Exception:
        pass
    finally:
        if room.pc_ws is ws:
            room.pc_ws = None
    return ws

# ============================================================
#  长连接流式: 手机（每台手机一条连接，互不影响）
# ============================================================

async def handle_stream(request):
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    if not auth_ok(room, request):
        return web.Response(status=403, text="forbidden")

    resp = web.StreamResponse(status=200, headers={
        "Content-Type": "application/octet-stream",
        "Cache-Control": "no-cache, no-store",
        "X-Accel-Buffering": "no",     # 禁用 Render/Nginx 缓冲
    })
    await resp.prepare(request)

    q: asyncio.Queue = asyncio.Queue(maxsize=4)
    room.streams.add(q)
    room.last_seen["phone"] = time.time()

    try:
        # 先送最近一帧，避免首屏空等
        if room.frames:
            latest = room.frames[-1]
            await resp.write(struct.pack(">I", len(latest)) + latest)
            await resp.drain()

        while True:
            try:
                data = await asyncio.wait_for(q.get(), timeout=10)
                room.last_seen["phone"] = time.time()
                await resp.write(struct.pack(">I", len(data)) + data)
                room.stream_sent += 1
            except asyncio.TimeoutError:
                # 心跳：长度为 0 的帧，手机端忽略
                await resp.write(struct.pack(">I", 0))
            await resp.drain()
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    except Exception:
        pass
    finally:
        room.streams.discard(q)
    return resp

# ============================================================
#  兼容 v3: 批量轮询
# ============================================================

async def handle_poll(request):
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    room.last_seen["phone"] = time.time()

    if not room.frames:
        await asyncio.sleep(0.6)

    frames = []
    while room.frames:
        frames.append(room.frames.popleft())

    if not frames:
        return web.Response(status=204)

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
    if not auth_ok(room, request):
        return web.Response(status=403, text="forbidden")
    room.last_seen["phone"] = time.time()
    data = await request.read()
    if room.pc_ws and not room.pc_ws.closed:
        await room.pc_ws.send_bytes(data)
    return web.Response(text="ok")

async def handle_status(request):
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    now = time.time()
    phone_online = bool(room.streams) or now - room.last_seen.get("phone", 0) < 5
    return web.json_response({
        "pc_connected": room.pc_ws is not None and not room.pc_ws.closed,
        "frames_queued": len(room.frames),
        "phone_online": phone_online,
        "phones": len(room.streams),
        "stream_sent": room.stream_sent,
        "mode": room.mode,
        "ratio": room.ratio
    })

async def handle_mode(request):
    did = request.match_info.get("device_id", "")
    room = get_room(did)
    if request.method == "POST":
        try:
            data = await request.json()
            mode = data.get("mode", "camera")
            if mode in ('camera', 'screen', 'parallel', 'bandwidth'):
                room.mode = mode
                ratio = data.get("ratio", room.ratio)
                room.ratio = max(0.0, min(1.0, float(ratio)))
                return web.json_response({"success": True, "mode": mode, "ratio": room.ratio})
        except Exception:
            pass
        return web.json_response({"success": False}, status=400)
    else:
        return web.json_response({"mode": room.mode, "ratio": room.ratio})

async def index(request):
    return web.json_response({"service": "RemoteStream Relay v4"})

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
    app.router.add_get("/stream/{device_id}", handle_stream)
    app.router.add_get("/poll/{device_id}/camera", handle_poll)
    app.router.add_post("/send/{device_id}/audio", handle_send)
    app.router.add_get("/status/{device_id}", handle_status)
    app.router.add_get("/mode/{device_id}", handle_mode)
    app.router.add_post("/mode/{device_id}", handle_mode)
    asyncio.create_task(periodic_cleanup())
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Relay v4 on :{port}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
