#!/usr/bin/env python3
"""
RemoteStream 中继服务器
部署到 Render / Railway 免费实例
两个客户端(PC+手机)通过同一个 device_id 配对, 中继转发加密数据
"""

import asyncio, json, signal, sys, os
from websockets.asyncio.server import serve

# device_id -> {pc: ws, phone: ws}
rooms: dict[str, dict[str, any]] = {}

async def handler(ws, request):
    path = request.path.strip("/")  # /ws/{device_id}
    parts = path.split("/")
    device_id = parts[-1] if len(parts) >= 2 and parts[0] == "ws" else ""

    if not device_id or len(device_id) < 8:
        await ws.close(4000, "Invalid device_id")
        return

    # 等待认证
    try:
        msg = await asyncio.wait_for(ws.recv(), timeout=10)
        auth = json.loads(msg)
        if auth.get("type") != "auth":
            await ws.close(4001, "Auth required")
            return
    except (asyncio.TimeoutError, json.JSONDecodeError):
        await ws.close(4001, "Auth timeout or invalid")
        return

    role = auth.get("role", "pc")  # "pc" or "phone"

    # 初始化房间
    if device_id not in rooms:
        rooms[device_id] = {}
    room = rooms[device_id]

    # 同角色踢掉旧连接
    old = room.get(role)
    if old and not old.closed:
        await old.close(4002, "Replaced")
    room[role] = ws

    # 通知对端
    peer_role = "phone" if role == "pc" else "pc"
    peer = room.get(peer_role)
    if peer and not peer.closed:
        await peer.send(json.dumps({"type": "peer_connected", "role": role}))

    print(f"[+] {device_id[:12]} {role} | room={len(room)} peers")

    try:
        async for message in ws:
            # 转发给对端 (二进制或文本)
            peer = room.get(peer_role)
            if peer and not peer.closed:
                await peer.send(message)
    except Exception:
        pass
    finally:
        if room.get(role) is ws:
            del room[role]
        if not room:
            rooms.pop(device_id, None)
        # 通知对端断开
        peer = room.get(peer_role) if device_id in rooms else None
        if peer and not peer.closed:
            try:
                await peer.send(json.dumps({"type": "peer_disconnected", "role": role}))
            except: pass
        print(f"[-] {device_id[:12]} {role} | room={len(room) if device_id in rooms else 0} peers")

async def main():
    port = int(os.environ.get("PORT", 10000))
    print(f"Relay starting on port {port}")
    async with serve(handler, "0.0.0.0", port):
        await asyncio.get_running_loop().create_future()  # run forever

if __name__ == "__main__":
    asyncio.run(main())
