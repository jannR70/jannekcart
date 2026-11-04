#!/usr/bin/env python3
"""
JannekCart Online-Server
========================
Starten:
    pip install aiohttp
    python server.py            (Standard-Port 8080, oder: python server.py --port 9000)

Dann im Browser öffnen:  http://localhost:8080
Freunde im selben WLAN nutzen die Adresse, die beim Start ausgegeben wird.

Der Server
  * liefert die Spieldatei cart-online.html aus (muss im selben Ordner liegen),
  * verwaltet Räume (max. 6 Spieler pro Raum),
  * leitet Positionen und Item-Ereignisse zwischen den Spielern weiter.
Jeder Spieler berechnet sein eigenes Kart selbst, der Server ist nur der Vermittler.
"""
import argparse
import json
import os
import random
import socket
import string
import uuid
from pathlib import Path

from aiohttp import web, WSMsgType

MAX_PLAYERS = 6
HTML_FILE = Path(__file__).with_name("cart-online.html")

# Nachrichten, die einfach an alle anderen im Raum weitergereicht werden
RELAY_TYPES = {"s", "box", "banana", "bhit", "rocket", "rhit"}
# Felder, die in diesen Nachrichten erlaubt sind (alles andere wird verworfen)
ALLOWED_KEYS = {
    "x", "z", "h", "v", "st", "dd", "dt", "b", "sp", "inv", "lap", "pr", "fin", "ft", "c",  # Zustand
    "i", "hid", "rid", "pf", "lat",                                                       # Items
}


class Player:
    __slots__ = ("id", "name", "slot", "ws")

    def __init__(self, name, slot, ws):
        self.id = uuid.uuid4().hex[:8]
        self.name = name
        self.slot = slot
        self.ws = ws


class Room:
    def __init__(self, code):
        self.code = code
        self.players = {}      # id -> Player
        self.host = None       # id des Hosts
        self.racing = False

    def free_slot(self):
        used = {p.slot for p in self.players.values()}
        return next(s for s in range(MAX_PLAYERS) if s not in used)

    def lobby_msg(self):
        return {
            "t": "lobby",
            "room": self.code,
            "host": self.host,
            "players": [
                {"id": p.id, "name": p.name, "slot": p.slot}
                for p in sorted(self.players.values(), key=lambda p: p.slot)
            ],
        }

    async def broadcast(self, msg, exclude=None):
        data = json.dumps(msg, separators=(",", ":"))
        for p in list(self.players.values()):
            if p.id == exclude:
                continue
            try:
                await p.ws.send_str(data)
            except Exception:
                pass


rooms = {}


def clean_room(value):
    value = "".join(c for c in str(value or "").upper() if c.isalnum())
    return value[:12]


def clean_name(value, slot):
    value = " ".join(str(value or "").split())[:14]
    return value or f"Fahrer{slot + 1}"


def random_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase, k=4))
        if code not in rooms:
            return code


def clean_payload(m):
    out = {}
    for key, val in m.items():
        if key not in ALLOWED_KEYS:
            continue
        if isinstance(val, bool) or isinstance(val, (int, float)):
            out[key] = val
        elif isinstance(val, str) and len(val) <= 24:
            out[key] = val
    return out


async def leave(room, player):
    if room is None or player is None or player.id not in room.players:
        return
    del room.players[player.id]
    if not room.players:
        if rooms.get(room.code) is room:
            del rooms[room.code]
        return
    if room.host == player.id:
        room.host = next(iter(room.players))
    await room.broadcast({"t": "leave", "id": player.id})
    await room.broadcast(room.lobby_msg())


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=15, max_msg_size=16384)
    await ws.prepare(request)
    player = None
    room = None
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                m = json.loads(msg.data)
            except ValueError:
                continue
            if not isinstance(m, dict):
                continue
            t = m.get("t")

            # ---- Beitreten ----
            if player is None:
                if t != "join":
                    continue
                code = clean_room(m.get("room")) or random_code()
                r = rooms.get(code)
                error = None
                if r and r.racing:
                    error = "In diesem Raum läuft gerade ein Rennen."
                elif r and len(r.players) >= MAX_PLAYERS:
                    error = f"Der Raum ist voll (max. {MAX_PLAYERS} Spieler)."
                if error:
                    await ws.send_str(json.dumps({"t": "error", "msg": error}))
                    break
                if r is None:
                    r = Room(code)
                    rooms[code] = r
                slot = r.free_slot()
                player = Player(clean_name(m.get("name"), slot), slot, ws)
                r.players[player.id] = player
                if r.host is None:
                    r.host = player.id
                room = r
                await ws.send_str(json.dumps({"t": "joined", "id": player.id, "slot": slot, "room": code}))
                await room.broadcast(room.lobby_msg())
                continue

            # ---- Host-Aktionen ----
            if t == "start":
                if player.id == room.host and not room.racing:
                    room.racing = True
                    await room.broadcast({"t": "start"})
            elif t == "rematch":
                if player.id == room.host and room.racing:
                    room.racing = False
                    await room.broadcast({"t": "rematch"})

            # ---- Weiterleiten (Position, Items) ----
            elif t in RELAY_TYPES and room.racing:
                payload = clean_payload(m)
                payload["t"] = t
                payload["id"] = player.id
                await room.broadcast(payload, exclude=player.id)
    finally:
        await leave(room, player)
    return ws


async def index(request):
    if not HTML_FILE.exists():
        return web.Response(text="cart-online.html fehlt (muss neben server.py liegen).", status=404)
    return web.FileResponse(HTML_FILE)


def lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def main():
    parser = argparse.ArgumentParser(description="JannekCart Online-Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    args = parser.parse_args()

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)

    print("=" * 50)
    print(" JannekCart Server läuft")
    print(f"  Du selbst:        http://localhost:{args.port}")
    print(f"  Freunde (WLAN):   http://{lan_ip()}:{args.port}")
    print("  Beenden mit Strg+C")
    print("=" * 50)
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
