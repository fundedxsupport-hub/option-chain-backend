"""Run: python test_upstox_connect.py  (from option-chain-backend folder)"""
import asyncio
import base64
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
from dotenv import dotenv_values

BASE = Path(__file__).resolve().parent
env = dotenv_values(BASE / ".env")


def load_token() -> str:
    for name in ("UPSTOX_ACCESS_TOKEN", "ACCESS_TOKEN"):
        v = (os.environ.get(name) or env.get(name) or "").strip().strip('"').strip("'")
        if v:
            return v
    return ""


def main() -> int:
    token = load_token()
    if not token:
        print("FAIL: no UPSTOX_ACCESS_TOKEN in .env")
        return 1

    try:
        part = token.split(".")[1]
        part += "=" * (-len(part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(part))
        exp = payload.get("exp")
        if exp:
            print(f"expiry={datetime.fromtimestamp(exp)} expired={datetime.now().timestamp() >= exp}")
    except Exception as exc:
        print(f"jwt_warning={exc}")

    r = requests.get(
        "https://api.upstox.com/v3/feed/market-data-feed/authorize",
        headers={"Authorization": f"Bearer {token}", "Accept": "*/*"},
        timeout=20,
    )
    print(f"authorize_http={r.status_code}")
    if r.status_code != 200:
        print(r.text[:400])
        return 2

    ws_url = r.json()["data"]["authorized_redirect_uri"]

    async def try_ws() -> bool:
        import websockets

        try:
            async with websockets.connect(ws_url, open_timeout=25):
                print("websocket=OK")
                return True
        except Exception as exc:
            print(f"websocket=FAIL {exc}")
            return False

    return 0 if asyncio.run(try_ws()) else 3


if __name__ == "__main__":
    raise SystemExit(main())
