import websocket
import json
import threading
import requests
import gzip
import market_data_pb2

LIVE_DATA = {}

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyREFNNDUiLCJqdGkiOiI2OWJjY2QyYzAyYWIzNTEyZTllNGRkNjUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3Mzk4MDk3MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzc0MDQ0MDAwfQ.TSE3WUYiqF-Iz7H_nvKNinx6Wm8ryf3Q6AvgKOKpAf4"


def get_ws_url():
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    response = requests.get(url, headers=headers, timeout=10)
    data = response.json()

    print("WS FULL RESPONSE:", data)

    if "data" not in data:
        raise Exception(f"WS URL ERROR: {data}")

    return data["data"]["authorized_redirect_uri"]


def on_message(ws, message):
    global LIVE_DATA

    try:
        import gzip

        if message[:2] == b'\x1f\x8b':
            message = gzip.decompress(message)

        print("🔥 RAW:", message)

        # 👇 JSON try (IMPORTANT)
        try:
            data = json.loads(message.decode("utf-8"))
            print("📊 JSON DATA:", data)

            if "data" in data:
                for key, value in data["data"].items():
                    if "ltpc" in value:
                        LIVE_DATA[key] = {
                            "last_price": value["ltpc"]["ltp"]
                        }

        except:
            print("⚡ Not JSON, protobuf maybe")

        print("📊 LIVE DATA:", LIVE_DATA)

    except Exception as e:
        print("❌ ERROR:", e)


def on_error(ws, error):
    print("❌ WS ERROR:", error)


def on_close(ws, close_status_code, close_msg):
    print("🔌 CLOSED")


def on_open(ws):
    print("🔥 on_open CALLED")
    print("✅ CONNECTED")

    subscribe_data = {
    "guid": "test",
    "method": "sub",
    "data": {
        "mode": "ltpc",
        "instrumentKeys": [
            "NSE_INDEX|Nifty Bank"
        ]
    }
}

    print("📤 SUBSCRIBE DATA:", subscribe_data)

    ws.send(json.dumps(subscribe_data))

    print("🔥 SUBSCRIBE SENT")


def start_websocket(token):
    global ACCESS_TOKEN
    ACCESS_TOKEN = token

    ws_url = get_ws_url()

    ws = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever(ping_interval=20, ping_timeout=10)