import websocket
import requests
import json

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyREFNNDUiLCJqdGkiOiI2OWJjY2QyYzAyYWIzNTEyZTllNGRkNjUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3Mzk4MDk3MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzc0MDQ0MDAwfQ.TSE3WUYiqF-Iz7H_nvKNinx6Wm8ryf3Q6AvgKOKpAf4"


def get_ws_url():
    url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    res = requests.get(url, headers=headers)
    data = res.json()

    print("FULL RESPONSE:", data)

    return data["data"]["authorized_redirect_uri"]


def on_open(ws):
    print("✅ CONNECTED")

    data = {
        "guid": "test",
        "method": "sub",
        "data": {
            "mode": "full",
            "instrumentKeys": [
                "NSE_INDEX|Nifty 50",
                "NSE_INDEX|Nifty Bank"
            ]
        }
    }

    ws.send(json.dumps(data))


def on_message(ws, message):
    print("📊 DATA:", message)


def on_error(ws, error):
    print("❌ ERROR:", error)


def on_close(ws, a, b):
    print("🔌 CLOSED")


ws_url = get_ws_url()

ws = websocket.WebSocketApp(
    ws_url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()