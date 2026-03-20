from flask import Flask, jsonify
import requests
from websocket_handler import start_websocket, LIVE_DATA
import threading
from flask_socketio import SocketIO

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyREFNNDUiLCJqdGkiOiI2OWJjY2QyYzAyYWIzNTEyZTllNGRkNjUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3Mzk4MDk3MiwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzc0MDQ0MDAwfQ.TSE3WUYiqF-Iz7H_nvKNinx6Wm8ryf3Q6AvgKOKpAf4"


def get_option_chain(instrument, expiry):
    url = "https://api.upstox.com/v2/option/chain"

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    params = {
        "instrument_key": instrument,
        "expiry_date": expiry
    }

    response = requests.get(url, headers=headers, params=params)
    return response.json()


@app.route('/option-chain')
def option_chain():
    return jsonify(get_ltp())


def run_ws():
    start_websocket(ACCESS_TOKEN)

    import requests

def get_ltp():
    url = "https://api.upstox.com/v2/market-quote/ltp"

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    params = {
        "instrument_key": "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank"
    }

    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    return data


if __name__ == '__main__':
    get_ltp()
    threading.Thread(target=run_ws).start()
    socketio.run(app, host="0.0.0.0", port=5000)
