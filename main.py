import os
import asyncio
import websockets
import json
import MarketDataFeed_pb2 as pb2
from google.protobuf.json_format import MessageToDict
from dotenv import load_dotenv
import requests
import pandas as pd # CSV read karne ke liye

# .env loading
load_dotenv()

# ✅ AAPKA ASLI TOKEN (Wapas daal diya hai)
TOKEN = os.getenv("ACCESS_TOKEN") or "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyREFNNDUiLCJqdGkiOiI2OWJlMmJlZjkzM2UwNzZmNTU4NGVlMGIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3NDA3MDc2NywiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzc0MTMwNDAwfQ._QSI4dlJD94xtntJJ8PFQ688a7eXvNzgvvnYdbRCVac"

class UpstoxDataFetcher:
    def __init__(self):
        self.websocket = None

    def get_authorized_ws_url(self):
        """Upstox se authorized URL lene ke liye"""
        url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "application/json"
        }
        try:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                print("❌ Auth URL Error:", response.text)
                return None
            data = response.json()
            return data["data"]["authorized_redirect_uri"]
        except Exception as e:
            print(f"❌ Auth Request Failed: {e}")
            return None

    async def connect(self):
        try:
            auth_ws_url = self.get_authorized_ws_url()
            if not auth_ws_url:
                print("❌ Failed to get authorized URL")
                return False

            print("🔗 Authorized URL Mili: ", auth_ws_url[:60], "...")
            self.websocket = await websockets.connect(auth_ws_url)
            print("✅ Successfully Connected to Upstox!")
            return True
        except Exception as e:
            print(f"❌ Connection Failed: {e}")
            return False

    async def subscribe(self, instrument_keys):
        """Dono Indices aur Option Strikes ko subscribe karne ke liye"""
        subscription_payload = {
            "guid": "request1",
            "method": "sub",
            "data": {
                "instrumentKeys": instrument_keys,
                "mode": "full" # Professional chain ke liye full mode (OI + Volume)
            }
        }
        await self.websocket.send(json.dumps(subscription_payload).encode('utf-8'))
        print(f"🚀 Sent subscription request for {len(instrument_keys)} instruments!")

    async def fetch_live_data(self):
        print("📡 Waiting for live data feed...")
        while True:
            try:
                message = await self.websocket.recv()
                feed = pb2.MarketDataFeed()
                feed.ParseFromString(message)
                data_dict = MessageToDict(feed)
                
                if data_dict:
                    # Sirf testing ke liye terminal par prices dikhane ke liye
                    for key, val in data_dict.get("feeds", {}).items():
                        # LTP dhoondna
                        ltp = val.get('ff', {}).get('marketFF', {}).get('ltpc', {}).get('ltp', '0')
                        print(f"📈 LIVE: {key} -> Price: {ltp}")
                else:
                    print("⚠️ Market is Closed or No Movement.")
                
            except Exception as e:
                print(f"❌ Data Fetch Error: {e}")
                break

if __name__ == "__main__":
    fetcher = UpstoxDataFetcher()
    async def main():
        if await fetcher.connect():
            # 1. Base Indices (Nifty aur Bank Nifty)
            all_keys = ["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank"]
            
            # 2. CSV se Option Strikes load karna
            try:
                if os.path.exists("current_strikes.csv"):
                    df = pd.read_csv("current_strikes.csv")
                    # Hum 30-40 strikes subscribe karte hain (Top 20 CE + 20 PE)
                    option_keys = df['instrument_key'].tolist()[:40]
                    all_keys.extend(option_keys)
                    print(f"📂 CSV se {len(option_keys)} strikes uthayi gayi hain.")
                else:
                    print("⚠️ current_strikes.csv nahi mili! Check 'get_strikes.py'.")
            except Exception as e:
                print(f"⚠️ CSV Error: {e}")

            await fetcher.subscribe(all_keys)
            await fetcher.fetch_live_data()
    
    asyncio.run(main())