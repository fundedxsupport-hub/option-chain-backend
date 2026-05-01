import os
import asyncio
import websockets
import json
import MarketDataFeed_pb2 as pb2
from google.protobuf.json_format import MessageToDict
from dotenv import load_dotenv

# .env se token uthao
load_dotenv()
# Yahan dhyan dena: .env mein ACCESS_TOKEN="aapka_token" hona chahiye
TOKEN = os.getenv("ACCESS_TOKEN") or "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyREFNNDUiLCJqdGkiOiI2OWY0NzdjNGFkOTZlNTQ5ZWI5OWJhNDYiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3NzYyOTEyNCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzc3NjcyODAwfQ.s04NiQ6iei25LBvayXy7P2A4eWIn6X2-sCF7vvdEqAA"

class UpstoxDataFetcher:
    def __init__(self):
        # Upstox v3 URL (LTP/Full mode ke liye latest yahi hai)
        self.ws_url = f"wss://api.upstox.com/v3/feed/market-data-feed?access_token={TOKEN}"
        self.websocket = None

    async def connect(self):
        """WebSocket connection banata hai"""
        try:
            self.websocket = await websockets.connect(self.ws_url)
            print("✅ Successfully Connected to Upstox!")
            return True
        except Exception as e:
            print(f"❌ Connection Failed: {e}")
            return False

    async def subscribe(self, instrument_keys):
        """Specific stocks/indices ko subscribe karne ke liye"""
        data = {
            "guid": "request1",
            "method": "sub",
            "data": {
                "instrumentKeys": instrument_keys,
                "mode": "full"
            }
        }
        # Request bhej rahe hain
        await self.websocket.send(json.dumps(data))
        print(f"Sent subscription request for: {instrument_keys}")

    async def fetch_live_data(self):
        """Lagaatar data receive aur decode karne ke liye"""
        print("Waiting for live data feed...")
        while True:
            try:
                print("🔥 Live loop chal raha hai...")   # 👈 YE LINE ADD KAR
                message = await self.websocket.recv()
                print("RAW DATA:", message)
                
                # Protobuf decoding logic
                feed = pb2.MarketDataFeed()
                feed.ParseFromString(message)
                
                # Dictionary mein badal kar print karna
                data_dict = MessageToDict(feed)
                
                if data_dict:
                    print(f"Data Received: {data_dict}")
                else:
                    print("Market is Closed - Data is Empty.")
                
            except Exception as e:
                print(f"Data Fetch Error: {e}")
                break

# Test karne ke liye (Nifty + Bank Nifty dono add kar diye hain)
if __name__ == "__main__":
    fetcher = UpstoxDataFetcher()
    async def main():
        if await fetcher.connect():
            # YAHAN UPDATE KIYA HAI: Dono indices ek saath
            indices = ["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank"]
            await fetcher.subscribe(indices)
            await fetcher.fetch_live_data()
    
    asyncio.run(main())