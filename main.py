import os
import asyncio
import websockets
import json
import threading
from pathlib import Path

import MarketDataFeed_pb2 as pb2
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI
from google.protobuf.json_format import MessageToDict

app = FastAPI()

option_chain_data = {
    "feeds": {},
    "type": None,
    "currentTs": None,
}

instrument_meta = {}

BASE_DIR = Path(__file__).resolve().parent

load_dotenv(BASE_DIR / ".env")
TOKEN = (os.getenv("UPSTOX_ACCESS_TOKEN") or os.getenv("ACCESS_TOKEN") or "").strip()


class UpstoxDataFetcher:
    def __init__(self):
        self.websocket = None

    def get_authorized_ws_url(self):
        url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Accept": "*/*",
        }

        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                print("Auth URL Error:", response.status_code, response.text)
                return None

            data = response.json()
            return data["data"]["authorized_redirect_uri"]
        except Exception as e:
            print(f"Auth Request Failed: {e}")
            return None

    async def connect(self):
        try:
            if not TOKEN:
                print("UPSTOX_ACCESS_TOKEN .env me nahi mila")
                return False

            auth_ws_url = self.get_authorized_ws_url()
            if not auth_ws_url:
                print("Failed to get authorized URL")
                return False

            print("Authorized URL Mili:", auth_ws_url[:60], "...")
            self.websocket = await websockets.connect(
                auth_ws_url,
                max_size=None,
                ping_interval=20,
                ping_timeout=20,
                open_timeout=30,
            )
            print("Successfully Connected to Upstox!")
            return True
        except Exception as e:
            print(f"Connection Failed: {e}")
            return False

    async def subscribe(self, instrument_keys):
        subscription_payload = {
            "guid": "option-chain-1",
            "method": "sub",
            "data": {
                "instrumentKeys": instrument_keys,
                "mode": "full",
            },
        }

        await self.websocket.send(json.dumps(subscription_payload).encode("utf-8"))
        print(f"Sent subscription request for {len(instrument_keys)} instruments!")

    async def fetch_live_data(self):
        global option_chain_data

        print("Waiting for live data feed...")

        option_chain_data = {
            "feeds": {},
            "type": None,
            "currentTs": None,
        }

        while True:
            try:
                message = await self.websocket.recv()

                feed = pb2.FeedResponse()
                feed.ParseFromString(message)
                data_dict = MessageToDict(feed, preserving_proto_field_name=False)

                if data_dict.get("type") == "market_info":
                    print("Market status:", data_dict.get("marketInfo", {}))
                    continue

                feeds = data_dict.get("feeds", {})
                if not feeds:
                    print("No feed yet. Waiting...")
                    continue

                option_chain_data.setdefault("feeds", {})
                option_chain_data["feeds"].update(feeds)
                option_chain_data["type"] = data_dict.get("type")
                option_chain_data["currentTs"] = data_dict.get("currentTs")

                expected_count = len(instrument_meta) + 2
                cached_count = len(option_chain_data["feeds"])
                print(f"Cached feeds: {cached_count} / {expected_count}")

                for key, val in feeds.items():
                    full_feed = val.get("fullFeed", {})
                    market_ff = full_feed.get("marketFF", {})
                    index_ff = full_feed.get("indexFF", {})
                    ltpc = (
                        market_ff.get("ltpc")
                        or index_ff.get("ltpc")
                        or val.get("ltpc")
                        or {}
                    )

                    ltp = ltpc.get("ltp", "0")
                    print(f"LIVE: {key} -> Price: {ltp}")

            except Exception as e:
                print(f"Data Fetch Error: {e}")
                break


def get_last_price(symbol):
    url = f"https://api.upstox.com/v2/market-quote/quotes?symbol={symbol}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            print(f"LTP Error for {symbol}:", response.status_code, response.text)
            return None

        data = response.json().get("data", {})
        if not data:
            print(f"LTP empty for {symbol}")
            return None

        first_value = next(iter(data.values()))
        return float(first_value.get("last_price"))
    except Exception as e:
        print(f"LTP fetch failed for {symbol}: {e}")
        return None


def round_to_step(price, step):
    return round(price / step) * step


def select_atm_strikes(df, index_name, spot_price, step, strikes_each_side=50):
    if spot_price is None:
        print(f"{index_name} spot nahi mila, fallback first strikes use honge.")
        return df.head(strikes_each_side * 4)

    atm = round_to_step(spot_price, step)
    low = atm - (strikes_each_side * step)
    high = atm + (strikes_each_side * step)

    selected = df[
        (df["strike"] >= low)
        & (df["strike"] <= high)
    ].copy()

    selected = selected.sort_values(by=["strike", "option_type"])

    print(f"{index_name} spot: {spot_price}")
    print(f"{index_name} ATM strike: {atm}")
    print(f"{index_name} selected range: {low} - {high}")
    print(f"{index_name} selected contracts: {len(selected)}")

    return selected


@app.get("/option-chain")
def get_chain():
    return {
        "feeds": option_chain_data.get("feeds", {}),
        "instruments": instrument_meta,
        "cached_feed_count": len(option_chain_data.get("feeds", {})),
        "instrument_count": len(instrument_meta),
        "currentTs": option_chain_data.get("currentTs"),
    }


def start_backend():
    fetcher = UpstoxDataFetcher()

    async def main():
        if await fetcher.connect():
            all_keys = ["NSE_INDEX|Nifty 50", "NSE_INDEX|Nifty Bank"]

            try:
                csv_path = BASE_DIR / "current_strikes.csv"

                if csv_path.exists():
                    df = pd.read_csv(csv_path)
                    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
                    df = df.dropna(
                        subset=["strike", "instrument_key", "tradingsymbol"]
                    )

                    symbols = df["tradingsymbol"].astype(str)

                    nifty_all = df[
                        symbols.str.startswith("NIFTY")
                        & ~symbols.str.startswith("BANKNIFTY")
                        & ~symbols.str.startswith("FINNIFTY")
                        & ~symbols.str.startswith("MIDCPNIFTY")
                    ].copy()

                    banknifty_all = df[
                        symbols.str.startswith("BANKNIFTY")
                    ].copy()

                    nifty_spot = get_last_price("NSE_INDEX|Nifty 50")
                    banknifty_spot = get_last_price("NSE_INDEX|Nifty Bank")

                    nifty_df = select_atm_strikes(
                        nifty_all,
                        "NIFTY",
                        nifty_spot,
                        50,
                        strikes_each_side=50,
                    )

                    banknifty_df = select_atm_strikes(
                        banknifty_all,
                        "BANKNIFTY",
                        banknifty_spot,
                        100,
                        strikes_each_side=50,
                    )

                    selected_df = pd.concat([nifty_df, banknifty_df])

                    instrument_meta.clear()

                    for _, row in selected_df.iterrows():
                        key = str(row["instrument_key"])
                        instrument_meta[key] = {
                            "tradingsymbol": str(row.get("tradingsymbol", "")),
                            "name": str(row.get("name", "")),
                            "strike": float(row.get("strike", 0)),
                            "option_type": str(row.get("option_type", "")),
                        }

                    option_keys = (
                        selected_df["instrument_key"].dropna().astype(str).tolist()
                    )
                    all_keys.extend(option_keys)

                    print(f"NIFTY strikes: {len(nifty_df)}")
                    print(f"BANKNIFTY strikes: {len(banknifty_df)}")
                    print(f"CSV se total {len(option_keys)} strikes uthayi gayi hain.")
                else:
                    print("current_strikes.csv nahi mili. Pehle python get_strikes.py run karo.")
            except Exception as e:
                print(f"CSV Error: {e}")

            await fetcher.subscribe(all_keys)
            await fetcher.fetch_live_data()

    asyncio.run(main())


threading.Thread(target=start_backend, daemon=True).start()
