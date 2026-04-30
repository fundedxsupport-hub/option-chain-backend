import os
import asyncio
import websockets
import json
import threading
import base64
from pathlib import Path
from urllib.parse import quote

import MarketDataFeed_pb2 as pb2
import pandas as pd
import requests
from dotenv import dotenv_values
from fastapi import FastAPI, Query
from google.protobuf.json_format import MessageToDict

from get_strikes import fetch_and_filter_strikes

app = FastAPI()

@app.get("/")
def home():
    return {"status": "running"}

option_chain_data = {
    "feeds": {},
    "type": None,
    "currentTs": None,
}

instrument_meta = {}
all_instrument_meta = {}
available_expiries = {
    "NIFTY": [],
    "BANKNIFTY": [],
}
subscribed_expiries = {
    "NIFTY": [],
    "BANKNIFTY": [],
}

BASE_DIR = Path(__file__).resolve().parent
ACTIVE_EXPIRY_COUNT = 3
STRIKES_EACH_SIDE = 20

DOTENV_VALUES = dotenv_values(BASE_DIR / ".env")


def clean_token(value):
    return (value or "").strip().strip('"').strip("'")


def get_token_info():
    # Render/dashboard environment should win. .env is only a local fallback.
    for name in ("UPSTOX_ACCESS_TOKEN", "ACCESS_TOKEN"):
        value = clean_token(os.environ.get(name))
        if value:
            return f"env:{name}", value

    for name in ("UPSTOX_ACCESS_TOKEN", "ACCESS_TOKEN"):
        value = clean_token(DOTENV_VALUES.get(name))
        if value:
            return f".env:{name}", value

    return "missing", ""


def print_token_debug(source, token):
    if not token:
        print("Token debug: token missing")
        return

    expiry = "unknown"
    try:
        payload_part = token.split(".")[1]
        padded_payload = payload_part + "=" * (-len(payload_part) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded_payload))
        if payload.get("exp"):
            from datetime import datetime

            expiry = datetime.fromtimestamp(payload["exp"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
    except Exception:
        pass

    print(
        "Token debug:",
        f"source={source}",
        f"length={len(token)}",
        f"starts={token[:8]}",
        f"ends={token[-8:]}",
        f"expiry={expiry}",
    )


TOKEN_SOURCE, TOKEN = get_token_info()
print_token_debug(TOKEN_SOURCE, TOKEN)


def refresh_strike_csv_on_startup():
    try:
        print("Startup strike refresh shuru...")
        fetch_and_filter_strikes()
        print("Startup strike refresh complete.")
    except Exception as e:
        print(f"Startup strike refresh failed: {e}")


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
                print("UPSTOX_ACCESS_TOKEN / ACCESS_TOKEN nahi mila")
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
                message = await asyncio.wait_for(self.websocket.recv(), timeout=30)

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

            except asyncio.TimeoutError:
                print("Ping keep-alive...")
                try:
                    await self.websocket.ping()
                except Exception as e:
                    print("Ping failed:", e)
                    break

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


def normalize_candle(candle):
    if not isinstance(candle, list) or len(candle) < 5:
        return None

    return {
        "time": candle[0],
        "open": safe_float(candle[1]),
        "high": safe_float(candle[2]),
        "low": safe_float(candle[3]),
        "close": safe_float(candle[4]),
        "volume": safe_float(candle[5]) if len(candle) > 5 else 0,
        "oi": safe_float(candle[6]) if len(candle) > 6 else 0,
    }


def round_to_step(price, step):
    return round(price / step) * step


def safe_float(value, default=0.0):
    try:
        number = float(value)
        if pd.isna(number):
            return default
        return number
    except Exception:
        return default


def get_expiry_list(df):
    if "expiry" not in df.columns:
        return []

    expiries = pd.to_datetime(df["expiry"], errors="coerce").dropna()
    return [
        pd.Timestamp(expiry).date().isoformat()
        for expiry in sorted(expiries.unique())
    ]


def select_atm_strikes(
    df,
    index_name,
    spot_price,
    step,
    strikes_each_side=50,
    expiry_count=ACTIVE_EXPIRY_COUNT,
):
    if spot_price is None:
        print(f"{index_name} spot nahi mila, fallback first strikes use honge.")
        return df.head(strikes_each_side * 4)

    if "expiry" in df.columns:
        expiries = pd.to_datetime(df["expiry"], errors="coerce")
        nearest_expiries = sorted(expiries.dropna().unique())[:expiry_count]
        if nearest_expiries:
            expiry_dates = [
                pd.Timestamp(expiry).date().isoformat()
                for expiry in nearest_expiries
            ]
            df = df[expiries.isin(nearest_expiries)].copy()
            subscribed_expiries[index_name] = expiry_dates
            print(f"{index_name} subscribed expiries: {expiry_dates}")

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
def get_chain(
    symbol: str | None = Query(default=None),
    expiry: str | None = Query(default=None),
):
    filtered_meta = all_instrument_meta or instrument_meta

    if symbol:
        symbol_upper = symbol.upper()
        filtered_meta = {
            key: value
            for key, value in filtered_meta.items()
            if value.get("name", "").upper() == symbol_upper
        }

    if expiry:
        filtered_meta = {
            key: value
            for key, value in filtered_meta.items()
            if value.get("expiry") == expiry
        }

    allowed_keys = set(filtered_meta.keys()) | {
        "NSE_INDEX|Nifty 50",
        "NSE_INDEX|Nifty Bank",
    }
    filtered_feeds = {
        key: value
        for key, value in option_chain_data.get("feeds", {}).items()
        if key in allowed_keys
    }

    return {
        "feeds": filtered_feeds,
        "instruments": filtered_meta,
        "expiries": available_expiries,
        "subscribed_expiries": subscribed_expiries,
        "selected_symbol": symbol,
        "selected_expiry": expiry,
        "cached_feed_count": len(filtered_feeds),
        "total_cached_feed_count": len(option_chain_data.get("feeds", {})),
        "instrument_count": len(filtered_meta),
        "subscribed_instrument_count": len(instrument_meta),
        "total_instrument_count": len(all_instrument_meta or instrument_meta),
        "currentTs": option_chain_data.get("currentTs"),
    }


@app.get("/expiries")
def get_expiries():
    return {
        "expiries": available_expiries,
        "subscribed_expiries": subscribed_expiries,
    }


@app.get("/candles")
def get_candles(
    instrument_key: str = Query(...),
    unit: str = Query(default="minutes"),
    interval: int = Query(default=1),
):
    encoded_key = quote(instrument_key, safe="")
    url = (
        "https://api.upstox.com/v3/historical-candle/intraday/"
        f"{encoded_key}/{unit}/{interval}"
    )
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
        if response.status_code != 200:
            return {
                "status": "error",
                "message": response.text,
                "status_code": response.status_code,
                "instrument_key": instrument_key,
                "candles": [],
            }

        data = response.json()
        raw_candles = data.get("data", {}).get("candles", [])
        candles = [
            normalized
            for normalized in (normalize_candle(candle) for candle in raw_candles)
            if normalized is not None
        ]
        candles.reverse()

        return {
            "status": "success",
            "instrument_key": instrument_key,
            "unit": unit,
            "interval": interval,
            "candles": candles,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "instrument_key": instrument_key,
            "candles": [],
        }


def start_backend():
    refresh_strike_csv_on_startup()

    fetcher = UpstoxDataFetcher()

    async def main():
        while True:
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

                        available_expiries["NIFTY"] = get_expiry_list(nifty_all)
                        available_expiries["BANKNIFTY"] = get_expiry_list(banknifty_all)

                        nifty_spot = get_last_price("NSE_INDEX|Nifty 50")
                        banknifty_spot = get_last_price("NSE_INDEX|Nifty Bank")

                        nifty_df = select_atm_strikes(
                            nifty_all,
                            "NIFTY",
                            nifty_spot,
                            50,
                            strikes_each_side=STRIKES_EACH_SIDE,
                            expiry_count=ACTIVE_EXPIRY_COUNT,
                        )

                        banknifty_df = select_atm_strikes(
                            banknifty_all,
                            "BANKNIFTY",
                            banknifty_spot,
                            100,
                            strikes_each_side=STRIKES_EACH_SIDE,
                            expiry_count=ACTIVE_EXPIRY_COUNT,
                        )

                        selected_df = pd.concat([nifty_df, banknifty_df])

                        instrument_meta.clear()
                        all_instrument_meta.clear()

                        all_expiry_df = pd.concat(
                            [nifty_all, banknifty_all],
                            ignore_index=True,
                        )

                        for _, row in all_expiry_df.iterrows():
                            key = str(row["instrument_key"])
                            all_instrument_meta[key] = {
                                "tradingsymbol": str(row.get("tradingsymbol", "")),
                                "name": str(row.get("name", "")),
                                "expiry": str(row.get("expiry", "")),
                                "strike": safe_float(row.get("strike", 0)),
                                "last_price": safe_float(row.get("last_price", 0)),
                                "option_type": str(row.get("option_type", "")),
                            }

                        for _, row in selected_df.iterrows():
                            key = str(row["instrument_key"])
                            instrument_meta[key] = {
                                "tradingsymbol": str(row.get("tradingsymbol", "")),
                                "name": str(row.get("name", "")),
                                "expiry": str(row.get("expiry", "")),
                                "strike": safe_float(row.get("strike", 0)),
                                "last_price": safe_float(row.get("last_price", 0)),
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

            print("Reconnect ho raha hai 3 sec me...")
            await asyncio.sleep(3)

    asyncio.run(main())


threading.Thread(target=start_backend, daemon=True).start()
