import os
import asyncio
import websockets
import json
import threading
import base64
from pathlib import Path
from urllib.parse import quote
from datetime import date, datetime, timedelta

import MarketDataFeed_pb2 as pb2
import pandas as pd
import requests
from dotenv import dotenv_values
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from google.protobuf.json_format import MessageToDict

from get_strikes import fetch_and_filter_strikes

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"status": "running"}

@app.websocket("/ws/option-chain")
async def option_chain_ws(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(
                {
                    "feeds": dict(option_chain_data.get("feeds", {})),
                    "type": option_chain_data.get("type"),
                    "currentTs": option_chain_data.get("currentTs"),
                    "is_live": option_chain_data.get("is_live", False),
                    "last_live_at": option_chain_data.get("last_live_at"),
                }
            )
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        return
    except Exception as e:
        print(f"Client websocket error: {e}")

option_chain_data = {
    "feeds": {},
    "type": None,
    "currentTs": None,
    "is_live": False,
    "last_live_at": None,
}

live_candles = {}
backend_thread = None

instrument_meta = {}
all_instrument_meta = {}
INDEX_CONFIG = {
    "NIFTY": {"name": "NIFTY", "exchange": "NSE_FO", "index_key": "NSE_INDEX|Nifty 50", "step": 50},
    "BANKNIFTY": {"name": "BANKNIFTY", "exchange": "NSE_FO", "index_key": "NSE_INDEX|Nifty Bank", "step": 100},
    "FINNIFTY": {"name": "FINNIFTY", "exchange": "NSE_FO", "index_key": "NSE_INDEX|Nifty Fin Service", "step": 50},
    "MIDCPNIFTY": {"name": "MIDCPNIFTY", "exchange": "NSE_FO", "index_key": "NSE_INDEX|NIFTY MID SELECT", "step": 25},
    "SENSEX": {"name": "SENSEX", "exchange": "BSE_FO", "index_key": "BSE_INDEX|SENSEX", "step": 100},
    "BANKEX": {"name": "BANKEX", "exchange": "BSE_FO", "index_key": "BSE_INDEX|BANKEX", "step": 100},
}

available_expiries = {symbol: [] for symbol in INDEX_CONFIG}
subscribed_expiries = {symbol: [] for symbol in INDEX_CONFIG}

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
                option_chain_data["is_live"] = True
                option_chain_data["last_live_at"] = datetime.now().isoformat()

                expected_count = len(instrument_meta) + 2
                cached_count = len(option_chain_data["feeds"])
                print(f"Cached feeds: {cached_count} / {expected_count}")

                for key, val in feeds.items():
                    update_live_candle(key, val, data_dict.get("currentTs"))
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
                option_chain_data["is_live"] = False
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


def parse_feed_timestamp(value):
    try:
        if value is None:
            return datetime.now()

        if isinstance(value, str) and value.isdigit():
            value = int(value)

        if isinstance(value, (int, float)):
            # Upstox currentTs is usually epoch milliseconds.
            if value > 100000000000:
                return datetime.fromtimestamp(value / 1000)
            return datetime.fromtimestamp(value)

        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None)
    except Exception:
        return datetime.now()


def candle_bucket_time(ts, interval):
    minute = (ts.minute // interval) * interval
    return ts.replace(minute=minute, second=0, microsecond=0)


def update_live_candle(instrument_key, feed, current_ts):
    if not instrument_key or not isinstance(feed, dict):
        return

    full_feed = feed.get("fullFeed", {})
    market_ff = full_feed.get("marketFF", {})
    index_ff = full_feed.get("indexFF", {})
    ff = market_ff or index_ff
    ltpc = ff.get("ltpc") or feed.get("ltpc") or {}

    ltp = safe_float(ltpc.get("ltp"), None)
    if ltp is None or ltp <= 0:
        return

    volume = safe_float(ff.get("vtt", 0))
    oi = safe_float(ff.get("oi", 0))
    ts = parse_feed_timestamp(current_ts)

    for interval in (1, 3, 5, 15):
        bucket = candle_bucket_time(ts, interval)
        time_key = bucket.isoformat()
        instrument_candles = live_candles.setdefault(instrument_key, {})
        interval_candles = instrument_candles.setdefault(interval, {})
        candle = interval_candles.get(time_key)

        if candle is None:
            interval_candles[time_key] = {
                "time": time_key,
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp,
                "volume": volume,
                "oi": oi,
            }
        else:
            candle["high"] = max(candle["high"], ltp)
            candle["low"] = min(candle["low"], ltp)
            candle["close"] = ltp
            candle["volume"] = max(candle.get("volume", 0), volume)
            candle["oi"] = oi or candle.get("oi", 0)

        if len(interval_candles) > 1500:
            for old_key in sorted(interval_candles.keys())[:-1200]:
                interval_candles.pop(old_key, None)

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


def infer_strike_step(df, fallback=50):
    try:
        strikes = sorted(pd.Series(df["strike"]).dropna().astype(float).unique())
        diffs = [round(strikes[i] - strikes[i - 1], 2) for i in range(1, len(strikes))]
        diffs = [diff for diff in diffs if diff > 0]
        if not diffs:
            return fallback
        return int(pd.Series(diffs).mode().iloc[0])
    except Exception:
        return fallback


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


def fallback_spot_from_strikes(df, step):
    try:
        strikes = sorted(pd.Series(df["strike"]).dropna().astype(float).unique())
        if not strikes:
            return None
        return round_to_step(strikes[len(strikes) // 2], step)
    except Exception:
        return None


def select_atm_strikes(
    df,
    index_name,
    spot_price,
    step,
    strikes_each_side=50,
    expiry_count=ACTIVE_EXPIRY_COUNT,
):
    working_df = df.copy()

    if "expiry" in working_df.columns:
        expiries = pd.to_datetime(working_df["expiry"], errors="coerce")
        nearest_expiries = sorted(expiries.dropna().unique())[:expiry_count]
        if nearest_expiries:
            expiry_dates = [
                pd.Timestamp(expiry).date().isoformat()
                for expiry in nearest_expiries
            ]
            working_df = working_df[expiries.isin(nearest_expiries)].copy()
            subscribed_expiries[index_name] = expiry_dates
            print(f"{index_name} subscribed expiries: {expiry_dates}")

    if working_df.empty:
        print(f"{index_name} selected expiry data empty.")
        return working_df

    if spot_price is None:
        spot_price = fallback_spot_from_strikes(working_df, step)
        print(f"{index_name} spot nahi mila, strike fallback center use hoga: {spot_price}")

    if spot_price is None:
        fallback_rows = strikes_each_side * 2 * max(1, expiry_count)
        print(f"{index_name} fallback first {fallback_rows} rows use honge.")
        return working_df.head(fallback_rows)

    atm = round_to_step(spot_price, step)
    low = atm - (strikes_each_side * step)
    high = atm + (strikes_each_side * step)

    selected = working_df[
        (working_df["strike"] >= low)
        & (working_df["strike"] <= high)
    ].copy()

    if selected.empty:
        strikes = sorted(working_df["strike"].dropna().astype(float).unique())
        nearest_strikes = sorted(strikes, key=lambda strike: abs(strike - atm))[
            : (strikes_each_side * 2) + 1
        ]
        selected = working_df[working_df["strike"].isin(nearest_strikes)].copy()

    sort_cols = [col for col in ["expiry", "strike", "option_type"] if col in selected.columns]
    if sort_cols:
        selected = selected.sort_values(by=sort_cols)

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
    feeds_snapshot = dict(option_chain_data.get("feeds", {}))
    instrument_meta_snapshot = dict(instrument_meta)
    all_instrument_meta_snapshot = dict(all_instrument_meta)
    expiries_snapshot = {key: list(value) for key, value in available_expiries.items()}
    subscribed_expiries_snapshot = {
        key: list(value) for key, value in subscribed_expiries.items()
    }

    # When app asks for a selected expiry, return the actively subscribed
    # ATM-range instruments. Returning every listed contract for that expiry
    # creates many rows without live feed and makes the mobile UI look blank.
    if symbol and expiry and instrument_meta_snapshot:
        filtered_meta = instrument_meta_snapshot
    else:
        filtered_meta = all_instrument_meta_snapshot or instrument_meta_snapshot

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

    allowed_keys = set(filtered_meta.keys()) | {config["index_key"] for config in INDEX_CONFIG.values()}
    filtered_feeds = {
        key: value
        for key, value in feeds_snapshot.items()
        if key in allowed_keys
    }
    for key, meta in filtered_meta.items():
        if key in filtered_feeds:
            continue
        fallback_feed = fallback_feed_from_meta(meta)
        if fallback_feed:
            filtered_feeds[key] = fallback_feed

    return {
        "feeds": filtered_feeds,
        "instruments": filtered_meta,
        "expiries": expiries_snapshot,
        "subscribed_expiries": subscribed_expiries_snapshot,
        "selected_symbol": symbol,
        "selected_expiry": expiry,
        "cached_feed_count": len(filtered_feeds),
        "total_cached_feed_count": len(feeds_snapshot),
        "instrument_count": len(filtered_meta),
        "subscribed_instrument_count": len(instrument_meta_snapshot),
        "total_instrument_count": len(all_instrument_meta_snapshot or instrument_meta_snapshot),
        "currentTs": option_chain_data.get("currentTs"),
        "is_live": option_chain_data.get("is_live", False),
        "last_live_at": option_chain_data.get("last_live_at"),
        "data_source": "live" if option_chain_data.get("is_live", False) else "fallback_csv",
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
    history_days: int = Query(default=7),
):
    encoded_key = quote(instrument_key, safe="")
    today = date.today()
    from_day = today - timedelta(days=max(1, min(history_days, 30)))
    historical_url = (
        "https://api.upstox.com/v3/historical-candle/"
        f"{encoded_key}/{unit}/{interval}/{today.isoformat()}/{from_day.isoformat()}"
    )
    intraday_url = (
        "https://api.upstox.com/v3/historical-candle/intraday/"
        f"{encoded_key}/{unit}/{interval}"
    )
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }

    raw_candles = []
    errors = []

    try:
        historical_response = requests.get(historical_url, headers=headers, timeout=20)
        if historical_response.status_code == 200:
            raw_candles.extend(
                historical_response.json().get("data", {}).get("candles", [])
            )
        else:
            errors.append(
                {
                    "source": "historical",
                    "status_code": historical_response.status_code,
                    "message": historical_response.text,
                }
            )

        intraday_response = requests.get(intraday_url, headers=headers, timeout=20)
        if intraday_response.status_code == 200:
            raw_candles.extend(
                intraday_response.json().get("data", {}).get("candles", [])
            )
        else:
            errors.append(
                {
                    "source": "intraday",
                    "status_code": intraday_response.status_code,
                    "message": intraday_response.text,
                }
            )

        candle_by_time = {}
        for normalized in (normalize_candle(candle) for candle in raw_candles):
            if normalized is None:
                continue
            candle_by_time[normalized["time"]] = normalized

        for live in live_candles.get(instrument_key, {}).get(interval, {}).values():
            candle_by_time[live["time"]] = live

        candles = sorted(candle_by_time.values(), key=lambda candle: candle["time"])

        if not candles:
            return {
                "status": "error",
                "message": "No candle data returned",
                "errors": errors,
                "instrument_key": instrument_key,
                "candles": [],
            }

        return {
            "status": "success",
            "instrument_key": instrument_key,
            "unit": unit,
            "interval": interval,
            "history_days": history_days,
            "source": "historical+intraday+websocket",
            "live_candle_count": len(live_candles.get(instrument_key, {}).get(interval, {})),
            "errors": errors,
            "candles": candles,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "instrument_key": instrument_key,
            "candles": [],
        }


def meta_from_row(row):
    return {
        "tradingsymbol": str(row.get("tradingsymbol", "")),
        "name": str(row.get("name", "")),
        "expiry": str(row.get("expiry", "")),
        "strike": safe_float(row.get("strike", 0)),
        "last_price": safe_float(row.get("last_price", 0)),
        "option_type": str(row.get("option_type", "")),
        "index_symbol": str(row.get("name", "")),
        "exchange": str(row.get("exchange", "")),
    }


def load_instrument_cache_from_csv():
    csv_path = BASE_DIR / "current_strikes.csv"
    all_keys = [config["index_key"] for config in INDEX_CONFIG.values()]

    if not csv_path.exists():
        print("current_strikes.csv nahi mili. Pehle python get_strikes.py run karo.")
        return all_keys

    try:
        df = pd.read_csv(csv_path)
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        df = df.dropna(subset=["strike", "instrument_key", "tradingsymbol"])

        df["name"] = df["name"].astype(str).str.upper()
        df["exchange"] = df["exchange"].astype(str)
        symbols = df["tradingsymbol"].astype(str).str.upper()

        selected_frames = []
        all_frames = []

        for index_name, config in INDEX_CONFIG.items():
            index_all = df[
                (df["exchange"] == config["exchange"])
                & (df["name"] == config["name"])
            ].copy()

            if index_all.empty:
                starts = symbols.str.startswith(config["name"])
                if index_name == "NIFTY":
                    starts = (
                        starts
                        & ~symbols.str.startswith("BANKNIFTY")
                        & ~symbols.str.startswith("FINNIFTY")
                        & ~symbols.str.startswith("MIDCPNIFTY")
                        & ~symbols.str.startswith("NIFTYNXT50")
                    )
                index_all = df[(df["exchange"] == config["exchange"]) & starts].copy()

            available_expiries[index_name] = get_expiry_list(index_all)
            all_frames.append(index_all)

            if index_all.empty:
                subscribed_expiries[index_name] = []
                print(f"{index_name} options CSV me nahi mile.")
                continue

            spot = get_last_price(config["index_key"])
            step = config.get("step") or infer_strike_step(index_all)
            selected = select_atm_strikes(
                index_all,
                index_name,
                spot,
                step,
                strikes_each_side=STRIKES_EACH_SIDE,
                expiry_count=ACTIVE_EXPIRY_COUNT,
            )
            selected_frames.append(selected)

        selected_df = (
            pd.concat(selected_frames, ignore_index=True)
            if selected_frames
            else pd.DataFrame()
        )
        all_expiry_df = (
            pd.concat(all_frames, ignore_index=True)
            if all_frames
            else pd.DataFrame()
        )

        instrument_meta.clear()
        all_instrument_meta.clear()

        for _, row in all_expiry_df.iterrows():
            all_instrument_meta[str(row["instrument_key"])] = meta_from_row(row)

        for _, row in selected_df.iterrows():
            instrument_meta[str(row["instrument_key"])] = meta_from_row(row)

        option_keys = (
            selected_df["instrument_key"].dropna().astype(str).tolist()
            if not selected_df.empty
            else []
        )
        all_keys.extend(option_keys)

        for index_name in INDEX_CONFIG:
            count = (
                len(selected_df[selected_df["name"].astype(str).str.upper() == index_name])
                if not selected_df.empty and "name" in selected_df.columns
                else 0
            )
            print(f"{index_name} strikes: {count}")
        print(f"CSV cache ready: {len(option_keys)} subscribed strikes, {len(all_instrument_meta)} total instruments.")
        return all_keys
    except Exception as e:
        print(f"CSV cache load error: {e}")
        return all_keys


def fallback_feed_from_meta(meta):
    last_price = safe_float(meta.get("last_price", 0), 0)
    if last_price <= 0:
        return None
    return {
        "fullFeed": {
            "marketFF": {
                "ltpc": {"ltp": last_price, "cp": last_price},
                "optionGreeks": {},
                "marketOHLC": {"ohlc": []},
                "vtt": 0,
                "oi": 0,
                "iv": 0,
            }
        },
        "requestMode": "fallback_csv",
    }


def start_backend():
    refresh_strike_csv_on_startup()
    cached_keys = load_instrument_cache_from_csv()

    fetcher = UpstoxDataFetcher()

    async def main():
        reconnect_delay = 3
        while True:
            if await fetcher.connect():
                reconnect_delay = 3
                await fetcher.subscribe(cached_keys)
                await fetcher.fetch_live_data()
            else:
                option_chain_data["is_live"] = False

            print(f"Reconnect ho raha hai {reconnect_delay} sec me...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)

    asyncio.run(main())


@app.on_event("startup")
def start_background_feed():
    global backend_thread
    if backend_thread and backend_thread.is_alive():
        return
    backend_thread = threading.Thread(target=start_backend, daemon=True)
    backend_thread.start()






