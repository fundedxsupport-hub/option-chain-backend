import gzip
import io
from pathlib import Path

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "current_strikes.csv"

MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"


def nearest_expiry_rows(df, name):
    part = df[df["name"].astype(str).str.upper() == name].copy()

    if part.empty:
        print(f"{name} strikes nahi mile.")
        return part

    nearest_expiry = part["expiry"].min()
    result = part[part["expiry"] == nearest_expiry].copy()

    print(f"{name} target expiry: {nearest_expiry}")
    print(f"{name} strikes: {len(result)}")

    return result


def fetch_and_filter_strikes():
    print("Upstox master list download ho rahi hai...")

    try:
        response = requests.get(MASTER_URL, timeout=30)
        response.raise_for_status()

        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            df = pd.read_csv(f)

        required_cols = [
            "instrument_key",
            "tradingsymbol",
            "name",
            "expiry",
            "strike",
            "instrument_type",
            "option_type",
            "exchange",
        ]

        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            print(f"CSV columns missing hain: {missing}")
            return

        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.date
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        df["name"] = df["name"].astype(str).str.upper()

        today = pd.Timestamp.today(tz="Asia/Kolkata").date()

        df = df[
            (df["exchange"] == "NSE_FO")
            & (df["instrument_type"] == "OPTIDX")
            & (df["expiry"] >= today)
            & (df["strike"].notna())
            & (df["instrument_key"].notna())
            & (df["tradingsymbol"].notna())
            & (df["name"].isin(["NIFTY", "BANKNIFTY"]))
            & (~df["tradingsymbol"].astype(str).str.startswith("FINNIFTY"))
            & (~df["tradingsymbol"].astype(str).str.startswith("MIDCPNIFTY"))
        ].copy()

        if df.empty:
            print("Aaj ya future expiry ke NIFTY/BANKNIFTY options nahi mile.")
            return

        nifty_df = nearest_expiry_rows(df, "NIFTY")
        banknifty_df = nearest_expiry_rows(df, "BANKNIFTY")

        current_expiry_df = pd.concat([nifty_df, banknifty_df], ignore_index=True)

        if current_expiry_df.empty:
            print("NIFTY/BANKNIFTY current expiry strikes empty hain.")
            return

        current_expiry_df = current_expiry_df.sort_values(
            by=["name", "expiry", "strike", "option_type"]
        )

        current_expiry_df.to_csv(CSV_PATH, index=False)

        print(f"Today: {today}")
        print(f"current_strikes.csv update ho gayi: {CSV_PATH}")
        print(f"Total strikes: {len(current_expiry_df)}")
        print(
            current_expiry_df[
                [
                    "name",
                    "tradingsymbol",
                    "strike",
                    "option_type",
                    "instrument_key",
                    "expiry",
                ]
            ].head(30)
        )

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    fetch_and_filter_strikes()
