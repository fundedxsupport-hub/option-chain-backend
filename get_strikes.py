import requests
import pandas as pd
import gzip
import io


def fetch_and_filter_strikes():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"

    print("Upstox Master List download ho rahi hai...")

    try:
        response = requests.get(url)
        if response.status_code != 200:
            print("Download Failed!", response.status_code)
            return

        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            df = pd.read_csv(f)

        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
        df = df.dropna(subset=["expiry"])

        today = pd.Timestamp.today().normalize()

        df = df[
            (df["exchange"] == "NSE_FO")
            & (df["instrument_type"] == "OPTIDX")
            & (df["expiry"] >= today)
            & (
                df["tradingsymbol"].astype(str).str.startswith("NIFTY")
                | df["tradingsymbol"].astype(str).str.startswith("BANKNIFTY")
            )
            & (~df["tradingsymbol"].astype(str).str.startswith("FINNIFTY"))
            & (~df["tradingsymbol"].astype(str).str.startswith("MIDCPNIFTY"))
        ].copy()

        if df.empty:
            print("NIFTY/BANKNIFTY future expiry strikes nahi mili.")
            return

        nearest_expiry = df["expiry"].min()
        current_expiry_df = df[df["expiry"] == nearest_expiry].copy()

        if current_expiry_df.empty:
            print("Koi strikes nahi mili expiry filter ke baad.")
            return

        current_expiry_df = current_expiry_df.sort_values(
            by=["name", "strike", "option_type"]
        )

        current_expiry_df.to_csv("current_strikes.csv", index=False)

        print(f"Target Expiry Mili: {nearest_expiry.date()}")
        print(f"current_strikes.csv ban gayi. Total {len(current_expiry_df)} strikes.")
        print(current_expiry_df[["tradingsymbol", "strike", "option_type", "instrument_key"]].head(20))

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    fetch_and_filter_strikes()
