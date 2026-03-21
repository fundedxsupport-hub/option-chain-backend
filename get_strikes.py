import requests
import pandas as pd
import gzip
import io

def fetch_and_filter_strikes():
    # 1. Upstox Master File URL
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    
    print("⏳ Step 2: Upstox Master List download ho rahi hai...")
    try:
        response = requests.get(url)
        if response.status_code != 200:
            print("❌ Download Failed!")
            return

        # Gzip file ko kholna
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            df = pd.read_csv(f)

        # 2. Sirf Nifty aur Bank Nifty ke Options (OPTIDX) filter karein
        print(df.columns)
        df = df[df['tradingsymbol'].str.contains('NIFTY|BANKNIFTY', na=False)]
        df = df[df['instrument_type'] == 'OPTIDX']

        # ✅ FIX: expiry clean karo
        df['expiry'] = pd.to_datetime(df['expiry'], errors='coerce')
        df = df.dropna(subset=['expiry'])   # 👈 important

        if df.empty:
            print("❌ Expiry data empty hai!")
            return

        # 3. Sabse pass wali Expiry dhoondna (Weekly)
        nearest_expiry = df['expiry'].min()
        print(f"🎯 Target Expiry Mili: {nearest_expiry.date()}")

        # 4. Sirf usi expiry ka data rakhein
        current_expiry_df = df[df['expiry'] == nearest_expiry]

        if current_expiry_df.empty:
            print("❌ Koi strikes nahi mili (expiry filter ke baad)")
            return

        # 5. Ek CSV file mein save kar lo taaki WebSocket ise use kar sake
        current_expiry_df.to_csv("current_strikes.csv", index=False)
        
        print(f"✅ 'current_strikes.csv' ban gayi hai! Total {len(current_expiry_df)} strikes mili hain.")
        
        # Sample dikhane ke liye
        print("\n--- Sample Strikes ---")

        # ✅ FIX: strike_price → strike
        print(current_expiry_df[['strike', 'option_type', 'instrument_key']].head(5))

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    fetch_and_filter_strikes()