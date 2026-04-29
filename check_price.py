import requests

# 1. Aapka token (Maine wahi rakha hai jo aapne bheja)
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIyREFNNDUiLCJqdGkiOiI2OWYxN2Y4ZTE3NGM3MzJiZTcxMGRjYjciLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3NzQzNDUxMCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzc3NTAwMDAwfQ.yO90YrCDDBBaFJOj6NwD5hi-mHqy1dZjEEWVerIr4TQ"

def get_last_price():
    # 2. Dono Indices ki keys (Comma se separate karke)
    nifty_key = "NSE_INDEX|Nifty 50"
    bank_nifty_key = "NSE_INDEX|Nifty Bank"
    
    # URL mein dono symbols bhej rahe hain
    url = f"https://api.upstox.com/v2/market-quote/quotes?symbol={nifty_key},{bank_nifty_key}"
    
    headers = {
        'accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            # Upstox response mein '|' ko ':' kar deta hai
            res_nifty = "NSE_INDEX:Nifty 50"
            res_bank = "NSE_INDEX:Nifty Bank"
            
            print(f"\n✅ --- Market Status: CLOSED (LTP) ---")
            
            # Nifty 50 Check
            if res_nifty in data['data']:
                n_price = data['data'][res_nifty]['last_price']
                print(f"Nifty 50 Last Price: ₹{n_price}")
            
            # Bank Nifty Check
            if res_bank in data['data']:
                b_price = data['data'][res_bank]['last_price']
                print(f"Bank Nifty Last Price: ₹{b_price}")
                
            print(f"--------------------------------------")
            
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"Kuch galat hua: {e}")

if __name__ == "__main__":
    get_last_price()