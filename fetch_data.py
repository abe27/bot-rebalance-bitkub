import requests
import pandas as pd
from datetime import datetime, timedelta

# การตั้งค่า
API_HOST = 'https://api.bitkub.com'
PAIRS = ['THB_XRP','THB_SAND']  # คู่เหรียญที่ต้องการดึงข้อมูล
RESOLUTION = 'D'  # ความละเอียดของข้อมูล: 'D' = รายวัน, '60' = รายชั่วโมง
DAYS_OF_DATA = 365  # จำนวนวันที่ต้องการข้อมูลย้อนหลัง

def fetch_historical_data(symbol, resolution, from_timestamp, to_timestamp):
    """
    ดึงข้อมูลราคาย้อนหลัง (K-Line) จาก Bitkub TradingView API
    """
    print(f"Fetching data for {symbol}...")
    try:
        url = f"{API_HOST}/tradingview/history"
        params = {
            'symbol': symbol,
            'resolution': resolution,
            'from': from_timestamp,
            'to': to_timestamp
        }
        response = requests.get(url, params=params)
        response.raise_for_status()  # ตรวจสอบว่า request สำเร็จหรือไม่
        data = response.json()

        if data.get('s') != 'ok':
            # Print the full error response for better debugging
            print(f"Error fetching data for {symbol}. Full response: {data}")
            return pd.DataFrame()

        # แปลงข้อมูลเป็น DataFrame
        df = pd.DataFrame({
            'timestamp': pd.to_datetime(data['t'], unit='s'),
            symbol: data['c']  # ใช้ราคาปิด (close price)
        })
        df = df.set_index('timestamp')
        return df
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data for {symbol}: {e}")
        return pd.DataFrame()

def main():
    """
    ฟังก์ชันหลักสำหรับดึงข้อมูลและบันทึกลงไฟล์ CSV
    """
    # คำนวณช่วงเวลา
    to_time = datetime.now()
    from_time = to_time - timedelta(days=DAYS_OF_DATA)
    to_timestamp = int(to_time.timestamp())
    from_timestamp = int(from_time.timestamp())

    # ดึงข้อมูลของแต่ละคู่เหรียญ
    all_data_frames = []
    for pair in PAIRS:
        # The TradingView API expects symbols like 'btc_thb', not 'THB_BTC'.
        try:
            base_currency, quote_currency = pair.split('_')
            tv_symbol = f"{quote_currency.lower()}_{base_currency.lower()}"
            
            df = fetch_historical_data(tv_symbol, RESOLUTION, from_timestamp, to_timestamp)
            
            if not df.empty:
                # Rename column back to the standard format (e.g., 'THB_BTC')
                df = df.rename(columns={tv_symbol: pair})
                all_data_frames.append(df)
        except Exception as e:
            print(f"An error occurred while processing pair {pair}: {e}")

    # Check if we successfully fetched any data
    if not all_data_frames:
        print("\nError: Failed to fetch any historical data. CSV file not created. Exiting.")
        return

    # รวม DataFrame ทั้งหมดเข้าด้วยกัน
    # Use outer join to keep all timestamps and forward-fill/back-fill missing values
    combined_df = pd.concat(all_data_frames, axis=1, join='outer').ffill().bfill()
    combined_df = combined_df.reset_index()
    combined_df = combined_df.rename(columns={'index': 'timestamp'})
    combined_df['timestamp'] = combined_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # บันทึกเป็นไฟล์ CSV
    combined_df.to_csv('historical_prices.csv', index=False)
    print("\nSuccessfully saved historical data to historical_prices.csv")

if __name__ == "__main__":
    main()