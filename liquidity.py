import requests
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import os

# --- Configuration ---
SAVE_TO_SHEET = True
SHEET_NAME = "Bitkub Liquidity Data"  # ชื่อไฟล์ Google Sheet หลัก
WORKSHEET_NAME = "Liquidity"         # ชื่อแท็บสำหรับข้อมูล Liquidity
CREDENTIALS_FILE = "credentials.json"
# -------------------

console = Console()

# --- Google Sheet Functions ---
def get_gspread_client():
    if not os.path.exists(CREDENTIALS_FILE):
        console.print(f"[bold red]Credentials file not found: '{CREDENTIALS_FILE}'[/bold red]")
        return None
    try:
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope) # type: ignore
        return gspread.authorize(creds) # type: ignore
    except Exception as e:
        console.print(f"[bold red]Failed to authorize Google Sheets: {e}[/bold red]")
        return None

def save_data_to_worksheet(client, df):
    try:
        spreadsheet = client.open(SHEET_NAME)
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            console.print(f"[yellow]Worksheet '{WORKSHEET_NAME}' not found. Creating it...[/yellow]")
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1, cols=len(df.columns))
        
        console.print(f"[yellow]Saving data to worksheet: '{WORKSHEET_NAME}'...[/yellow]")
        # Data is already formatted, so we can just write it
        if not worksheet.get_all_records():
            worksheet.append_rows([df.columns.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')
        else:
            worksheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
        console.print(f"[bold green]Successfully saved data to '{WORKSHEET_NAME}'[/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error saving to '{WORKSHEET_NAME}': {e}[/bold red]")

# --- Data Fetching and Processing ---

def fetch_bitkub_data():
    """ดึงข้อมูลดิบจาก Bitkub API"""
    url = "https://api.bitkub.com/api/market/ticker"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        console.print(f"[bold red]Error fetching data from Bitkub API: {e}[/bold red]")
        return None

def create_formatted_liquidity_df(data):
    """สร้าง DataFrame ที่ข้อมูลถูกจัดรูปแบบแล้ว สำหรับใช้ทั้งแสดงผลและบันทึก"""
    records = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for symbol, info in data.items():
        if not symbol.startswith("THB_"): continue
        
        last_price = info.get('last', 0)
        spread = info.get('lowestAsk', 0) - info.get('highestBid', 0)
        spread_percentage = (spread / last_price * 100) if last_price != 0 else 0
        volume = info.get('baseVolume', 0)

        records.append({
            "Timestamp": timestamp,
            "Coin": symbol.replace("THB_", ""),
            "Last Price (THB)": f"{last_price:,.2f}",
            "Volume (24h)": f"{volume:,.2f}",
            "Bid-Ask Spread (THB)": f"{spread:.4f}",
            "Spread (%)": f"{spread_percentage:.4f}%",
            "_raw_volume": volume # คอลัมน์ชั่วคราวสำหรับเรียงลำดับ
        })
    
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.sort_values(by="_raw_volume", ascending=False)
    df = df.drop(columns=["_raw_volume"]) # ลบคอลัมน์ชั่วคราวออก
    return df

def display_liquidity_tables(df):
    """แสดงตารางข้อมูลใน Terminal จาก DataFrame ที่จัดรูปแบบแล้ว"""
    if df.empty:
        return

    console.print(Panel("Liquidity Analysis of Coins on Bitkub", style="bold green"))

    # --- Main Table ---
    main_table = Table(show_header=True, header_style="bold magenta")
    for col in df.columns:
        main_table.add_column(col)
    
    for _, row in df.iterrows():
        main_table.add_row(*row.values)
    
    console.print(main_table)

    # --- Top 5 Tables ---
    # Re-calculate numeric values for sorting, as they are now strings
    df_copy = df.copy()
    df_copy['Volume (24h)'] = df_copy['Volume (24h)'].str.replace(",", "").astype(float)
    df_copy['Spread (%)'] = df_copy['Spread (%)'].str.replace("%", "").astype(float)

    # Top 5 Volume
    volume_table = Table(title="Top 5 Coins by Trading Volume", show_header=True, header_style="bold cyan")
    volume_table.add_column("Coin")
    volume_table.add_column("Volume (24h)")
    for _, row in df_copy.nlargest(5, 'Volume (24h)').iterrows():
        volume_table.add_row(row["Coin"], f"{row['Volume (24h)']:,}")
    console.print(volume_table)

    # Top 5 Spread
    spread_table = Table(title="Top 5 Coins by Lowest Spread (%)", show_header=True, header_style="bold green")
    spread_table.add_column("Coin")
    spread_table.add_column("Spread (%)")
    for _, row in df_copy.nsmallest(5, 'Spread (%)').iterrows():
        spread_table.add_row(row["Coin"], f"{row['Spread (%)']:.4f}%")
    console.print(spread_table)


if __name__ == "__main__":
    raw_data = fetch_bitkub_data()
    if raw_data:
        formatted_df = create_formatted_liquidity_df(raw_data)
        
        if not formatted_df.empty:
            display_liquidity_tables(formatted_df)
            
            if SAVE_TO_SHEET:
                gspread_client = get_gspread_client()
                if gspread_client:
                    save_data_to_worksheet(gspread_client, formatted_df)
        else:
            console.print("[yellow]No THB market data found to process.[/yellow]")