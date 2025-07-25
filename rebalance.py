import csv
import json
import os
import requests
import hashlib
import hmac
import time
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# --- การตั้งค่าหลัก ---
API_HOST = 'https://api.bitkub.com'
MIN_TRADE_AMOUNT = 50  # ลดจาก 50 เพื่อให้เหมาะกับ THB 500
FEE_RATE = 0.0025
BANK = 'KBank'
THRESHOLD = 0.01  # ลดจาก 0.05 เพื่อให้ซื้อขายเกิดง่ายขึ้น
DRY_RUN = False  # เปลี่ยนเป็น False เพื่อให้ซื้อขายจริง

# --- การตั้งค่า Google Sheet ---
SAVE_TO_SHEET = True
SHEET_NAME = "Bitkub Liquidity Data"
PORTFOLIO_WORKSHEET_NAME = "Portfolio"
SUMMARY_WORKSHEET_NAME = "Rebalance"
TRANSACTION_WORKSHEET_NAME = "Transaction"
CREDENTIALS_FILE = "credentials.json"

console = Console()

# --- Core Functions ---
def get_api_credentials():
    load_dotenv()
    api_key = os.environ.get('BITKUB_API_KEY')
    api_secret = os.environ.get('BITKUB_API_SECRET')
    if not api_key or not api_secret: raise Exception("API keys not set.")
    return api_key, api_secret

def gen_sign(api_secret, payload):
    return hmac.new(api_secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

def format_number(number, decimals=2):
    return f"{number:,.{decimals}f}"

def log_transaction(timestamp, currency, action, amount, price, fee, portfolio_value):
    file_exists = os.path.exists('trade_log.csv')
    with open('trade_log.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['Timestamp', 'Currency', 'Action', 'Amount', 'Price', 'Fee', 'Portfolio_Value'])
        writer.writerow([timestamp, currency, action, amount, price, fee, portfolio_value])

def calculate_withdrawal_fee(amount, bank):
    if bank == 'KBank': return 20.0
    if amount <= 100000: return 20.0
    if amount <= 500000: return 75.0
    if amount <= 2000000: return 200.0
    return 300.0  # เพิ่มสำหรับกรณีเกิน 2,000,000

def load_config():
    try:
        with open('config.json', 'r') as f: config = json.load(f)
        target_allocations = config.get('target_allocations', {})
        if not target_allocations: raise Exception("'target_allocations' not found.")
        if abs(sum(target_allocations.values()) - 1.0) > 0.01: raise Exception("Targets must sum to 1.0.")
        if 'THB' not in target_allocations: raise Exception("'THB' must be in targets.")
        coins = [c for c in target_allocations if c != 'THB']
        return [f'THB_{c}' for c in coins], target_allocations
    except FileNotFoundError: raise Exception("'config.json' not found.")
    except json.JSONDecodeError: raise Exception("Invalid 'config.json'.")

# --- Google Sheet Functions ---
def get_gspread_client():
    if not os.path.exists(CREDENTIALS_FILE):
        console.print(f"[bold red]Credentials file not found: '{CREDENTIALS_FILE}'[/bold red]")
        return None
    try:
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        return gspread.authorize(creds)
    except Exception as e:
        console.print(f"[bold red]Failed to authorize Google Sheets: {e}[/bold red]")
        return None

def save_data_to_worksheet(client, data, worksheet_name):
    try:
        spreadsheet = client.open(SHEET_NAME)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1, cols=len(data.columns) if isinstance(data, pd.DataFrame) else len(data))
        
        console.print(f"[yellow]Saving data to worksheet: '{worksheet_name}'...[/yellow]")
        if isinstance(data, pd.DataFrame):
            if not worksheet.get_all_records():
                worksheet.append_rows([data.columns.tolist()] + data.values.tolist(), value_input_option='USER_ENTERED')
            else:
                worksheet.append_rows(data.values.tolist(), value_input_option='USER_ENTERED')
        else:
            if not worksheet.get_all_records():
                worksheet.append_row(list(data.keys()), value_input_option='USER_ENTERED')
            worksheet.append_row(list(data.values()), value_input_option='USER_ENTERED')
        console.print(f"[bold green]Successfully saved data to '{worksheet_name}'[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Error saving to '{worksheet_name}': {e}[/bold red]")

# --- Bitkub API ---
def check_api_status():
    try:
        r = requests.get(f'{API_HOST}/api/status', timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        console.print(f"[bold red]API status check failed: {e}[/bold red]")
        return None

def make_request(api_secret, endpoint, method='POST', body=None):
    API_KEY, _ = get_api_credentials()
    ts = str(int(time.time() * 1000))
    
    # Correctly create the signature payload
    sig_body = json.dumps(body) if body else ''
    payload = f"{ts}{method.upper()}{endpoint}{sig_body}"
    sig = gen_sign(api_secret, payload)

    # The actual request body for POST requests
    req_data = json.dumps(body) if body else None

    headers = {'Accept': 'application/json', 'Content-Type': 'application/json', 'X-BTK-TIMESTAMP': ts, 'X-BTK-SIGN': sig, 'X-BTK-APIKEY': API_KEY}
    try:
        r = requests.request(method, f'{API_HOST}{endpoint}', headers=headers, data=req_data, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get('error') != 0: raise Exception(f"API Error {data.get('error')}")
        return data
    except Exception as e:
        console.print(f"[bold red]API request failed: {e}[/bold red]")
        raise

def get_balances(api_secret, coins):
    try:
        all_assets = coins + ['THB']
        portfolio = {asset: 0.0 for asset in all_assets}
        balances_data = make_request(api_secret, '/api/v3/market/balances').get('result', {})
        for asset, values in balances_data.items():
            if asset in portfolio:
                portfolio[asset] = float(values.get('available', 0.0))
        console.print(f"[green]Successfully fetched balances: {portfolio}[/green]")
        return portfolio
    except Exception as e:
        console.print(f"[bold red]Error fetching balances: {e}. Exiting.[/bold red]")
        raise

def fetch_current_prices(pairs):
    try:
        r = requests.get(f'{API_HOST}/api/v3/market/ticker', timeout=10)
        r.raise_for_status()
        api_data = {item['symbol']: item for item in r.json()}
        prices = {pair: float(api_data[f"{pair.split('_')[1]}_THB"]['last']) for pair in pairs}
        console.print(f"[cyan]Fetched prices: {prices}[/cyan]")
        if not any(prices.values()): raise Exception("All prices are zero.")
        return prices
    except Exception as e:
        console.print(f"[bold red]Error fetching prices: {e}[/bold red]")
        return {pair: 0 for pair in pairs}

def place_order(api_secret, symbol, action, amount):
    endpoint = '/api/v3/market/place-bid' if action == 'buy' else '/api/v3/market/place-ask'
    return make_request(api_secret, endpoint, 'POST', {'sym': str(symbol).lower(), 'amt': amount, 'rat': 0, 'typ': 'market'}).get('result', {})

# --- Data Formatting and Display ---
def calculate_portfolio_value(portfolio, prices):
    return portfolio.get('THB', 0.0) + sum(amount * prices.get(f'THB_{currency}', 0) for currency, amount in portfolio.items() if currency != 'THB')

def create_formatted_portfolio_df(timestamp, portfolio, prices, current_allocations, target_allocations):
    records = []
    for asset, target in sorted(target_allocations.items()):
        amount = portfolio.get(asset, 0.0)
        price = prices.get(f'THB_{asset}', 1) if asset != 'THB' else 1
        value = amount * price
        allocation = current_allocations.get(asset, 0.0)
        records.append({
            "Timestamp": timestamp,
            "Asset": asset,
            "Amount": format_number(amount, 8),
            "Price (THB)": format_number(price),
            "Value (THB)": format_number(value),
            "Allocation": f"{allocation:.2%}",
            "Target": f"{target:.2%}"
        })
    return pd.DataFrame(records)

def display_portfolio(df, total_value):
    timestamp = df["Timestamp"].iloc[0]
    portfolio_table = Table(title=f"📅 Portfolio at {timestamp}", show_header=True, header_style="bold cyan")
    for col in df.columns:
        if col == "Timestamp": continue
        portfolio_table.add_column(col, style="yellow" if col == "Target" else "green")
    for _, row in df.iterrows():
        row_values = [str(row[col]) for col in df.columns if col != "Timestamp"]
        raw_alloc = float(row['Allocation'].strip('%')) / 100
        raw_target = float(row['Target'].strip('%')) / 100
        if abs(raw_alloc - raw_target) > THRESHOLD:
            row_values[4] = f"[bold red]{row['Allocation']}[/bold red]"
        portfolio_table.add_row(*row_values)
    console.print(Panel(portfolio_table, title=f"💰 Current Portfolio Value: {format_number(total_value)} THB", border_style="blue"))

def create_formatted_transactions_df(transactions_raw_list):
    records = []
    for t in transactions_raw_list:
        records.append({
            "Timestamp": t['timestamp'],
            "Currency": t['currency'],
            "Action": t['action'],
            "Amount": format_number(t['amount'], 8),
            "Price (THB)": format_number(t['price']),
            "Fee (THB)": format_number(t['fee'], 4),
            "Portfolio Value (THB)": format_number(t['portfolio_value'])
        })
    return pd.DataFrame(records)

def display_transactions(df):
    if df.empty: 
        console.print("[yellow]No transactions executed.[/yellow]")
        return
    transaction_table = Table(title="📈 Transactions Executed", show_header=True, header_style="bold cyan")
    for col in df.columns:
        transaction_table.add_column(col)
    for _, row in df.iterrows():
        action_style = "green" if row['Action'] == 'Buy' else "red"
        row_values = list(row.values)
        row_values[2] = f"[{action_style}]{row_values[2]}[/{action_style}]"
        transaction_table.add_row(*row_values)
    console.print(transaction_table)

def create_formatted_summary_dict(timestamp, initial_value, final_value, buy_hold_value, fees):
    profit = final_value - initial_value
    roi = (profit / initial_value * 100) if initial_value > 0 else 0
    bh_profit = buy_hold_value - initial_value
    bh_roi = (bh_profit / initial_value * 100) if initial_value > 0 else 0
    return {
        "Date": timestamp,
        "Initial Value": f"{format_number(initial_value)} THB",
        "Final Value": f"{format_number(final_value)} THB",
        "Total Fees": f"{format_number(fees, 4)} THB",
        "Net Profit": f"{format_number(profit)} THB",
        "Net ROI": f"{format_number(roi, 2)}%",
        "Buy & Hold Value": f"{format_number(buy_hold_value)} THB",
        "Buy & Hold Profit": f"{format_number(bh_profit)} THB",
        "Buy & Hold ROI": f"{format_number(bh_roi, 2)}%",
    }

def display_summary(summary_data):
    summary_table = Table(title="📊 Rebalance Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Metric", style="magenta")
    summary_table.add_column("Value", style="green")
    for key, value in summary_data.items():
        color = "white"
        if "Profit" in key or "ROI" in key:
            if "-" not in str(value): color = "green"
            else: color = "red"
        summary_table.add_row(key, f"[{color}]{value}[/{color}]")
    console.print(Panel(summary_table, title="✅ Rebalance Complete", border_style="green"))

# --- Main Rebalance Logic ---
def rebalance():
    console.print(Panel("🚀 Starting Rebalance Bot", style="bold green"))
    gspread_client = get_gspread_client() if SAVE_TO_SHEET else None
    _, api_secret = get_api_credentials()
    PAIRS, TARGET_ALLOCATIONS = load_config()
    coins = [p.split('_')[1] for p in PAIRS]
    
    if check_api_status() is None:
        console.print("[bold red]API is down, exiting...[/bold red]")
        return

    console.print("Fetching initial portfolio state...")
    portfolio = get_balances(api_secret, coins)
    prices = fetch_current_prices(PAIRS)
    if not any(prices.values()):
        console.print("[bold red]Could not fetch prices, exiting...[/bold red]")
        return
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    initial_value = calculate_portfolio_value(portfolio, prices)
    current_allocations = {c: (portfolio.get(c, 0) * prices.get(f'THB_{c}', 0)) / initial_value if initial_value > 0 else 0 for c in TARGET_ALLOCATIONS}
    
    # --- 1. Format, Display, and Save Initial Portfolio ---
    formatted_portfolio_df = create_formatted_portfolio_df(timestamp, portfolio, prices, current_allocations, TARGET_ALLOCATIONS)
    display_portfolio(formatted_portfolio_df, initial_value)
    if gspread_client:
        save_data_to_worksheet(gspread_client, formatted_portfolio_df, PORTFOLIO_WORKSHEET_NAME)

    # --- 2. Execute Transactions ---
    transactions_raw = []
    total_fees = 0.0
    console.print("[yellow]Calculating rebalance trades...[/yellow]")
    target_values = {asset: TARGET_ALLOCATIONS[asset] * initial_value for asset in TARGET_ALLOCATIONS}
    available_thb = portfolio['THB']
    console.print(f"[cyan]Available THB: {format_number(available_thb)}[/cyan]")
    console.print(f"[cyan]Prices: {prices}[/cyan]")
    console.print(f"[cyan]Current Allocations: {current_allocations}[/cyan]")
    console.print(f"[cyan]Target Allocations: {TARGET_ALLOCATIONS}[/cyan]")

    for asset in TARGET_ALLOCATIONS:
        if asset == 'THB': continue
        api_symbol = f'{asset}_THB'
        symbol = f'THB_{asset}'
        current_value = portfolio.get(asset, 0) * prices.get(symbol, 0)
        target_value = target_values[asset]
        diff_value = target_value - current_value
        console.print(f"[cyan]Asset: {asset}, Current: {format_number(current_value)}, Target: {format_number(target_value)}, Diff: {format_number(diff_value)} ({format_number(abs(diff_value)/initial_value*100, 2)}%)[/cyan]")

        if abs(diff_value) / initial_value > THRESHOLD:
            amount_thb = min(abs(diff_value), available_thb / (1 + FEE_RATE))
            if amount_thb < MIN_TRADE_AMOUNT:
                console.print(f"[yellow]Skipping {asset}: Trade amount {format_number(amount_thb)} THB is below minimum {MIN_TRADE_AMOUNT} THB[/yellow]")
                continue

            amount_coin = amount_thb / prices[symbol] if prices[symbol] > 0 else 0
            action = 'buy' if diff_value > 0 else 'sell'
            fee = amount_thb * FEE_RATE
            total_fees += fee

            if action == 'buy' and available_thb < (amount_thb + fee):
                console.print(f"[bold red]Insufficient THB balance for {asset}: Need {format_number(amount_thb + fee)} THB, Available {format_number(available_thb)} THB[/bold red]")
                continue

            try:
                console.print(f"[yellow]Executing {action} order for {asset}: {format_number(amount_coin, 8)} at {format_number(prices[symbol])} THB, Fee: {format_number(fee)} THB[/yellow]")
                if DRY_RUN:
                    console.print(f"[yellow]DRY RUN: Would execute {action} order for {asset}[/yellow]")
                else:
                    result = place_order(api_secret, api_symbol, action, amount_thb if action == 'buy' else amount_coin)
                    console.print(f"[green]Successfully executed {action} order for {asset}: {format_number(amount_coin, 8)} at {format_number(prices[symbol])} THB[/green]")

                # อัพเดท portfolio
                if action == 'buy':
                    portfolio[asset] = portfolio.get(asset, 0) + amount_coin
                    portfolio['THB'] -= (amount_thb + fee)
                    available_thb -= (amount_thb + fee)
                else:
                    portfolio[asset] = portfolio.get(asset, 0) - amount_coin
                    portfolio['THB'] += (amount_thb - fee)
                    available_thb += (amount_thb - fee)

                # บันทึก transaction
                transactions_raw.append({
                    'timestamp': timestamp,
                    'currency': asset,
                    'action': action.capitalize(),
                    'amount': amount_coin,
                    'price': prices[symbol],
                    'fee': fee,
                    'portfolio_value': calculate_portfolio_value(portfolio, prices)
                })
                log_transaction(timestamp, asset, action.capitalize(), amount_coin, prices[symbol], fee, transactions_raw[-1]['portfolio_value'])

                time.sleep(1)  # หน่วงเวลาเพื่อป้องกัน Rate Limit
            except Exception as e:
                console.print(f"[bold red]Failed to execute {action} order for {asset}: {e}[/bold red]")
        else:
            console.print(f"[yellow]Skipping {asset}: Difference {format_number(abs(diff_value)/initial_value*100, 2)}% is below threshold {THRESHOLD*100}%[/yellow]")

    # --- 3. Format, Display, and Save Transactions ---
    formatted_transactions_df = create_formatted_transactions_df(transactions_raw)
    display_transactions(formatted_transactions_df)
    if gspread_client:
        save_data_to_worksheet(gspread_client, formatted_transactions_df, TRANSACTION_WORKSHEET_NAME)

    # --- 4. Format, Display, and Save Summary ---
    final_value = calculate_portfolio_value(portfolio, prices)
    buy_hold_value = calculate_portfolio_value(get_balances(api_secret, coins), prices)
    formatted_summary = create_formatted_summary_dict(timestamp, initial_value, final_value, buy_hold_value, total_fees)
    display_summary(formatted_summary)
    if gspread_client:
        save_data_to_worksheet(gspread_client, formatted_summary, SUMMARY_WORKSHEET_NAME)

def main():
    try:
        rebalance()
    except Exception as e:
        console.print(f"[bold red]❗ An error occurred: {e}[/bold red]")

if __name__ == "__main__":
    main()