import pandas as pd
import csv
import math
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

# การตั้งค่า
API_HOST = 'https://api.bitkub.com'
MIN_TRADE_AMOUNT = 40
FEE_RATE = 0.0025
BANK = 'KBank'
THRESHOLD = 0.05

console = Console()

# --- Core Functions ---

def get_api_credentials():
    """ดึง API Key และ Secret จาก environment variables"""
    load_dotenv()
    api_key = os.environ.get('BITKUB_API_KEY')
    api_secret = os.environ.get('BITKUB_API_SECRET')
    if not api_key or not api_secret:
        raise Exception("Error: BITKUB_API_KEY or BITKUB_API_SECRET not set in environment variables")
    return api_key, api_secret

def gen_sign(api_secret, payload):
    """สร้าง signature สำหรับ secure endpoints"""
    return hmac.new(api_secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

def format_number(number, decimals=2):
    """เพิ่ม comma ในตัวเลขและควบคุมทศนิยม"""
    return f"{number:,.{decimals}f}"

def log_transaction(timestamp, currency, action, amount, price, fee, portfolio_value):
    """บันทึกธุรกรรมลง trade_log.csv"""
    # Create file and write header if it doesn't exist
    if not os.path.exists('trade_log.csv'):
        with open('trade_log.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Timestamp', 'Currency', 'Action', 'Amount', 'Price', 'Fee', 'Portfolio_Value'])
    
    with open('trade_log.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, currency, action, amount, price, fee, portfolio_value])

def calculate_withdrawal_fee(amount, bank):
    """คำนวณค่าธรรมเนียมการถอน"""
    if bank == 'KBank': return 20.0
    else:
        if amount <= 100000: return 20.0
        elif amount <= 500000: return 75.0
        elif amount <= 2000000: return 200.0
        else: return None

def load_config():
    """โหลดสัดส่วนเป้าหมายจาก config.json"""
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        target_allocations = config.get('target_allocations', {})
        if not target_allocations:
            raise Exception("Error: 'target_allocations' not found in config.json")
        if abs(sum(target_allocations.values()) - 1.0) > 0.01:
            raise Exception("Error: Sum of target allocations must be 1.0 (100%)")
        if 'THB' not in target_allocations:
            raise Exception("Error: 'THB' must be in 'target_allocations'")
        
        coins = [coin for coin in target_allocations.keys() if coin != 'THB']
        pairs = [f'THB_{coin}' for coin in coins]
        return pairs, target_allocations
    except FileNotFoundError:
        raise Exception("Error: 'config.json' not found. Please create it from 'config.json.example'.")
    except json.JSONDecodeError:
        raise Exception("Error: Could not decode 'config.json'. Please ensure it is valid JSON.")

# --- Bitkub API Interaction ---

def make_request(api_secret, endpoint, method='POST', body=None):
    """ฟังก์ชันกลางสำหรับสร้างและส่ง request ไปยัง Bitkub API"""
    API_KEY, _ = get_api_credentials()
    ts = str(int(time.time() * 1000))
    
    body_str = json.dumps(body) if body else ''
    payload = f"{ts}{method.upper()}{endpoint}{body_str}"
    sig = gen_sign(api_secret, payload)
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-BTK-TIMESTAMP': ts,
        'X-BTK-SIGN': sig,
        'X-BTK-APIKEY': API_KEY
    }
    try:
        response = requests.request(method, f'{API_HOST}{endpoint}', headers=headers, data=body_str)
        response.raise_for_status()
        data = response.json()
        if data.get('error') != 0:
            raise Exception(f"API Error {data.get('error')}: {data.get('message', 'Unknown error')}")
        return data
    except requests.exceptions.RequestException as e:
        raise Exception(f"Request failed: {e}")

def get_balances(api_secret, coins):
    """ดึงยอดคงเหลือจาก Bitkub"""
    portfolio = {'THB': 0.0}
    for coin in coins:
        portfolio[coin] = 0.0
    
    data = make_request(api_secret, '/api/v3/market/balances')
    balances = data.get('result', {})
    portfolio['THB'] = balances.get('THB', {}).get('available', 0.0)
    for coin in coins:
        portfolio[coin] = balances.get(coin, {}).get('available', 0.0)
    return portfolio

def fetch_current_prices(pairs):
    """ดึงราคาล่าสุดจาก Bitkub. API format is COIN_THB."""
    prices = {}
    try:
        response = requests.get(f'{API_HOST}/api/v3/market/ticker')
        response.raise_for_status()
        # The API returns a list of dicts. Convert it to a dict keyed by symbol for efficient lookup.
        api_data = {item['symbol']: item for item in response.json()}

        for pair in pairs: # pair is in THB_COIN format, e.g., 'THB_XRP'
            # Convert internal format to API format for lookup
            coin = pair.split('_')[1]
            ticker_symbol = f"{coin}_THB" # e.g., 'XRP_THB'
            
            if ticker_symbol in api_data:
                prices[pair] = float(api_data[ticker_symbol]['last'])
            else:
                console.print(f"[yellow]Warning: Price for {pair} (lookup as {ticker_symbol}) not found in API response.[/yellow]")
                prices[pair] = 0
    except requests.exceptions.RequestException as e:
        console.print(f"[red]❗ Error fetching prices: {e}[/red]")
        for pair in pairs:
            prices[pair] = 0
    return prices

def place_order(api_secret, symbol, action, amount):
    """ส่งคำสั่งซื้อ/ขาย (Market Order). Symbol must be in COIN_THB format. Amount must be a float.
    The amount will be formatted to string with no trailing zeros before sending to API.
    """
    endpoint = '/api/v3/market/place-bid' if action == 'buy' else '/api/v3/market/place-ask'

    # Format amount to string with no trailing zeros
    req_body = {'sym': symbol, 'amt': round(amount, 2), 'rat': 0,'typ': 'market'}
    data = make_request(api_secret, endpoint, method='POST', body=req_body)
    return data.get('result', {})

# --- Portfolio & Display ---

def calculate_portfolio_value(portfolio, prices):
    """คำนวณมูลค่าพอร์ตรวม"""
    total_value = portfolio.get('THB', 0.0)
    for currency, amount in portfolio.items():
        if currency != 'THB':
            price = prices.get(f'THB_{currency}', 0) # CORRECT: Use internal format
            total_value += amount * price
    return total_value

def display_portfolio(timestamp, total_value, current_allocations, portfolio, prices, target_allocations):
    """แสดงข้อมูลพอร์ตในรูปแบบตาราง"""
    portfolio_table = Table(title=f"📅 Portfolio at {timestamp}", show_header=True, header_style="bold cyan")
    portfolio_table.add_column("Asset", style="magenta", width=10)
    portfolio_table.add_column("Amount", style="yellow", width=15)
    portfolio_table.add_column("Price (THB)", style="cyan", width=15)
    portfolio_table.add_column("Value (THB)", style="green", width=15)
    portfolio_table.add_column("Allocation", style="green", width=12)
    portfolio_table.add_column("Target", style="yellow", width=12)

    for asset, target in target_allocations.items():
        amount = portfolio.get(asset, 0.0)
        price = prices.get(f'THB_{asset}') if asset != 'THB' else 1 # CORRECT: Use internal format
        value = amount * price if price is not None else 0
        allocation = current_allocations.get(asset, 0.0)

        allocation_text = f"{allocation:.2%}"
        if abs(allocation - target) > THRESHOLD:
            allocation_text = f"[bold red]{allocation_text}[/bold red]"
        
        price_text = f"{format_number(price, 2)}" if price is not None and asset != 'THB' else "1.00"

        portfolio_table.add_row(
            asset,
            f"{format_number(amount, 6)}",
            price_text,
            f"{format_number(value, 2)}",
            allocation_text,
            f"{target:.2%}"
        )
    
    console.print(Panel(portfolio_table, title=f"💰 Current Portfolio Value: {format_number(total_value)} THB", border_style="blue"))

def display_transactions(transactions):
    """แสดงข้อมูลธุรกรรมในรูปแบบตาราง"""
    if not transactions: return

    transaction_table = Table(title="📈 Transactions Executed", show_header=True, header_style="bold cyan")
    transaction_table.add_column("Action", style="magenta")
    transaction_table.add_column("Currency", style="yellow")
    transaction_table.add_column("Amount", style="green")
    transaction_table.add_column("Value (THB)", style="green")
    transaction_table.add_column("Fee (THB)", style="red")

    for t in transactions:
        action_style = "green" if t['action'] == 'Buy' else "red"
        transaction_table.add_row(
            f"[{action_style}]{t['action']}[/{action_style}]",
            t['currency'],
            f"{format_number(t['amount'], 6)}",
            f"{format_number(t['value'], 2)}",
            f"{format_number(t['fee'], 4)}"
        )
    console.print(transaction_table)

# --- Main Rebalance Logic ---

def rebalance():
    """ฟังก์ชัน Rebalance ซื้อ/ขายจริง"""
    console.print(Panel("🚀 Starting Rebalance Bot", style="bold green"))
    
    _, api_secret = get_api_credentials()
    PAIRS, TARGET_ALLOCATIONS = load_config()
    coins = [p.split('_')[1] for p in PAIRS]
    
    # 1. Get Initial State
    console.print("Fetching initial portfolio state...")
    portfolio = get_balances(api_secret, coins + ['THB'])
    initial_prices = fetch_current_prices(PAIRS)
    if not initial_prices or all(v == 0 for v in initial_prices.values()):
        raise Exception("Could not fetch any valid prices from API. Aborting.")
    initial_value = calculate_portfolio_value(portfolio, initial_prices)
    console.print(f"[green]Initial portfolio value: {format_number(initial_value)} THB[/green]")

    # 2. Setup Buy-and-Hold for comparison
    buy_hold_portfolio = portfolio.copy()

    # 3. Start Rebalance Check
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    prices = fetch_current_prices(PAIRS)
    if not prices or all(v == 0 for v in prices.values()):
        raise Exception("Could not fetch current prices for rebalancing. Aborting.")

    total_value = calculate_portfolio_value(portfolio, prices)
    current_allocations = {c: (portfolio.get(c, 0) * prices.get(f'THB_{c}', 0)) / total_value if total_value > 0 else 0 for c in TARGET_ALLOCATIONS}
    current_allocations['THB'] = portfolio['THB'] / total_value if total_value > 0 else 0

    display_portfolio(timestamp, total_value, current_allocations, portfolio, prices, TARGET_ALLOCATIONS)

    transactions = []
    # 4. Execute Trades if needed
    for currency, target in TARGET_ALLOCATIONS.items():
        if currency == 'THB': continue

        current_alloc = current_allocations.get(currency, 0.0)
        if abs(current_alloc - target) > THRESHOLD:
            internal_pair = f'THB_{currency}' # Internal format for price lookup
            api_symbol = f'{currency}_THB'.lower()   # API format for placing orders, must be lowercase

            price = prices.get(internal_pair, 0)
            if price == 0: 
                console.print(f"[yellow]Skipping {currency} due to missing price.[/yellow]")
                continue

            current_value = portfolio[currency] * price
            target_value = total_value * target
            diff_value = target_value - current_value
            
            action = 'buy' if diff_value > 0 else 'sell'

            try:
                if action == 'buy':
                    amount_in_thb = diff_value
                    if amount_in_thb < MIN_TRADE_AMOUNT: continue
                    # Format amount to 2 decimal places for THB
                    formatted_amount = round(amount_in_thb, 2)
                    console.print(f"Attempting to [green]BUY[/green] {formatted_amount} THB of {currency} using symbol {api_symbol}")
                    order = place_order(api_secret, api_symbol, 'buy', formatted_amount)
                    actual_amount_crypto = order.get('amt', 0)
                    actual_fee = order.get('fee', 0)
                    log_transaction(timestamp, currency, 'Buy', actual_amount_crypto, price, actual_fee, total_value)
                    transactions.append({'action': 'Buy', 'currency': currency, 'amount': actual_amount_crypto, 'value': formatted_amount, 'fee': actual_fee})
                
                else: # Sell
                    amount_to_sell_crypto = abs(diff_value) / price
                    value_in_thb = amount_to_sell_crypto * price
                    if value_in_thb < MIN_TRADE_AMOUNT: continue
                    # Format amount to 8 decimal places for crypto
                    formatted_amount = round(amount_to_sell_crypto, 8)
                    console.print(f"Attempting to [red]SELL[/red] {formatted_amount} {currency} using symbol {api_symbol}")
                    order = place_order(api_secret, api_symbol, 'sell', formatted_amount)
                    actual_fee = order.get('fee', 0)
                    log_transaction(timestamp, currency, 'Sell', formatted_amount, price, actual_fee, total_value)
                    transactions.append({'action': 'Sell', 'currency': currency, 'amount': formatted_amount, 'value': value_in_thb, 'fee': actual_fee})

            except Exception as e:
                console.print(f"[red]❗ Error executing {action} order for {currency}: {e}[/red]")

    # 5. Display Results
    display_transactions(transactions)

    console.print("\nFetching final portfolio state...")
    final_portfolio = get_balances(api_secret, coins + ['THB'])
    final_prices = fetch_current_prices(PAIRS)
    final_value = calculate_portfolio_value(final_portfolio, final_prices)
    buy_hold_final = calculate_portfolio_value(buy_hold_portfolio, final_prices)
    
    total_fees = sum(t['fee'] for t in transactions)
    withdrawal_fee = calculate_withdrawal_fee(final_portfolio.get('THB', 0), BANK) or 0

    net_value = final_value - withdrawal_fee
    profit = net_value - initial_value
    roi = (profit / initial_value) * 100 if initial_value > 0 else 0
    buy_hold_profit = buy_hold_final - initial_value
    buy_hold_roi = (buy_hold_profit / initial_value) * 100 if initial_value > 0 else 0

    summary_table = Table(title="📊 Rebalance Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Metric", style="magenta")
    summary_table.add_column("Value", style="green")
    summary_table.add_row("Date", f"{timestamp}")
    summary_table.add_row("Initial Value", f"{format_number(initial_value)} THB")
    summary_table.add_row("Rebalance Final Value", f"{format_number(final_value)} THB")
    summary_table.add_row("Total Trading Fees", f"{format_number(total_fees, 4)} THB")
    summary_table.add_row("Withdrawal Fee", f"{format_number(withdrawal_fee)} THB")
    summary_table.add_row("Rebalance Net Value", f"{format_number(net_value)} THB")
    summary_table.add_row("Rebalance Profit", f"[{ 'green' if profit >= 0 else 'red'}]{format_number(profit)} THB[/{ 'green' if profit >= 0 else 'red'}]")
    summary_table.add_row("Rebalance ROI", f"[{ 'green' if roi >= 0 else 'red'}]{format_number(roi, 2)}%[/{ 'green' if roi >= 0 else 'red'}]")
    summary_table.add_row("Buy-and-Hold Final Value", f"{format_number(buy_hold_final)} THB")
    summary_table.add_row("Buy-and-Hold Profit", f"[{ 'green' if buy_hold_profit >= 0 else 'red'}]{format_number(buy_hold_profit)} THB[/{ 'green' if buy_hold_profit >= 0 else 'red'}]")
    summary_table.add_row("Buy-and-Hold ROI", f"[{ 'green' if buy_hold_roi >= 0 else 'red'}]{format_number(buy_hold_roi, 2)}%[/{ 'green' if buy_hold_roi >= 0 else 'red'}]")
    
    console.print(Panel(summary_table, title="✅ Rebalance Complete", border_style="green"))

def main():
    try:
        rebalance()
    except Exception as e:
        console.print(f"[bold red]❗ An error occurred: {e}[/bold red]")

if __name__ == "__main__":
    main()