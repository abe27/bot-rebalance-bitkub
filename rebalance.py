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
from rich.text import Text

# การตั้งค่า
API_HOST = 'https://api.bitkub.com'
MIN_TRADE_AMOUNT = 40  # ขั้นต่ำการซื้อขาย 40 THB ตาม Bitkub
FEE_RATE = 0.0025  # ค่าธรรมเนียมการซื้อขาย 0.25%
BANK = 'KBank'  # ธนาคาร: 'KBank' หรือ 'Other'
THRESHOLD = 0.05
INITIAL_THB = 1000.0  # เงินทุนเริ่มต้นสำหรับการคำนวณ Buy-and-Hold

# สร้าง Console สำหรับ Rich
console = Console()

# การตั้งค่า Bitkub API จาก environment variables
API_KEY = os.environ.get('BITKUB_API_KEY')
API_SECRET = os.environ.get('BITKUB_API_SECRET')
if not API_KEY or not API_SECRET:
    raise Exception("Error: BITKUB_API_KEY or BITKUB_API_SECRET not set in environment variables")

def gen_sign(api_secret, payload):
    """สร้าง signature สำหรับ secure endpoints"""
    return hmac.new(api_secret.encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).hexdigest()

def format_number(number, decimals=2):
    """เพิ่ม comma ในตัวเลขและควบคุมทศนิยม"""
    return f"{number:,.{decimals}f}"

def log_transaction(timestamp, currency, action, amount, price, fee, portfolio_value):
    """บันทึกธุรกรรมลง CSV"""
    with open('trade_log.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, currency, action, amount, price, fee, portfolio_value])

def calculate_withdrawal_fee(amount, bank):
    """คำนวณค่าธรรมเนียมการถอน"""
    if bank == 'KBank':
        return 20.0
    else:  # Other banks
        if amount <= 100000:
            return 20.0
        elif amount <= 500000:
            return 75.0
        elif amount <= 2000000:
            return 200.0
        else:
            return None  # ไม่สามารถถอนได้

def load_config():
    """โหลดสัดส่วนเป้าหมายจาก environment variable TARGET_ALLOCATIONS"""
    try:
        target_allocations_str = os.environ.get('TARGET_ALLOCATIONS')
        if not target_allocations_str:
            raise Exception("Error: TARGET_ALLOCATIONS not set in environment variables")
        target_allocations = json.loads(target_allocations_str)
        if not target_allocations:
            raise Exception("Error: No target allocations found in TARGET_ALLOCATIONS")
        if abs(sum(target_allocations.values()) - 1.0) > 0.01:
            raise Exception("Error: Sum of target allocations must be 100%")
        coins = [coin for coin in target_allocations.keys() if coin != 'THB']
        pairs = [f'THB_{coin}' for coin in coins]
        if not all(coin in target_allocations for coin in ['THB'] + coins):
            raise Exception("Error: All coins and THB must have target allocations")
        return pairs, target_allocations
    except json.JSONDecodeError:
        raise Exception("Error: TARGET_ALLOCATIONS must be a valid JSON string")
    except Exception as e:
        raise Exception(f"Error loading target allocations: {e}")

def initialize_portfolio(coins):
    """สร้างพอร์ตเริ่มต้นจากยอดคงเหลือจริงใน Bitkub"""
    portfolio = {'THB': 0.0}
    for coin in coins:
        portfolio[coin] = 0.0
    try:
        ts = str(int(time.time() * 1000))  # Timestamp in milliseconds
        payload = f"{ts}POST/api/v3/market/balances"
        sig = gen_sign(API_SECRET, payload)
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-BTK-TIMESTAMP': ts,
            'X-BTK-SIGN': sig,
            'X-BTK-APIKEY': API_KEY
        }
        response = requests.post(f'{API_HOST}/api/v3/market/balances', headers=headers, json={})
        data = response.json()
        if data.get('error') != 0:
            raise Exception(f"Error fetching balance: {data.get('error')}")
        balances = data['result']
        portfolio['THB'] = balances['THB']['available'] if 'THB' in balances else 0.0
        for coin in coins:
            portfolio[coin] = balances[coin]['available'] if coin in balances else 0.0
        return portfolio
    except Exception as e:
        raise Exception(f"Error fetching balance: {e}")

def fetch_current_prices(pairs):
    """ดึงราคาเหรียญแบบเรียลไทม์จาก Bitkub"""
    prices = {}
    try:
        response = requests.get(f'{API_HOST}/api/v3/market/ticker')
        data = response.json()
        if data.get('error') != 0:
            raise Exception(f"Error fetching ticker: {data.get('error')}")
        for pair in pairs:
            symbol = pair  # Bitkub API ใช้ THB_BTC
            if symbol in data:
                prices[pair] = data[symbol]['last']
            else:
                console.print(f"[red]❗ Price for {pair} not found[/red]")
                prices[pair] = 0
    except Exception as e:
        console.print(f"[red]❗ Error fetching prices: {e}[/red]")
        for pair in pairs:
            prices[pair] = 0
    return prices

def place_order(symbol, action, amount):
    """ส่งคำสั่งซื้อ/ขายแบบ market order"""
    try:
        ts = str(int(time.time() * 1000))
        endpoint = '/api/v3/market/place-bid' if action == 'buy' else '/api/v3/market/place-ask'
        req_body = {'sym': symbol, 'amt': amount, 'typ': 'market'}
        payload = f"{ts}POST{endpoint}{json.dumps(req_body)}"
        sig = gen_sign(API_SECRET, payload)
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-BTK-TIMESTAMP': ts,
            'X-BTK-SIGN': sig,
            'X-BTK-APIKEY': API_KEY
        }
        response = requests.post(f'{API_HOST}{endpoint}', headers=headers, json=req_body)
        data = response.json()
        if data.get('error') != 0:
            raise Exception(f"Error placing {action} order for {symbol}: {data.get('error')}")
        return data['result']
    except Exception as e:
        raise Exception(f"Error placing order: {e}")

def calculate_portfolio_value(portfolio, prices):
    """คำนวณมูลค่าพอร์ต"""
    total_value = portfolio['THB']
    for pair, price in prices.items():
        if pair.startswith('THB_') and pd.notna(price):
            currency = pair.split('_')[1]
            total_value += portfolio[currency] * price
    return total_value

def display_portfolio(timestamp, total_value, current_allocations, portfolio, prices, transactions):
    """แสดงข้อมูลพอร์ตและธุรกรรมในรูปแบบตาราง"""
    portfolio_table = Table(title=f"📅 Portfolio at {timestamp}", show_header=True, header_style="bold cyan")
    portfolio_table.add_column("Asset", style="magenta", width=10)
    portfolio_table.add_column("Amount", style="yellow", width=15)
    portfolio_table.add_column("Price (THB)", style="cyan", width=15)
    portfolio_table.add_column("Buy", style="green", width=15)
    portfolio_table.add_column("Sell", style="red", width=15)
    portfolio_table.add_column("Allocation", style="green", width=12)
    portfolio_table.add_column("Target", style="yellow", width=12)
    for asset, allocation in current_allocations.items():
        target = TARGET_ALLOCATIONS[asset]
        allocation_text = f"{allocation:.2%}"
        if abs(allocation - target) > THRESHOLD:
            allocation_text = f"[red]{allocation_text}[/red]"
        amount = portfolio[asset] if asset != 'THB' else 0
        price = prices.get(f'THB_{asset}', 0) if asset != 'THB' else None
        price_text = f"{format_number(price, 2)}" if price is not None and pd.notna(price) else "-"
        buy_amount = sum(t['amount'] for t in transactions if t['currency'] == asset and t['action'] == 'Buy')
        sell_amount = sum(t['amount'] for t in transactions if t['currency'] == asset and t['action'] == 'Sell')
        buy_text = f"{format_number(buy_amount, 6)}" if buy_amount > 0 else "-"
        sell_text = f"{format_number(sell_amount, 6)}" if sell_amount > 0 else "-"
        portfolio_table.add_row(
            asset,
            f"{format_number(amount, 6)}" if asset != 'THB' else "-",
            price_text,
            buy_text,
            sell_text,
            allocation_text,
            f"{target:.2%}"
        )
    
    console.print(Panel(portfolio_table, title=f"💰 Portfolio Value: {format_number(total_value)} THB", border_style="blue"))
    
    if transactions:
        transaction_table = Table(title="📈 Transactions", show_header=True, header_style="bold cyan")
        transaction_table.add_column("Action", style="magenta")
        transaction_table.add_column("Currency", style="yellow")
        transaction_table.add_column("Amount", style="green")
        transaction_table.add_column("Price (THB)", style="green")
        transaction_table.add_column("Fee (THB)", style="red")
        transaction_table.add_column("Remaining", style="cyan")
        for t in transactions:
            action_style = "green" if t['action'] == 'Buy' else "red"
            transaction_table.add_row(
                f"[{action_style}]{t['action']}[/{action_style}]",
                t['currency'],
                f"{format_number(t['amount'], 6)}",
                f"{format_number(t['price'], 2)}",
                f"{format_number(t['fee'], 2)}",
                f"{format_number(t['remaining'], 6)}"
            )
        console.print(transaction_table)

def rebalance():
    """ฟังก์ชัน Rebalance ซื้อ/ขายจริง"""
    global TARGET_ALLOCATIONS
    console.print(Panel("🚀 Starting Rebalance", style="bold green"))
    
    with open('trade_log.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Currency', 'Action', 'Amount', 'Price', 'Fee', 'Portfolio_Value'])
    
    PAIRS, TARGET_ALLOCATIONS = load_config()
    coins = [pair.split('_')[1] for pair in PAIRS]
    portfolio = initialize_portfolio(coins)
    buy_hold_portfolio = initialize_portfolio(coins)
    total_fees = 0.0

    # ดึงราคาเริ่มต้นสำหรับ Buy-and-Hold
    initial_prices = fetch_current_prices(PAIRS)
    initial_value = calculate_portfolio_value(portfolio, initial_prices)
    
    # คำนวณ Buy-and-Hold (สมมติซื้อครั้งแรกตามสัดส่วนเป้าหมาย)
    for currency, target in TARGET_ALLOCATIONS.items():
        if currency != 'THB' and f'THB_{currency}' in initial_prices:
            amount = (target * INITIAL_THB) / initial_prices[f'THB_{currency}']
            buy_hold_portfolio[currency] = amount
            buy_hold_portfolio['THB'] -= amount * initial_prices[f'THB_{currency}']
    
    # เริ่ม Rebalance
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    prices = fetch_current_prices(PAIRS)
    transactions = []
    
    total_value = calculate_portfolio_value(portfolio, prices)
    current_allocations = {'THB': portfolio['THB'] / total_value if total_value > 0 else 0}
    for pair in PAIRS:
        currency = pair.split('_')[1]
        current_allocations[currency] = (portfolio[currency] * prices.get(pair, 0)) / total_value if pair in prices and total_value > 0 else 0
    
    for currency, target in TARGET_ALLOCATIONS.items():
        current = current_allocations[currency]
        if abs(current - target) > THRESHOLD:
            target_value = total_value * target
            current_value = portfolio[currency] * prices[f'THB_{currency}'] if currency != 'THB' and f'THB_{currency}' in prices else portfolio['THB']
            diff_value = target_value - current_value
            
            if currency != 'THB' and f'THB_{currency}' in prices:
                pair = f'THB_{currency}'
                price = prices[pair]
                amount = abs(diff_value) / price
                trade_value = amount * price
                fee = math.ceil((trade_value * FEE_RATE) * 100) / 100
                
                if trade_value >= MIN_TRADE_AMOUNT:
                    action = 'buy' if diff_value > 0 else 'sell'
                    action_str = 'Buy' if action == 'buy' else 'Sell'
                    
                    try:
                        # ตรวจสอบยอดคงเหลือ
                        if action == 'buy' and portfolio['THB'] < trade_value + fee:
                            console.print(f"[red]❗ Insufficient THB balance for {action_str} {currency}: {portfolio['THB']} < {trade_value + fee}[/red]")
                            continue
                        if action == 'sell' and portfolio[currency] < amount:
                            console.print(f"[red]❗ Insufficient {currency} balance: {portfolio[currency]} < {amount}[/red]")
                            continue
                        
                        # ส่งคำสั่งซื้อ/ขาย
                        order = place_order(pair, action, amount)
                        actual_amount = order.get('amt', amount)
                        actual_price = order.get('rat', price) or price
                        actual_fee = order.get('fee', fee) or fee
                        remaining = portfolio[currency]
                        if action == 'buy':
                            portfolio[currency] += actual_amount
                            portfolio['THB'] -= actual_amount * actual_price + actual_fee
                            remaining += actual_amount
                        else:
                            portfolio[currency] -= actual_amount
                            portfolio['THB'] += actual_amount * actual_price - actual_fee
                            remaining -= actual_amount
                        total_fees += actual_fee
                        
                        log_transaction(timestamp, currency, action_str, actual_amount, actual_price, actual_fee, total_value)
                        transactions.append({
                            'action': action_str,
                            'currency': currency,
                            'amount': actual_amount,
                            'price': actual_price,
                            'fee': actual_fee,
                            'remaining': remaining
                        })
                    except Exception as e:
                        console.print(f"[red]❗ Error executing {action_str} order for {currency}: {e}[/red]")
    
    display_portfolio(timestamp, total_value, current_allocations, portfolio, prices, transactions)
    
    # คำนวณผลลัพธ์
    final_prices = fetch_current_prices(PAIRS)
    final_value = calculate_portfolio_value(portfolio, final_prices)
    buy_hold_final = calculate_portfolio_value(buy_hold_portfolio, final_prices)
    withdrawal_fee = calculate_withdrawal_fee(portfolio['THB'], BANK)
    if withdrawal_fee is None:
        withdrawal_fee = 0
        console.print(f"[red]❗ Warning: Cannot withdraw {format_number(portfolio['THB'])} THB with {BANK}[/red]")
    
    net_value = final_value - withdrawal_fee
    profit = net_value - initial_value
    roi = (profit / initial_value) * 100 if initial_value > 0 else 0
    buy_hold_profit = buy_hold_final - INITIAL_THB
    buy_hold_roi = (buy_hold_profit / INITIAL_THB) * 100 if INITIAL_THB > 0 else 0
    
    # แสดงสรุปผล
    summary_table = Table(title="📊 Rebalance Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Metric", style="magenta")
    summary_table.add_column("Value", style="green")
    summary_table.add_row("Date", f"{timestamp}")
    summary_table.add_row("Initial Value", f"{format_number(initial_value)} THB")
    summary_table.add_row("Rebalance Final Value", f"{format_number(final_value)} THB")
    summary_table.add_row("Total Trading Fees", f"{format_number(total_fees)} THB")
    summary_table.add_row("Withdrawal Fee", f"{format_number(withdrawal_fee)} THB")
    summary_table.add_row("Rebalance Net Value", f"{format_number(net_value)} THB")
    summary_table.add_row("Rebalance Profit", f"[{'green' if profit >= 0 else 'red'}]{format_number(profit)} THB[/{'green' if profit >= 0 else 'red'}]")
    summary_table.add_row("Rebalance ROI", f"[{'green' if roi >= 0 else 'red'}]{format_number(roi, 2)}%[/{'green' if roi >= 0 else 'red'}]")
    summary_table.add_row("Buy-and-Hold Value", f"{format_number(buy_hold_final)} THB")
    summary_table.add_row("Buy-and-Hold Profit", f"[{'green' if buy_hold_profit >= 0 else 'red'}]{format_number(buy_hold_profit)} THB[/{'green' if buy_hold_profit >= 0 else 'red'}]")
    summary_table.add_row("Buy-and-Hold ROI", f"[{'green' if buy_hold_roi >= 0 else 'red'}]{format_number(buy_hold_roi, 2)}%[/{'green' if buy_hold_roi >= 0 else 'red'}]")
    
    console.print(Panel(summary_table, title="✅ Rebalance Complete", border_style="green"))

def main():
    try:
        global INITIAL_PORTFOLIO
        PAIRS, TARGET_ALLOCATIONS = load_config()
        coins = [pair.split('_')[1] for pair in PAIRS]
        INITIAL_PORTFOLIO = initialize_portfolio(coins)
        rebalance()
    except Exception as e:
        console.print(f"[red]❗ Rebalance Error: {e}[/red]")

if __name__ == "__main__":
    main()