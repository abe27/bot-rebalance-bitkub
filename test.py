import pandas as pd
import csv
import math
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

# การตั้งค่า
MIN_TRADE_AMOUNT = 40  # ขั้นต่ำการซื้อขาย 40 THB ตาม Bitkub
FEE_RATE = 0.0025  # ค่าธรรมเนียมการซื้อขาย 0.25%
BANK = 'KBank'  # ธนาคาร: 'KBank' หรือ 'Other'
THRESHOLD = 0.05
INITIAL_THB = 100000.0  # เงินทุนเริ่มต้น

# สร้าง Console สำหรับ Rich
console = Console()

def format_number(number, decimals=2):
    """เพิ่ม comma ในตัวเลขและควบคุมทศนิยม"""
    return f"{number:,.{decimals}f}"

def log_transaction(timestamp, currency, action, amount, price, fee, portfolio_value):
    """บันทึกธุรกรรมลง CSV"""
    with open('backtest_log.csv', 'a', newline='') as f:
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

def place_test_order(pair, side, amount, rate):
    """จำลองการซื้อ/ขายโดยไม่เรียก API"""
    return {'error': 0, 'result': {'pair': pair, 'side': side, 'amount': amount, 'rate': rate}}

def load_historical_prices():
    """โหลดข้อมูลราคาย้อนหลังจาก CSV และกำหนด PAIRS และ TARGET_ALLOCATIONS"""
    try:
        df = pd.read_csv('historical_prices.csv')
        if 'timestamp' not in df.columns:
            raise Exception("Error: 'timestamp' column missing in historical_prices.csv")
        if df.empty:
            raise Exception("Error: historical_prices.csv is empty")
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        pairs = [col for col in df.columns if col.startswith('THB_') and col != 'THB']
        if not pairs:
            raise Exception("Error: No valid THB_<COIN> columns found in historical_prices.csv")
        # ใช้ ffill และ bfill สำหรับจัดการ NaN
        df[pairs] = df[pairs].ffill().bfill()
        
        # โหลดสัดส่วนเป้าหมายจาก config.json
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
            target_allocations = config.get('target_allocations', {})
            if not target_allocations:
                raise Exception("Error: No target allocations found in config.json")
            if abs(sum(target_allocations.values()) - 1.0) > 0.01:
                raise Exception("Error: Sum of target allocations must be 100%")
            if not all(pair.split('_')[1] in target_allocations for pair in pairs) or 'THB' not in target_allocations:
                raise Exception("Error: All coins and THB must have target allocations")
        except FileNotFoundError:
            num_coins = len(pairs)
            coin_allocation = (1.0 - 0.4) / num_coins if num_coins > 0 else 0
            target_allocations = {pair.split('_')[1]: coin_allocation for pair in pairs}
            target_allocations['THB'] = 0.4
        
        return df, pairs, target_allocations
    except FileNotFoundError:
        raise Exception("Error: 'historical_prices.csv' not found.")
    except Exception as e:
        raise Exception(f"Error loading historical prices: {e}")

def initialize_portfolio(coins):
    """สร้างพอร์ตเริ่มต้นจากรายชื่อเหรียญ"""
    portfolio = {'THB': INITIAL_THB}
    for coin in coins:
        portfolio[coin] = 0.0
    return portfolio

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

def backtest_rebalance():
    """ฟังก์ชัน Backtesting"""
    global TARGET_ALLOCATIONS
    console.print(Panel("🚀 Starting Rebalance Backtest", style="bold green"))
    
    with open('backtest_log.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Currency', 'Action', 'Amount', 'Price', 'Fee', 'Portfolio_Value'])
    
    prices_df, PAIRS, TARGET_ALLOCATIONS = load_historical_prices()
    coins = [pair.split('_')[1] for pair in PAIRS]
    portfolio = initialize_portfolio(coins)
    buy_hold_portfolio = initialize_portfolio(coins)
    portfolio_values = []
    buy_hold_values = []
    total_fees = 0.0

    for index, row in prices_df.iterrows():
        timestamp = row['timestamp']
        prices = {pair: row[pair] for pair in PAIRS if pair in row and pd.notna(row[pair])}
        transactions = []
        
        # คำนวณ Buy-and-Hold
        if index == 0:
            for currency, target in TARGET_ALLOCATIONS.items():
                if currency != 'THB' and f'THB_{currency}' in prices:
                    amount = (target * INITIAL_THB) / prices[f'THB_{currency}']
                    buy_hold_portfolio[currency] = amount
                    buy_hold_portfolio['THB'] -= amount * prices[f'THB_{currency}']
        buy_hold_value = calculate_portfolio_value(buy_hold_portfolio, prices)
        buy_hold_values.append(buy_hold_value)
        
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
                        action = 'bid' if diff_value > 0 else 'ask'
                        result = place_test_order(pair, action, amount, price)
                        action_str = 'Buy' if action == 'bid' else 'Sell'
                        
                        if result.get('error') == 0:
                            remaining = portfolio[currency]
                            if action == 'bid':
                                portfolio[currency] += amount
                                portfolio['THB'] -= trade_value + fee
                                remaining += amount
                            else:
                                portfolio[currency] -= amount
                                portfolio['THB'] += trade_value - fee
                                remaining -= amount
                            total_fees += fee
                            
                            log_transaction(timestamp, currency, action_str, amount, price, fee, total_value)
                            transactions.append({
                                'action': action_str,
                                'currency': currency,
                                'amount': amount,
                                'price': price,
                                'fee': fee,
                                'remaining': remaining
                            })
        
        display_portfolio(timestamp, total_value, current_allocations, portfolio, prices, transactions)
        portfolio_values.append(total_value)
    
    # คำนวณผลลัพธ์
    initial_prices = {pair: prices_df[pair].iloc[0] for pair in PAIRS if pair in prices_df and pd.notna(prices_df[pair].iloc[0])}
    initial_value = calculate_portfolio_value(INITIAL_PORTFOLIO, initial_prices)
    final_value = portfolio_values[-1]
    buy_hold_final = buy_hold_values[-1]
    withdrawal_fee = calculate_withdrawal_fee(portfolio['THB'], BANK)
    if withdrawal_fee is None:
        withdrawal_fee = 0
        console.print(f"[red]❗ Warning: Cannot withdraw {format_number(portfolio['THB'])} THB with {BANK}[/red]")
    
    net_value = final_value - withdrawal_fee
    profit = net_value - initial_value
    roi = (profit / initial_value) * 100
    buy_hold_profit = buy_hold_final - initial_value
    buy_hold_roi = (buy_hold_profit / initial_value) * 100
    
    # แสดงสรุปผล
    summary_table = Table(title="📊 Backtest Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Metric", style="magenta")
    summary_table.add_column("Value", style="green")
    summary_table.add_row("Last Date", f"{prices_df['timestamp'].iloc[-1]}")
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
    
    console.print(Panel(summary_table, title="✅ Backtest Complete", border_style="green"))

def main():
    try:
        global INITIAL_PORTFOLIO
        prices_df, PAIRS, TARGET_ALLOCATIONS = load_historical_prices()
        coins = [pair.split('_')[1] for pair in PAIRS]
        INITIAL_PORTFOLIO = initialize_portfolio(coins)
        backtest_rebalance()
    except Exception as e:
        console.print(f"[red]❗ Backtest Error: {e}[/red]")

if __name__ == "__main__":
    main()