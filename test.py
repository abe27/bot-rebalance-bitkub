
import pandas as pd
import csv
import math
import json
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# การตั้งค่า
MIN_TRADE_AMOUNT = 40
FEE_RATE = 0.0025
BANK = 'KBank'
THRESHOLD = 0.05
INITIAL_THB = 500.0

console = Console()

def format_number(number, decimals=2):
    return f"{number:,.{decimals}f}"

def log_transaction(timestamp, currency, action, amount, price, fee, portfolio_value):
    with open('backtest_log.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, currency, action, amount, price, fee, portfolio_value])

def calculate_withdrawal_fee(amount, bank):
    if bank == 'KBank':
        return 20.0
    else:
        if amount <= 100000: return 20.0
        elif amount <= 500000: return 75.0
        elif amount <= 2000000: return 200.0
        else: return None

def place_test_order(pair, side, amount, rate):
    return {'error': 0, 'result': {'pair': pair, 'side': side, 'amount': amount, 'rate': rate}}

def load_historical_prices():
    """โหลดข้อมูลราคาย้อนหลังจาก CSV และกำหนด PAIRS และ TARGET_ALLOCATIONS จาก config.json"""
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
        df[pairs] = df[pairs].ffill().bfill()
        
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
            target_allocations = config.get('target_allocations', {})
            if not target_allocations:
                raise Exception("Error: 'target_allocations' not found in config.json")
            if abs(sum(target_allocations.values()) - 1.0) > 0.01:
                raise Exception("Error: Sum of target allocations must be 1.0 (100%)")
            
            coins_from_config = [coin for coin in target_allocations.keys() if coin != 'THB']
            coins_from_csv = [p.split('_')[1] for p in pairs]
            
            if not all(coin in coins_from_csv for coin in coins_from_config):
                raise Exception("Error: Coins in 'target_allocations' must exist in 'historical_prices.csv'")
            if 'THB' not in target_allocations:
                 raise Exception("Error: 'THB' must be in 'target_allocations'")

        except FileNotFoundError:
            raise Exception("Error: 'config.json' not found. Please create it from 'config.json.example'.")
        except json.JSONDecodeError:
            raise Exception("Error: Could not decode 'config.json'. Please ensure it is valid JSON.")

        return df, pairs, target_allocations
    except FileNotFoundError:
        raise Exception("Error: 'historical_prices.csv' not found. Please run 'fetch_data.py' first.")
    except Exception as e:
        raise Exception(f"Error loading data: {e}")

def initialize_portfolio(coins):
    portfolio = {'THB': INITIAL_THB}
    for coin in coins:
        portfolio[coin] = 0.0
    return portfolio

def calculate_portfolio_value(portfolio, prices):
    total_value = portfolio['THB']
    for pair, price in prices.items():
        if pair.startswith('THB_') and pd.notna(price):
            currency = pair.split('_')[1]
            if currency in portfolio:
                total_value += portfolio[currency] * price
    return total_value

def display_portfolio(timestamp, total_value, current_allocations, portfolio, prices, transactions, target_allocations):
    portfolio_table = Table(title=f"📅 Portfolio at {timestamp}", show_header=True, header_style="bold cyan")
    portfolio_table.add_column("Asset", style="magenta", width=10)
    portfolio_table.add_column("Amount", style="yellow", width=15)
    portfolio_table.add_column("Price (THB)", style="cyan", width=15)
    portfolio_table.add_column("Value (THB)", style="green", width=15)
    portfolio_table.add_column("Allocation", style="green", width=12)
    portfolio_table.add_column("Target", style="yellow", width=12)
    
    for asset, allocation in current_allocations.items():
        target = target_allocations[asset]
        allocation_text = f"{allocation:.2%}"
        if abs(allocation - target) > THRESHOLD:
            allocation_text = f"[bold red]{allocation_text}[/bold red]"
        
        amount = portfolio.get(asset, 0.0)
        price = prices.get(f'THB_{asset}') if asset != 'THB' else 1
        value = amount * price if price is not None else 0
        
        price_text = f"{format_number(price, 2)}" if price is not None and pd.notna(price) and asset != 'THB' else "1.00"
        
        portfolio_table.add_row(
            asset,
            f"{format_number(amount, 6)}",
            price_text,
            f"{format_number(value, 2)}",
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
        
        for t in transactions:
            action_style = "green" if t['action'] == 'Buy' else "red"
            transaction_table.add_row(
                f"[{action_style}]{t['action']}[/{action_style}]",
                t['currency'],
                f"{format_number(t['amount'], 6)}",
                f"{format_number(t['price'], 2)}",
                f"{format_number(t['fee'], 4)}"
            )
        console.print(transaction_table)

def backtest_rebalance():
    """ฟังก์ชัน Backtesting ที่ปรับปรุงใหม่"""
    console.print(Panel("🚀 Starting Improved Rebalance Backtest", style="bold green"))
    
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

    # --- 1. Initial Investment on Day 1 ---
    first_day = prices_df.iloc[0]
    timestamp = first_day['timestamp']
    prices = {pair: first_day[pair] for pair in PAIRS if pair in first_day and pd.notna(first_day[pair])}
    
    # Create Buy & Hold portfolio
    for currency, target in TARGET_ALLOCATIONS.items():
        if currency != 'THB' and f'THB_{currency}' in prices:
            amount = (target * INITIAL_THB) / prices[f'THB_{currency}']
            buy_hold_portfolio[currency] = amount
            buy_hold_portfolio['THB'] -= amount * prices[f'THB_{currency}']

    # Create Rebalance portfolio (with fees)
    transactions = []
    initial_value = calculate_portfolio_value(portfolio, prices)
    for currency, target in TARGET_ALLOCATIONS.items():
        if currency != 'THB' and f'THB_{currency}' in prices:
            pair = f'THB_{currency}'
            price = prices[pair]
            target_value = initial_value * target
            amount_to_buy = target_value / price
            trade_value = amount_to_buy * price
            fee = math.ceil((trade_value * FEE_RATE) * 100) / 100
            
            if portfolio['THB'] >= trade_value + fee:
                portfolio[currency] += amount_to_buy
                portfolio['THB'] -= (trade_value + fee)
                total_fees += fee
                log_transaction(timestamp, currency, 'Buy', amount_to_buy, price, fee, initial_value)
                transactions.append({
                    'action': 'Buy', 'currency': currency, 'amount': amount_to_buy,
                    'price': price, 'fee': fee
                })

    # Display initial state
    day1_value = calculate_portfolio_value(portfolio, prices)
    day1_allocations = {c: (portfolio.get(c, 0) * prices.get(f'THB_{c}', 0)) / day1_value if day1_value > 0 else 0 for c in TARGET_ALLOCATIONS}
    day1_allocations['THB'] = portfolio['THB'] / day1_value if day1_value > 0 else 0
    console.print(Panel(f"📈 Initial investment on {timestamp}", style="bold yellow"))
    display_portfolio(timestamp, day1_value, day1_allocations, portfolio, prices, transactions, TARGET_ALLOCATIONS)
    
    portfolio_values.append(day1_value)
    buy_hold_values.append(calculate_portfolio_value(buy_hold_portfolio, prices))

    # --- 2. Loop through remaining days for rebalancing ---
    for index, row in prices_df.iloc[1:].iterrows():
        timestamp = row['timestamp']
        prices = {pair: row[pair] for pair in PAIRS if pair in row and pd.notna(row[pair])}
        transactions = []
        
        total_value = calculate_portfolio_value(portfolio, prices)
        current_allocations = {c: (portfolio.get(c, 0) * prices.get(f'THB_{c}', 0)) / total_value if total_value > 0 else 0 for c in TARGET_ALLOCATIONS}
        current_allocations['THB'] = portfolio['THB'] / total_value if total_value > 0 else 0

        for currency, target in TARGET_ALLOCATIONS.items():
            if currency == 'THB': continue

            current = current_allocations.get(currency, 0.0)
            if abs(current - target) > THRESHOLD:
                target_value = total_value * target
                current_value = portfolio[currency] * prices.get(f'THB_{currency}', 0)
                diff_value = target_value - current_value
                
                pair = f'THB_{currency}'
                price = prices[pair]
                amount = abs(diff_value) / price
                trade_value = amount * price
                
                if trade_value >= MIN_TRADE_AMOUNT:
                    fee = math.ceil((trade_value * FEE_RATE) * 100) / 100
                    action = 'Buy' if diff_value > 0 else 'Sell'
                    
                    if action == 'Buy' and portfolio['THB'] >= trade_value + fee:
                        portfolio[currency] += amount
                        portfolio['THB'] -= (trade_value + fee)
                        total_fees += fee
                        log_transaction(timestamp, currency, action, amount, price, fee, total_value)
                        transactions.append({'action': action, 'currency': currency, 'amount': amount, 'price': price, 'fee': fee})
                    elif action == 'Sell' and portfolio[currency] >= amount:
                        portfolio[currency] -= amount
                        portfolio['THB'] += (trade_value - fee)
                        total_fees += fee
                        log_transaction(timestamp, currency, action, amount, price, fee, total_value)
                        transactions.append({'action': action, 'currency': currency, 'amount': amount, 'price': price, 'fee': fee})

        # After all potential trades for the day, calculate final daily value
        final_day_value = calculate_portfolio_value(portfolio, prices)
        portfolio_values.append(final_day_value)
        buy_hold_values.append(calculate_portfolio_value(buy_hold_portfolio, prices))
        
        if transactions:
            final_allocations = {c: (portfolio.get(c, 0) * prices.get(f'THB_{c}', 0)) / final_day_value if final_day_value > 0 else 0 for c in TARGET_ALLOCATIONS}
            final_allocations['THB'] = portfolio['THB'] / final_day_value if final_day_value > 0 else 0
            display_portfolio(timestamp, final_day_value, final_allocations, portfolio, prices, transactions, TARGET_ALLOCATIONS)

    # --- 3. Final Results ---
    final_value = portfolio_values[-1]
    buy_hold_final = buy_hold_values[-1]
    withdrawal_fee = calculate_withdrawal_fee(portfolio['THB'], BANK)
    if withdrawal_fee is None:
        withdrawal_fee = 0
        console.print(f"[red]❗ Warning: Cannot withdraw {format_number(portfolio['THB'])} THB with {BANK}[/red]")
    
    net_value = final_value - withdrawal_fee
    profit = net_value - INITIAL_THB
    roi = (profit / INITIAL_THB) * 100
    buy_hold_profit = buy_hold_final - INITIAL_THB
    buy_hold_roi = (buy_hold_profit / INITIAL_THB) * 100
    
    summary_table = Table(title="📊 Backtest Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Metric", style="magenta")
    summary_table.add_column("Value", style="green")
    summary_table.add_row("Date Range", f"{prices_df['timestamp'].iloc[0].date()} to {prices_df['timestamp'].iloc[-1].date()}")
    summary_table.add_row("Initial Value", f"{format_number(INITIAL_THB)} THB")
    summary_table.add_row("Rebalance Final Value", f"{format_number(final_value)} THB")
    summary_table.add_row("Total Trading Fees", f"{format_number(total_fees)} THB")
    summary_table.add_row("Withdrawal Fee", f"{format_number(withdrawal_fee)} THB")
    summary_table.add_row("Rebalance Net Value", f"{format_number(net_value)} THB")
    summary_table.add_row("Rebalance Profit", f"[{ 'green' if profit >= 0 else 'red'}]{format_number(profit)} THB[/{ 'green' if profit >= 0 else 'red'}]")
    summary_table.add_row("Rebalance ROI", f"[{ 'green' if roi >= 0 else 'red'}]{format_number(roi, 2)}%[/{ 'green' if roi >= 0 else 'red'}]")
    summary_table.add_row("Buy-and-Hold Final Value", f"{format_number(buy_hold_final)} THB")
    summary_table.add_row("Buy-and-Hold Profit", f"[{ 'green' if buy_hold_profit >= 0 else 'red'}]{format_number(buy_hold_profit)} THB[/{ 'green' if buy_hold_profit >= 0 else 'red'}]")
    summary_table.add_row("Buy-and-Hold ROI", f"[{ 'green' if buy_hold_roi >= 0 else 'red'}]{format_number(buy_hold_roi, 2)}%[/{ 'green' if buy_hold_roi >= 0 else 'red'}]")
    
    console.print(Panel(summary_table, title="✅ Backtest Complete", border_style="green"))

def main():
    try:
        backtest_rebalance()
    except Exception as e:
        console.print(f"[bold red]❗ Backtest Error: {e}[/bold red]")

if __name__ == "__main__":
    main()
