import os
import pandas as pd
from dotenv import load_dotenv
from alpaca.data.live import StockDataStream
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from liv_arb_math import LiveArbitrageMath

#Initializing the keys
load_dotenv()
API_KEY = os.getenv('ALPACA_PAPER_API_KEY')
SECRET_KEY = os.getenv('ALPACA_PAPER_SECRET_KEY')

trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
stream_client = StockDataStream(API_KEY, SECRET_KEY)

engine = LiveArbitrageMath()

#Getting the required amount details and defining the max trades
try:
    account = trading_client.get_account()
    TOTAL_CAPITAL = float(account.equity)
except Exception as e:
    print(f"Failed to connect to Alpaca API. Please check your credentials. Error: {e}")
    exit(1)

MAX_CONCURRENT_TRADES = 10
ALLOCATION_PER_TRADE = TOTAL_CAPITAL / MAX_CONCURRENT_TRADES

Z_SCORE_ENTRY = 2.0
Z_SCORE_EXIT = 0.0
MAX_BUFFER_SIZE = 60

#Loading all the datasets
try:

    pairs_df = pd.read_csv("tradable_pairs.csv")
    pairs_df.columns = pairs_df.columns.str.replace(' ', '_')
    PAIRS = [(row['Stock_A'], row['Stock_B']) for _, row in pairs_df.iterrows()]
except FileNotFoundError:
    print("Error: tradable_pairs.csv not found. Please run the filter script first.")
    exit(1)

#Exracting a flat list to send through web socket
UNIQUE_TICKERS = list(set([p[0] for p in PAIRS] + [p[1] for p in PAIRS]))

#To keep a track of the prices
price_history = {ticker: [] for ticker in UNIQUE_TICKERS}

#To keep a track of all the stocks we have and their quantity
active_positions = {pair: {'state': 'FLAT', 'qty_a': 0.0, 'qty_b': 0.0} for pair in PAIRS}

#Defining the logic to executing the trade
def execute_trade(pair, action, price_a=0, price_b=0, live_beta=0):
    #Handles exact capital allocation, concurrent trade limits, and safe offsetting.
    stock_a, stock_b = pair
    current_state = active_positions[pair]['state']
    
    # Calculate how many pairs are currently actively trading
    active_count = sum(1 for v in active_positions.values() if v['state'] != 'FLAT')

    if action == "CLOSE":
        print(f"\n[EXIT] Mean Reversion for {stock_a}/{stock_b}. Flattening exact quantities.")
        qty_a = active_positions[pair]['qty_a']
        qty_b = active_positions[pair]['qty_b']
        
        # Issue opposing market orders to safely exit without touching overlapping pairs
        if current_state == "LONG_A_SHORT_B":
            trading_client.submit_order(MarketOrderRequest(symbol=stock_a, qty=qty_a, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))
            trading_client.submit_order(MarketOrderRequest(symbol=stock_b, qty=qty_b, side=OrderSide.BUY, time_in_force=TimeInForce.DAY))
        elif current_state == "SHORT_A_LONG_B":
            trading_client.submit_order(MarketOrderRequest(symbol=stock_a, qty=qty_a, side=OrderSide.BUY, time_in_force=TimeInForce.DAY))
            trading_client.submit_order(MarketOrderRequest(symbol=stock_b, qty=qty_b, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))
            
        active_positions[pair] = {'state': 'FLAT', 'qty_a': 0.0, 'qty_b': 0.0}
        return

    # Treasury Guardrail: Prevent exceeding the account margin limit
    if active_count >= MAX_CONCURRENT_TRADES:
        print(f"[{stock_a}/{stock_b}] Signal triggered, but Treasury is full ({MAX_CONCURRENT_TRADES}/10 trades active). Ignoring.")
        return

    # Calculate exact share sizing for Beta-Neutral exposure
    shares_a = round((ALLOCATION_PER_TRADE / (price_a + (live_beta * price_b))), 2)
    shares_b = round((shares_a * live_beta), 2)
    
    print(f"\n[ENTRY] Routing Orders for {stock_a}/{stock_b}: {shares_a} shares of {stock_a} | {shares_b} shares of {stock_b}")

    if action == "LONG_A_SHORT_B":
        trading_client.submit_order(MarketOrderRequest(symbol=stock_a, qty=shares_a, side=OrderSide.BUY, time_in_force=TimeInForce.DAY))
        trading_client.submit_order(MarketOrderRequest(symbol=stock_b, qty=shares_b, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))
        
    elif action == "SHORT_A_LONG_B":
        trading_client.submit_order(MarketOrderRequest(symbol=stock_a, qty=shares_a, side=OrderSide.SELL, time_in_force=TimeInForce.DAY))
        trading_client.submit_order(MarketOrderRequest(symbol=stock_b, qty=shares_b, side=OrderSide.BUY, time_in_force=TimeInForce.DAY))
        
    # Save the exact state and quantities so we can safely exit later
    active_positions[pair] = {'state': action, 'qty_a': shares_a, 'qty_b': shares_b}



#Definig the web socket handler
async def on_minute_bar(bar):
    symbol = bar.symbol
    price_history[symbol].append(bar.close)
    
    if len(price_history[symbol]) > MAX_BUFFER_SIZE:
        price_history[symbol].pop(0)

    #Find every pair in our universe that includes the ticker that just updated
    affected_pairs = [p for p in PAIRS if symbol in p]

    for pair in affected_pairs:
        stock_a, stock_b = pair
        
        #Only calculate math if we have a full 60 minutes of data for both legs of the pair
        if len(price_history[stock_a]) == MAX_BUFFER_SIZE and len(price_history[stock_b]) == MAX_BUFFER_SIZE:
            
            series_a = pd.Series(price_history[stock_a])
            series_b = pd.Series(price_history[stock_b])
            
            live_beta, z_score = engine.get_live_trade_metrics(series_a, series_b)
            current_state = active_positions[pair]['state']

            #The trading logic
            if current_state == "FLAT":
                if z_score > Z_SCORE_ENTRY:
                    execute_trade(pair, "SHORT_A_LONG_B", series_a.iloc[-1], series_b.iloc[-1], live_beta)
                elif z_score < -Z_SCORE_ENTRY:
                    execute_trade(pair, "LONG_A_SHORT_B", series_a.iloc[-1], series_b.iloc[-1], live_beta)
                    
            else: # We are currently holding this pair
                if current_state == "SHORT_A_LONG_B" and z_score <= Z_SCORE_EXIT:
                    execute_trade(pair, "CLOSE")
                elif current_state == "LONG_A_SHORT_B" and z_score >= Z_SCORE_EXIT:
                    execute_trade(pair, "CLOSE")


if __name__ == "__main__":
    print(f"\nSubscribing to {len(UNIQUE_TICKERS)} unique tickers across {len(PAIRS)} pairs...")
    
    # Unpack the list of tickers into the subscription command
    stream_client.subscribe_bars(on_minute_bar, *UNIQUE_TICKERS)
    
    print("WebSocket connected. System is armed and filling memory buffers (will take 60 minutes to arm)...")
    stream_client.run()