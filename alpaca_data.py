from dotenv import load_dotenv
import os
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from datetime import datetime
import pandas as pd
from alpaca.data.timeframe import TimeFrame
import time

#Loading the env file
load_dotenv()

#Fetching the keys
KEY = os.getenv('ALPACA_PAPER_API_KEY')
SECRET_KEY = os.getenv('ALPACA_PAPER_SECRET_KEY')

#Checking if keys are present or not
if not KEY or not SECRET_KEY:
    raise ValueError("API Keys not found, check .env file")

data_client = StockHistoricalDataClient(KEY, SECRET_KEY)

#Creating the function to get the historical data
def get_alpaca_historical_data(tickers, start_date, end_date, chunk_size = 20):
    print(f"Fetching 2 years of historical data for {len(tickers)} stocks from Alpaca...")
    all_dataframes = []

    #Dividing the tickers into 20 tickers to prevent the alpaca api from overloading
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        print(f"Fetching batch {i//chunk_size + 1} of {(len(tickers)//chunk_size) + 1}...")

        request_params = StockBarsRequest(
            symbol_or_symbols=chunk,
            timeframe=TimeFrame.Day,
            start=start_date,
            end=end_date,
        )
        
        try:
            #Using the alapaca client, getting the historical data while providing the request parameters
            bars = data_client.get_stock_bars(request_params=request_params)
            df = bars.df

            #Cleaning the dataset by remaning the columns with timestamp, symbol and closing values
            clean_df = df.reset_index().pivot(index='timestamp', columns='symbol', values='close')
            all_dataframes.append(clean_df)

            time.sleep(5)

        except Exception as e:
            print(f"Error Fetching Chunk {chunk[0]}-{chunk[-1]}: {e}")
            continue

    print("\nMerging all batches into a master matrix...")
    master_df = pd.concat(all_dataframes, axis=1)
    return master_df

if __name__ == "__main__":
    #Getting the tickers from the csv file
    try:
        ticker_df = pd.read_csv("sp500_tickers.csv")
        sp500_tickers = ticker_df["Ticker"].tolist()
    except FileNotFoundError:
        print("Error: sp500_tickers.csv not found. Please run scraper.py first.")
        exit(1)

    start = datetime(2023, 1, 1)
    end = datetime(2026, 1, 1)

    price_matrix = get_alpaca_historical_data(tickers=sp500_tickers, start_date=start, end_date=end)
    #Saving the csv
    price_matrix.to_csv("alpaca_sp500_prices.csv")
    print(f"Saved massive dataset to alpaca_sp500_prices.csv successfully.")

