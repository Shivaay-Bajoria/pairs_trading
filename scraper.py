import pandas as pd

def scrape_sp500():
    print("Fetching live S&P 500 ticker list from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    
    try:
        # Pass the browser disguise directly into Pandas via storage_options
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        
        # Pandas handles the request, the SSL, the decoding, and the parsing in one line
        tables = pd.read_html(url, match="Symbol", storage_options=headers)
        table = tables[0]
        
        raw_tickers = table["Symbol"].tolist()
        
        # Clean the tickers to prevent Alpaca API crashes
        clean_tickers = [ticker for ticker in raw_tickers if '.' not in ticker and '-' not in ticker]
        
        ticker_df = pd.DataFrame(clean_tickers, columns=["Ticker"])
        ticker_df.to_csv("sp500_tickers.csv", index=False)
        
        print(f"Success! {len(clean_tickers)} tickers securely saved to sp500_tickers.csv")
        
    except Exception as e:
        print(f"Scraping Failed: Error details: {e}")

if __name__ == "__main__":
    scrape_sp500()