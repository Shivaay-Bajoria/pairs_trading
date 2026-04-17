import pandas as pd
import numpy as np
import sys
import warnings
import statsmodels.tsa.stattools as ts
import itertools

#To supress the warning about the datasets when we are finding pairs, to keep terminal clean
warnings.filterwarnings("ignore")

def find_cointegrated_pairs(dataframe, p_value_threshold=0.02):
    #We will be finding the highly cointegrated pairs using the Engle_Granger Test
    tickers = dataframe.columns.tolist()
    all_pairs = list(itertools.combinations(tickers, 2))
    total_pairs = len(all_pairs)

    print(f"Total valid tickers: {len(tickers)}")
    print(f"Total unique pairs to analyze: {total_pairs}")
    print(f"Hunting for pairs with a P-Value < {p_value_threshold}...\n")

    cointegrated_pairs = []

    for i, (asset_a, asset_b) in enumerate(all_pairs):
        #To obatain a progress bar to check the progress
        if i % 500 == 0:
            sys.stdout.write(f"\rCrunching pair {i} of {total_pairs}...")
            sys.stdout.flush()

        series_a = dataframe[asset_a]
        series_b = dataframe[asset_b]

        try:
            #Running the Engle_granger Test
            coint_t, p_value, crit_value = ts.coint(series_a, series_b)

            #Checking if the p_value is less than the threshold
            #If it is then it is good cointegrate pair
            if p_value < p_value_threshold:
                cointegrated_pairs.append({
                    "Stock A": asset_a,
                    "Stock B": asset_b,
                    "P_Value": round(p_value, 5)
                })

        except Exception:
            continue
    
    print("\n Scan Complete")

    results_df = pd.DataFrame(cointegrated_pairs)
    if not results_df.empty:
        #Sorting it from highest to lowest score
        results_df = results_df.sort_values(by="P_Value").reset_index(drop=True)
    
    return results_df

if __name__ == "__main__":
    print("Loading the historical data for finding pairs.")

    #Getting the csv file of sp500 stocks
    df = pd.read_csv("alpaca_sp500_prices.csv", index_col="timestamp", parse_dates=True)

    print("Beginning $O(N^2)$ Cointegration Scan...")
    winners = find_cointegrated_pairs(df)

    if not winners.empty:
        winners.to_csv("cointegrated_pairs.csv", index=False)
        print("\nResults securely saved to cointegrated_pairs.csv")
    else:
        print("Increase the threshold, no cointegrated pairs found")

