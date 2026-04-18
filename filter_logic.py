import pandas as pd

def apply_logic_filter():
    print("Loading statistical pairs...")
    try:
        pairs_df = pd.read_csv("cointegrated_pairs.csv")
    except FileNotFoundError:
        print("Error: cointegrated_pairs.csv not found.")
        return
        
    #Dropping the 0.0 p-value of dual-class anomalies
    pairs_df = pairs_df[pairs_df['P_Value'] > 0.0]
    
    print("Fetching fundamental sector data from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    
    try:
        tables = pd.read_html(url, match="Symbol", storage_options=headers)
        wiki_df = tables[0]
        
        #Creating a dictionary mapping every ticker to its specific Sub-Industry
        sub_industry_map = dict(zip(wiki_df['Symbol'], wiki_df['GICS Sub-Industry']))
        
        #Map the industries to your math results
        pairs_df['SubIndustry_A'] = pairs_df['Stock A'].map(sub_industry_map)
        pairs_df['SubIndustry_B'] = pairs_df['Stock B'].map(sub_industry_map)
        
        #Only keeping the pairs in the exact same sub-industry
        logical_pairs = pairs_df[pairs_df['SubIndustry_A'] == pairs_df['SubIndustry_B']]
        
        #Clean up and sort by strongest P-Value
        logical_pairs = logical_pairs.dropna().sort_values(by='P_Value').reset_index(drop=True)
        
        logical_pairs.to_csv("tradable_pairs.csv", index=False)
        
    except Exception as e:
        print(f"\n--- SCRIPT FAILED ---")
        print(f"Error details: {e}")

if __name__ == "__main__":
    apply_logic_filter()