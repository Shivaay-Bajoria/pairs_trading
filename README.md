# 📈 Institutional Statistical Arbitrage Engine

An automated, end-to-end pairs trading pipeline built in Python. This engine discovers statistically cointegrated stock pairs, filters them through fundamental industry logic, and executes Beta-Neutral trades live using the Alpaca Trading API.

## 🧠 System Architecture

The pipeline is decoupled into three distinct engineering phases:

1. **The Statistical Engine:** Scans historical pricing data to find pairs that exhibit strong mean-reverting properties using the Engle-Granger Cointegration test.
2. **The Fundamental Filter:** Scrapes S&P 500 metadata from Wikipedia to cross-reference statistical pairs with GICS Sub-Industries. It automatically discards mathematically correlated pairs that lack fundamental business logic (e.g., pairing a tech stock with an oil drill).
3. **The Live Execution Bot:** Connects to Alpaca's WebSockets to monitor the filtered universe in real-time. It calculates dynamic Z-Scores and Hedge Ratios on a rolling 60-minute memory buffer.

## ✨ Key Features

* **Beta-Neutral Capital Allocation:** Dynamically calculates exact integer share sizing to ensure gross exposure is perfectly hedged regardless of price differences between the two assets.
* **Dynamic Risk Treasury:** Automatically syncs with the live broker account balance and strictly limits concurrent trades to protect margin requirements.
* **Warm-Up Memory Buffers:** Utilizes Alpaca's REST API (IEX feed) to pre-fill rolling historical data arrays on boot, bypassing the need to wait 60 minutes for live WebSocket data to accumulate.
* **API Armor:** Built-in `try/except` guardrails to catch fractional share rejections, "hard-to-borrow" shorting errors, and pending order locks without crashing the execution loop.

## 🛠️ Tech Stack

* **Language:** Python 3.x
* **Brokerage / Data:** [Alpaca Trading API](https://alpaca.markets/) (Paper Trading & IEX Market Data)
* **Quantitative Math:** `statsmodels`, `pandas`, `numpy`
* **Web Scraping:** `urllib`, `pandas.read_html`

## 📂 Project Structure

```text
├── .env                    # Secure Alpaca API credentials (ignored by git)
├── filter_pairs.py         # The Fundamental Logic Wikipedia Scraper
├── live_arb_math.py        # The lightweight OLS & Z-Score calculator
├── trading_bot.py          # The live WebSocket execution engine
├── cointegrated_pairs.csv  # Raw output from the historical scanner
└── tradable_pairs.csv      # The final filtered universe the bot trades
```

## 🚀 Installation & Setup

**1. Clone the repository**

```bash
git clone [https://github.com/YourUsername/Your-Repo-Name.git](https://github.com/YourUsername/Your-Repo-Name.git)
cd Your-Repo-Name
```

**2. Set up a Virtual Environment (Highly Recommended)**
It is best practice to run this bot inside an isolated environment.

```bash
# Create the virtual environment
python3 -m venv venv

# Activate the virtual environment (Mac/Linux)
source venv/bin/activate

# Activate the virtual environment (Windows)
venv\Scripts\activate
```

**3. Install dependencies**

```bash
pip install pandas statsmodels alpaca-py python-dotenv
```

**4. Configure Environment Variables**
Create a `.env` file in the root directory and add your Alpaca Paper Trading credentials. (Make sure `.env` is added to your `.gitignore` so you don't leak your keys!)

```env
ALPACA_PAPER_API_KEY=your_api_key_here
ALPACA_PAPER_SECRET_KEY=your_secret_key_here
```

## 💻 Usage

**Step 1: Filter the Universe**
Run the logic filter to sanitize your historical math data against fundamental business sectors.

```bash
python filter_pairs.py
```

*Output: Generates `tradable_pairs.csv*`

**Step 2: Deploy the Bot**
Launch the live execution engine. The bot will automatically warm up its buffers and connect to the Alpaca WebSocket.

```bash
python trading_bot.py
```

## ⚠️ Disclaimer

This software is for educational and research purposes only. Do not use this code to trade with real money. The author is not responsible for any financial losses incurred.
