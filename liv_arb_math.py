import statsmodels.api as sm

class LiveArbitrageMath:
    def __init__(self):
        pass

    def get_live_trade_metrics(self, recent_series_a, recent_series_b):
        #Using to calculate the Z-Value and well as the hedge ratio

        model = sm.OLS(recent_series_a, recent_series_b).fit()
        live_hedge_ratio = model.params.iloc[0]

        #Calculating the spread
        spread = recent_series_a - (live_hedge_ratio * recent_series_b)

        #Getting the Z-Value
        current_spread = spread.iloc[-1]
        spread_mean = spread.mean()
        spread_std = spread.std()

        if spread_std == 0:
            return round(live_hedge_ratio, 4), 0.0
        
        z_score = (current_spread - spread_mean) / spread_std

        return round(live_hedge_ratio, 4), round(z_score, 3)



