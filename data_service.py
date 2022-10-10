import yfinance as yf
import pandas as pd
from datetime import date
import pickle as pkl

#__all__ = ['HistoricalData']

class HistoricalData:
    def __init__(self, cache=True) -> None:
        self.load_data(cache=cache)

    def load_data(self, cache=True, sheet='usetf.xlsx'):
        self.start_date = '2015-01-01'
        self.end_date = date.today().isoformat()

        cache_file = f'etf_prices_{self.end_date}.pkl'
        if cache:
            try:
                self.prices = pd.read_pickle(cache_file)  
                return
            except:
                print(f"Cache data not available for {self.end_date}. Downloading...")

        etfs = pd.read_excel(sheet)

        data = yf.download(list(etfs['Ticker']),self.start_date,self.end_date)
        self.raw = data

        self.yahoo_to_prices(data)

        self.prices.to_pickle(cache_file)

    def yahoo_to_prices(self, data):
        # get adj close, after div tax
        #set(data.columns.get_level_values(0)) # {'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume'}
        whtax = 0.3
        self.prices = data['Adj Close'] * (1-whtax) + data['Close'] * whtax
        return self.prices
    
    def adj_factor(self):
        self.af = self.raw['Adj Close'] / self.raw['Close']
        return self.af

class LiveData:
    def __init__(self) -> None:
        self.default_date = date.today().isoformat()

    def get_fields(self, tks, dt=None, prd='1d', itvl='1m', fields=['Open','High','Low','Close','Volume']):
        if dt == None:
            dt = self.default_date
        
        df = yf.download(  # or pdr.get_data_yahoo(...
            # tickers list or string as well
            tickers = tks,

            # start and end dates in ISO format
            start = dt,
            end = dt,

            # use "period" instead of start/end
            # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            # (optional, default is '1mo')
            period = prd,

            # fetch data by interval (including intraday if period < 60 days)
            # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            # (optional, default is '1d')
            interval = itvl,

            # group by ticker (to access via data['SPY'])
            # (optional, default is 'column')
            group_by = 'ticker',

            # adjust all OHLC automatically
            # (optional, default is False)
            auto_adjust = True,

            # download pre/post regular market hours data
            # (optional, default is False)
            prepost = True,

            # use threads for mass downloading? (True/False/Integer)
            # (optional, default is True)
            threads = True,

            # proxy URL scheme use use when downloading?
            # (optional, default is None)
            proxy = None
        )
        return df[fields]
    
    def get_open(self, tks):
        pass

    def get_day_close(self, tks):
        pass

if __name__ == '__main__':
    #f = HistoricalData()
    ld = LiveData()
    