from functools import cache
from os import path
from sympy import timed
import yfinance as yf
import pandas as pd
from datetime import date, time, datetime, timedelta
import pytz
from util import tz_now, WZException
import pickle as pkl
from enum import Enum

#__all__ = ['HistoricalData']

Fields = ['Open', 'High', 'Low', 'Close', 'Volume']

def yf_download(tk, start, end=date.today().isoformat(), interval='1d'):
    return yf.download(  # or pdr.get_data_yahoo(...
        # tickers list or string as well
        tickers = tk,

        # start and end dates in ISO format
        start = start,
        end = end,

        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        interval = interval,

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

class HistoricalData:
    def __init__(self, cache=True) -> None:
        #self.load_data(cache=cache)
        pass

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
    
    def cache_daily(self, tk, full_refresh=False):
        self.ticker = tk
        sdate = '2015-01-01'
        edate = (date.today() + timedelta(days=1)).isoformat()

        cache_file = f'hist_data\{tk}.pkl'

        # if cache file doesn't exist or force refresh is requested, pull full history data from yahoo
        if (not path.exists(cache_file)) or full_refresh:
            print(f'Downloading historical data for {tk} from {sdate} to {edate}')
            data = yf.download(tk, sdate, edate)
        else:
            # else read data from cache and check if we need to append latest data
            data = pd.read_pickle(cache_file)
            if data.index.max() < pd.to_datetime(date.today()):
                sdate = data.index.max().isoformat()[:10]
                print(f'Refreshing historical data for {tk} from {sdate} to {edate}')
                new_data = yf.download(tk, sdate, edate)

                if new_data.shape[0] > 0:
                    data = pd.concat([data[data.index < new_data.index.min()], new_data])
        
        self.start_date = data.index.min().isoformat()[:10]
        self.end_date = data.index.max().isoformat()[:10]

        data.to_pickle(cache_file)
        self.yahoo_to_prices(data)

    def cache_daily_multi_tickers(self, tks, full_refresh=False):
        self.tickers = tks
        sdate = '2015-01-01'
        edate = (date.today() + timedelta(days=1)).isoformat()

        cache_files = [f'hist_data\{tk}.pkl' for tk in tks]

        # if cache file doesn't exist or force refresh is requested, pull full history data from yahoo
        if (not all([path.exists(f) for f in cache_files])) or full_refresh:
            ticker_str = ','.join(tks)
            print(f'Downloading historical data for {ticker_str} from {sdate} to {edate}')
            data = yf.download(tks, sdate, edate, group_by='ticker')
        else:
            # else read data from cache and check if we need to append latest data
            for tk in tks:
                data[tk] = pd.read_pickle(cache_file)
            if data.index.max() < pd.to_datetime(date.today()):
                sdate = data.index.max().isoformat()[:10]
                print(f'Refreshing historical data for {tk} from {sdate} to {edate}')
                new_data = yf.download(tk, sdate, edate)

                if new_data.shape[0] > 0:
                    data = pd.concat([data[data.index < new_data.index.min()], new_data])
        
        self.start_date = data.index.min().isoformat()[:10]
        self.end_date = data.index.max().isoformat()[:10]

        data.to_pickle(cache_file)
        self.yahoo_to_prices(data)

    def get_daily_price(self, tk, full_refresh=False):
        self.cache_daily(tk, full_refresh)
        return self.prices
    
    def get_cache(self, tk):
        cache_file = f'hist_data\{tk}.pkl'
        try:
            data = pd.read_pickle(cache_file)
            return data
        except:
            print(f"Cache file doesn't exist: {cache_file}")

class LiveData:
    def __init__(self) -> None:
        self.default_date_str = date.today().isoformat()
        self.avail_fields = ['Open', 'High', 'Low', 'Close', 'Volume']
    
    def get_daily(self, tk, prd):
        return yf.download(tickers=tk, period=prd, interval='1d', auto_adjust=True, prepost=True)

    def yf_load(self, tk, fld, dt):
        tz_name = 'America/New_York'

        start = pytz.timezone(tz_name).localize(datetime.combine(dt, time.fromisoformat('00:00')))
        end = start + timedelta(days=2)
        start_utc = start.astimezone(pytz.utc)
        end_utc = end.astimezone(pytz.utc)
        
        interval = '1m'
        if fld in self.avail_fields and dt < tz_now(tz_name).date():
            interval = '1d'

        return yf_download(tk,start_utc,end_utc,interval)

    def get(self, tk, fld, date_str=None):
        if date_str is None:
            date_str = self.default_date_str
        dt = date.fromisoformat(date_str)

        df = self.yf_load(tk, fld, dt)

        val = None

        # Try get latest price as default return value
        try:
            val = df['Close'][-1]
        except:
            print("Empty result from yfinance for " + date_str + " " + fld)

        # Try get prices where volume > 0
        try:
            filtered = df[df['Volume']>0].loc[date_str]
        except:
            print("Cannot locate requested date: " + date_str)
            filtered = df

        tz_name = 'America/New_York'
        if not fld in self.avail_fields:
            try:
                tm = time.fromisoformat(fld)
                snap_t = pytz.timezone(tz_name).localize(datetime.combine(dt, tm))
            except:
                raise Exception("Invalid data field requested: " + fld)

        if filtered.shape[0] > 0:
            if fld in self.avail_fields:
                if dt < tz_now(tz_name).date():
                    val = filtered[fld]
                else:
                    if fld == 'Open':
                        val = filtered[fld][0]
                    elif fld == 'Close':
                        val = filtered[fld][-1]
                    elif fld == 'High':
                        val == filtered[fld].max()
                    elif fld == 'Low':
                        val = filtered[fld].min()
                    elif fld == 'Volume':
                        val = filtered[fld].sum()
            else:
                try:
                    df.index = [x.strftime('%Y-%m-%d %H:%M') for x in df.index]
                    val = df.loc[snap_t.strftime('%Y-%m-%d %H:%M')]['Close']
                except:
                    raise Exception("Cannot locate requested datetime in yfinance data downloaded: " + snap_t.strftime('%Y-%m-%d %H:%M'))
        
        return val

class BacktestData:
    def __init__(self, tk, start, end, intervals) -> None:
        self.dfs = {}
        for interval in intervals:
            self.dfs[interval] = yf_download(tk,start,end,interval)
        self.df_col_level = self.dfs[intervals[-1]].columns.nlevels

    def get(self, tk, fld, date_str=None):
        if fld in Fields:
            ky = '1d'
            col = fld
            time_idx = date_str
        elif int(fld[-1])%5 == 0:
            ky = '5m'
            col = 'Open'
            time_idx = date_str+' '+fld
        else:
            raise WZException("TODO: Non-5m snaps")
        if self.df_col_level > 1:
            df = self.dfs[ky][tk]
        else:
            df = self.dfs[ky]
        return df[col][time_idx]


if __name__ == '__main__':
    #f = HistoricalData()
    #ld = LiveData()
    #df = ld.yf_load(['QQQ'],'15:55','2022-10-14')
    #tk, fld, date_str = ['QQQ'],'15:55','2022-10-14'
    #df.index = [x.strftime('%Y-%m-%d %H:%M') for x in df.index]
        
    bd = BacktestData(['QQQ','SPY'],'2022-09-10',str(date.today()),['1d','5m'])
