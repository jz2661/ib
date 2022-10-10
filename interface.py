from data_service import *
import pandas as pd
import numpy as np
from ib_insync import *
import os
from abc import abstractmethod
from util import today_str, tz_now, tz_combine
import datetime
import json
import exchange_calendars as xcals

ROOT = r'C:\ib\strategies'
MIN_DATE = '1970-01-01'

class Strategy:
    def __init__(self, ds) -> None:
        self.name = "Default"
        self.notional = 1e6 # $1m
        self.tz = 'US/Eastern' # None if HK
        self.ds = ds # data service
        self.cal = xcals.get_calendar("XNYS")

    def now(self):
        return tz_now(self.tz)

    def path_finder(self, x):
        return os.path.join(ROOT, self.name, x)

    @abstractmethod
    def evolve(self, valdate):
        # using yday_ds
        # get today ds, and publish trades
        pass

    def evolve_1day(self, valdate: str):
        self.yday_ds = {}
        ds_last, ds_today = [self.path_finder(x) for x in [f'ds_{self.cal.previous_session(valdate).date()}.json', f'ds_{valdate}.json']]

        try:
            with open(ds_last, 'r') as f:
                self.yday_ds = json.load(f)
        except:
            pass
        
        self.trades = []
        ds = self.evolve(valdate)
        self.signoff()
        ds['level'] = self.level

        for dfile in [ds_today]:
            with open(dfile, 'w') as f:
                json.dump(ds, f)

    def make_order(self, tk, q, p, ot='Market', rtime=None):
        odr = {
            'type': 'order',
            'time': self.now().isoformat(),
            'ticker': tk,
            'quantity': q,
            'price': p,
            'order_type': ot,
        }
        if rtime:
            odr['ready'] = tz_combine(rtime, self.tz).isoformat()
        return odr
    
    def make_limit_prices(self, p, offset=0.1e-2):
        return [x.round(2) for x in (p*(1-offset), p*(1+offset))]

    def signoff(self):
        # calc pnl
        cash = self.yday_ds['cash']
        for td in self.trades:
            snap_str = str(td['ready'])[:5]
            p = self.data_service.get(td['ticker'], snap_str, self.valdate)
            cash -= p * td['quantity']
        
        self.level = cash

        try:
            units = self.units
            # all in units should also in prices
            self.level += sum(self.prices.get(tk, np.nan)*q for tk,q in units)
        except AttributeError:
            # no EOD position, like intraday strategy
            pass
        
        
class DailyStrategy(Strategy):
    # evolve once a day
    def __init__(self, ds) -> None:
        super().__init__(ds)
        self.job_time = datetime.time(9,30) # mng job

    def has_traded(self):
        ds_last = self.path_finder('ds_last.json')

        try:
            with open(ds_last, 'r') as f:
                self.yday_ds = json.load(f)
                return self.yday_ds.get('date', MIN_DATE) >= today_str(self.tz)
        except:
            pass
        
        return False

    def job_ready(self):
        return True
        #return self.now().time() > self.job_time

    def evolve_1day(self, valdate):
        if not self.job_ready():
            self.trades = []
            print(f"{self.name} scheduled {self.job_time} not ready at {self.now().time()}, skipping.")
            return

        if self.has_traded():
            self.trades = []
            print(f"{self.name} has evolved for {today_str(self.tz)}, skipping.")
            return

        super().evolve_1day()

if __name__ == '__main__':
    pass