from data_service import *
import data_service
import pandas as pd
import numpy as np
from ib_insync import *
from interface import DailyStrategy
import datetime
from util import last_tindex
from scipy import stats

class COOC(DailyStrategy):
    mode_map = {
        'QQQ': {
            'name': 'COOC',
            'tk': 'QQQ',
            'intime': datetime.time(9,30),
            'outtime': datetime.time(15,55),
            'tz': 'US/Eastern',
        },
        '2800': {
            'name': 'COOC_2800',
            'tk': '2800',
            'tk_yf': '2800.HK',
            'intime': datetime.time(9,30),
            'outtime': datetime.time(9,59),
            'tz': None,
        },        
    }

    def __init__(self, mode='QQQ') -> None:
        super().__init__()
        attr = self.mode_map[mode]
        self.ref = attr

        self.job_time = datetime.time(9,30) # mng job
        
        self.notional = 1e4 # testing

        self.name = attr['name']
        self.tz = attr['tz']

        self.intime = attr['intime']
        self.outtime = attr['outtime']

        self.hdf = pd.read_pickle('cooc_history_data.pkl')
        self.tk = attr['tk']

        self.comap = {}
        self.comap.update({x: 1 for x in [9,2,]})
        self.comap.update({x: .5 for x in [8,5,]})
        self.comap.update({x: -1 for x in [7,1,4,]})

        self.tilt = 0 # mkt neutral
    
    def co_to_signal(self, co):
        cor = min(int(stats.percentileofscore(self.hdf['co'][-500:], co, kind='rank')/10), 9)
        return np.median([self.comap.get(cor, 0)+self.tilt,-1,1])

    def evolve(self):
        tk_yf = self.ref.get('tk_yf', self.ref['tk'])

        live_data = data_service.LiveData([tk_yf], prd='1d', itvl='1m')
        lp = live_data.prices

        if lp.index[-1].date() < self.now().date() or lp.index[-1].time() < self.job_time:
            # data not ready
            return self.yday_ds

        daily_data = data_service.LiveData([tk_yf], prd='5d', itvl='1d').prices
        last_close = daily_data['Close'][last_tindex(daily_data, self.now().date())]
        lpthis = lp[-100:]
        opening = lpthis[lpthis['Volume'] > 0]['Open'][0]

        ds = {'date': self.now().date().isoformat()}
        wgt = self.co_to_signal(opening / last_close - 1.)
        
        # wgt = .5 # test

        ds['open'] = opening
        ds['last_close'] = last_close
        ds['wgt'] = wgt

        aq = int(self.notional * abs(wgt) / lp['Close'][-1] / 10) * 10
        qty = aq * np.sign(wgt)
        ds['qty'] = qty

        if not qty:
            return ds

        ltp2, ltp1 = self.make_limit_prices(lp['Close'][-1])
        if qty < 0:
            ltp2, ltp1 = ltp1, ltp2

        self.trades = []
        self.trades.append(self.make_order(self.tk, qty, ltp1, 'Limit', self.intime))
        self.trades.append(self.make_order(self.tk, -qty, ltp2, 'Market', self.outtime))
        ds['trades'] = self.trades

        return ds