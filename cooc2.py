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
            'tk': '^NDX',
            'trade_tk': 'MNQ',
            'intime': '09:40',
            'outtime': '14:55',
            'tz': 'US/Eastern',
            'comap': {
                9:4,
                8:0,
                5:2,
                2:-2,
                7:-2,
                1:-2,
                4:-4,
                0:-0,
                3:2,
            },
        },
        '2800': {
            'name': 'COOC_2800',
            'tk': '2800',
            'tk_yf': '2800.HK',
            'intime': datetime.time(9,30),
            'outtime': datetime.time(9,59),
            'tz': None,
        },
        'CL': {
            'name': 'COOC_CL',
            'tk': 'USO',
            'trade_tk': 'MCL', # QM, MCL, QMMY
            'intime': '09:40',
            'outtime': '14:55',
            'tz': 'US/Eastern',
            'comap': {
                9:0,
                8:0,
                5:-2,
                2:0,
                
                7:-2,
                1:0,
                4:2,
                
                0:0,
                3:2,
                6:-0,
            },            
        },            
    }

    def __init__(self, ds, mode='QQQ', **kwargs) -> None:
        super().__init__(ds, **kwargs)
        attr = self.mode_map[mode]
        self.ref = attr
        
        self.notional = 1e4 # testing

        self.name = attr['name']
        self.tz = attr['tz']

        self.intime = attr['intime']
        self.outtime = attr['outtime']

        self.hdf = pd.read_pickle('cooc_history_data.pkl')
        self.tk = attr['tk']
        self.trade_tk = attr['trade_tk']

        self.comap = attr['comap']

        self.tilt = 0 # mkt neutral

        #self.data_service = ds

    def co_to_signal(self, co):
        pct = stats.percentileofscore(self.hdf['co'][-500:], co, kind='rank')
        cor = min(int(pct/10), 9)
        print(f'Percentage: {pct} Bar: {cor}')
        return np.median([self.comap.get(cor, 0)+self.tilt,-10,10])

    def evolve(self, valdate):

        try:
            last_close = self.yday_ds['prices'][self.tk]
        except:
            # no yday_ds
            last_close = self.data_service.get(self.tk, 'Close', str(self.cal.previous_session(valdate).date()))        

        opening = self.data_service.get(self.tk, 'Open', valdate)
        #latest = self.data_service.get(self.tk, 'Close', valdate) # live and backtest inconsistent
        latest = opening

        ds = {'date': valdate}
        print(f'Opening: {opening} Last Close: {last_close} Ret: {opening/last_close - 1.}')
        wgt = self.co_to_signal(opening / last_close - 1.)
        
        # wgt = .5 # test

        ds['open'] = opening
        ds['last_close'] = last_close
        ds['wgt'] = wgt

        aq = int(self.notional * abs(wgt) / latest / 10) * 10
        # futures
        aq = int(abs(wgt))

        qty = -aq * np.sign(wgt)
        
        ds['intraday_qty'] = qty

        self.trades = []
        ds['trades'] = self.trades
        ds['units'] = {}
        ds['prices'] = {self.tk: self.data_service.get(self.tk, 'Close', valdate)}

        if not qty:
            return ds

        #futures
        ltp2, ltp1 = self.make_limit_prices(latest,offset=0.4e-2)
        if qty < 0:
            ltp2, ltp1 = ltp1, ltp2

        self.trades.append(self.make_order(self.trade_tk, qty, ltp1, 'Market', self.intime))
        self.trades.append(self.make_order(self.trade_tk, -qty, ltp2, 'Market', self.outtime))

        return ds
