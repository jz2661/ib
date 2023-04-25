import json
import datetime as dt
from data_service import *
import exchange_calendars as xcals
from interface import Strategy
import pandas as pd
import numpy as np

from cooc2 import COOC
import json

import asyncio
asyncio.set_event_loop(asyncio.ProactorEventLoop())

MIN_DATE = '1970-01-01'

class Backtester:
    def __init__(self, strat: Strategy, data_args, strat_args={}, cal='XNYS') -> None:
        self.data_args = data_args
        self.data_service = BacktestData(*data_args)
        self.strat = strat(self.data_service, **strat_args)
        
        self.cal = xcals.get_calendar(cal)

        self.levels = {}

    def run(self):
        days = [x.date() for x in self.cal.sessions_in_range(*self.data_args[1:3])]
        print(f"{days[0]}-{days[-1]}: backtest trading strategy {self.strat.name}")
        
        for vd in [str(x) for x in days]:
            print(vd)
            try:
                self.strat.evolve_1day(vd)
            except:
                break
            print(self.strat.level)
            self.levels[vd] = self.strat.level

        self.analytic()

    def analytic(self):
        self.pdf = pd.DataFrame.from_dict(self.levels, orient='index',columns=['level'])
        self.pdf['dif'] = self.pdf['level'].diff()
        print('Sharpe:')
        print(self.pdf['dif'].mean()/self.pdf['dif'].std()*np.sqrt(252))


if __name__ == '__main__':
    # 60days
    if 0:
        bt = Backtester(COOC, [['^NDX'],'2023-03-10',str(dt.date.today()),['1d','5m']])
    #CL
    if 1:
        strat_args = {
            'mode': 'CL'
        }
        bt = Backtester(COOC, [['USO'],'2023-03-10',str(dt.date.today()),['1d','5m']],strat_args)

    bt.run()