import json
import datetime as dt
from data_service import *
import exchange_calendars as xcals
from interface import Strategy

from cooc2 import COOC
import json

import asyncio
asyncio.set_event_loop(asyncio.ProactorEventLoop())

MIN_DATE = '1970-01-01'

class Backtester:
    def __init__(self, strat: Strategy, data_args, cal='XNYS') -> None:
        self.data_args = data_args
        self.data_service = BacktestData(*data_args)
        self.strat = strat(self.data_service)
        
        self.cal = xcals.get_calendar(cal)

    def run(self):
        days = [x.date() for x in self.cal.sessions_in_range(*self.data_args[1:3])]
        print(f"{days[0]}-{days[-1]}: backtest trading strategy {self.strat.name}")

        for vd in [str(x) for x in days]:
            print(vd)
            self.strat.evolve_1day(vd)
            print(self.strat.level)

if __name__ == '__main__':
    bt = Backtester(COOC, [['QQQ'],'2022-09-10',str(dt.date.today()),['1d','5m']])

    bt.run()