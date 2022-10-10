import json
import datetime as dt
from data_service import *
import exchange_calendars as xcals
from interface import Strategy

from cooc import COOC
import json

import asyncio
asyncio.set_event_loop(asyncio.ProactorEventLoop())

MIN_DATE = '1970-01-01'

class Backtester:
    def __init__(self, strat: Strategy, data_args, cal='XNYS') -> None:
        self.data_args = data_args
        self.data_service = HistoryData(*data_args)
        self.strat = strat(self.data_service)
        
        self.cal = xcals.get_calendar(cal)

    def run(self):
        days = self.cal.sessions_in_range(*self.data_args[1:3])
        print(f"{days[0]}-{days[-1]}: backtest trading strategy {self.strat.name}")

        for vd in [str(x) for x in days]:
            print(vd)
            self.strat.evolve_1day(vd)
            print(self.level)

if __name__ == '__main__':
    bt = Backtester(COOC, [['QQQ'],'2022-09-01',str(dt.date.today()),['Open','09:30','15:55']])

    bt.run()