from enum import Enum
import redis
import json, uuid
from ib_insync import *
import ib_insync
#from datetime import datetime, time
import datetime as dt
from sortedcontainers import SortedDict
from util import tz_min, tz_now
from pprint import pprint
from sample_strat import SampleStrategyHK
from scheduler import Scheduler
import pytz
from data_service import *

from cooc2 import COOC
import json

import asyncio
asyncio.set_event_loop(asyncio.ProactorEventLoop())

MIN_DATE = '1970-01-01'

class Trader:
    def __init__(self, jobs, tz='US/Eastern') -> None:

        # connect to Redis and publish tradingview messages
        self.rds = redis.Redis(host='localhost', port=6379, db=0)

        self.jobs = jobs
        self.tz = tz

        tz_new_york = dt.timezone(dt.timedelta(hours=-4))
        tz_hong_kong = dt.timezone(dt.timedelta(hours=8))

        if tz == 'US/Eastern':
            self.tzinfo = tz_new_york
        else:
            self.tzinfo = tz_hong_kong

        self.sche = Scheduler(tzinfo=dt.timezone.utc)
        
        self.trades = []
        self.init_timed_jobs(jobs)

    def now(self):
        return tz_now(self.tz)

    def period_check(self):
        self.evolve()
        self.publish_messages()

    def init_timed_jobs(self, jobs):
        for jb in jobs:
            if jb[0]:
                self.sche.daily(jb[0].replace(tzinfo=self.tzinfo), lambda: self.strat_to_job(jb[1]))
            else: # immediate
                self.strat_to_job(jb[1])

    def strat_to_job(self, s):
        s.evolve_1day(self.now().date().isoformat()) # time zone aware????     
        self.trades += s.trades

    def evolve(self):
        print(f"{self.now().time()} - evolve trading strategies")

        self.trades = []

        self.sche.exec_jobs()
        print(self.sche)

    def publish_messages(self):
        print(f"{self.now().time()} - publishing tradingview webhook messages")
        for tr in self.trades:
            pprint(tr)
            self.rds.publish("tradingview", json.dumps(tr), type='order')

    async def run_periodically(self, interval, periodic_function):
        while True:
            periodic_function()
            await asyncio.sleep(interval)

    def run(self):
        asyncio.run(self.run_periodically(60, self.period_check))

if __name__ == '__main__':
    ps = LiveData()
    jobs = [
        (dt.time(9,30), COOC(ps)),
        (dt.time(16,30), COOC(ps)),
        (None, COOC(ps)),
    ]
    #td = Trader([SampleStrategyHK(),COOC('2800')], tz=None)
    td = Trader(jobs, tz='US/Eastern')

    td.run()