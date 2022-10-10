from data_service import *
import data_service
import pandas as pd
import numpy as np
from ib_insync import *
from interface import DailyStrategy
import datetime
from util import tz_combine

class SampleStrategyHK(DailyStrategy):
    def __init__(self) -> None:
        super().__init__()

        self.name = "Sample_HK"
        self.tz = None

        self.intime = datetime.time(9,30)
        self.outtime = datetime.time(9,42)

    def evolve(self):
        print("evolving sampleHK.")
        qty = 500
        td = {
            'type': 'order',
            'time': self.now().isoformat(),
            'ready': tz_combine(self.intime, self.tz).isoformat(),
            'ticker': '2800',
            'quantity': qty,
            'price': 18.55,
            'order_type': 'Market'
            
        }        
        tdout = {
            'type': 'order',
            'time': self.now().isoformat(),
            'ready': tz_combine(self.outtime, self.tz).isoformat(),
            'ticker': '2800',
            'quantity': -qty,
            'price': 18.55,
            'order_type': 'Market'
        }           

        self.trades = [td, tdout]

        return {'date': self.now().date().isoformat()}
