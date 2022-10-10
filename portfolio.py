from enum import Enum
from ib_insync import *
import ib_insync
from datetime import datetime

import asyncio
asyncio.set_event_loop(asyncio.ProactorEventLoop())

if 0:
    ibc = IBC(976, gateway=True, tradingMode='live',
        userid='zjzzjz2010', password='zjzct2517')
    ibc.start()
    IB.run()

util.startLoop()  # only use in interactive environments (i.e. Jupyter Notebooks)

ib = IB()
ib.connect(port=7497) # paper
#ib.connect(port=7496) # live

ib.positions()

#stock = Stock('2800', 'SEHK', 'HKD')
stock = Stock('QQQ', 'SMART', 'USD')
#stock = Stock('BARC', 'LSE', 'GBP')
ib.qualifyContracts(stock)  
t = ib.reqMktData(stock,'233',snapshot=False)
br = ib.reqHistoricalData(stock,'233',snapshot=False)

order = MarketOrder("BUY", 500)
#cd = ib_insync.contract.ContractDetails(stock)
#ib.qualifyContracts(stock)  
#trade = ib.placeOrder(stock, order)

async def run_periodically(interval, periodic_function):
    while True:
        await asyncio.gather(asyncio.sleep(interval), periodic_function())
