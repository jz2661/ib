from enum import Enum
import redis
import time
import json, uuid
from ib_insync import *
import ib_insync
from datetime import datetime
from sortedcontainers import SortedDict
from util import tz_min, tz_now, dtstr_add_tz, run_periodically
from pprint import pprint
import random

import asyncio
asyncio.set_event_loop(asyncio.ProactorEventLoop())

MIN_DATE = '1970-01-01'

class Executor:
    def __init__(self, port=7497, tz='US/Eastern', exch='SMART', ccy='USD') -> None:
        if 0:
            ibc = IBC(1012, gateway=False, tradingMode='live',
                    twsPath='C:\\tws',
                    ibcPath='C:\\ib\\IBC',
                    ibcIni='C:\\IBC\\config.ini',

                userid='zjzzjz2010', password='zjzct2517')
            ibc.start()
            IB.run()

        util.startLoop()  # only use in interactive environments (i.e. Jupyter Notebooks)

        self.ib = IB()
        random_id = random.randint(0, 9999)
        self.ib.connect(port=port, clientId=random_id, timeout=0) # paper

        # connect to Redis and subscribe to tradingview messages
        r = redis.Redis(host='localhost', port=6379, db=0)
        self.p = r.pubsub()

        self.tz = tz
        self.trades = SortedDict()

        self.exch = exch
        self.ccy = ccy

    '''
    order_message = {
        'type': 'order',
        'time': '2022-09-05T19:49:00Z',
        'data': {
            'ticker': 'QQQ',
            'side': 'BUY',
            'quantity': 500,
            'price': 180.55,
            'order_type': 'Market'
        }
    }
    '''

    def now(self):
        return tz_now(self.tz)

    def period_check(self):
        self.check_messages()
        self.execute()

    def valid_trade(self, stock, order):
        # just before submit
        if order.action == "SELL":
            avs = self.ib.accountValues()
            try:
                cash = float([x for x in avs if x.tag=='CashBalance' and x.currency==self.ccy][0].value)
            except:
                print("Cash Balance Check Fail")
                print([x for x in avs if x.tag=='CashBalance' and x.currency==self.ccy])

        return True

    def cross_orders(self, tsubmit):
        # return list of (stock, order)
        # if multi orders for one name, only one (market) order is returned
        tk_orders = {}
        sym_contract = {}
        for k in tsubmit:
            for (stock, order) in self.trades[k]:
                sym = stock.symbol
                sym_contract[sym] = stock
                tk_orders[sym] = tk_orders.get(sym, [])+[order]
        res = []
        for sym,ods in tk_orders.items():
            k = sym_contract[sym]
            if len(ods)==1:
                res.append([k,ods[0]])
            else:
                qty = sum([o.totalQuantity if o.action=="BUY" else -o.totalQuantity for o in ods])
                if abs(qty) >= 1:
                    side = "BUY" if qty > 0 else "SELL"                
                    order = MarketOrder(side, abs(qty))
                    res.append([k,order])                
        return res

    def execute(self):
        tnow = self.now()
        print(f"{self.now().time()} - try executing")

        ts = self.trades.keys()
        tsubmit = [t for t in ts if tnow > t]

        # merge orders
        for (stock, order) in self.cross_orders(tsubmit):
            # TODO: check trade not overflow
            if not self.valid_trade(stock, order):
                print(f"Failed to validate trade:")
                print(f"Rejecting order {stock} {order}")

            self.ib.qualifyContracts(stock)  
            trade = self.ib.placeOrder(stock, order)        
            print(f"Submitting order {stock} {order}")

        for k in tsubmit:
            del self.trades[k]
        
        if self.trades:
            print(f"Pending orders:")
            pprint(self.trades)
        else:
            print(f"No pending orders.")

    def check_messages(self):
        print(f"{self.now().time()} - checking for tradingview webhook messages")
        try:
            if not self.p.subscribed:
                self.p.subscribe('tradingview')
        except:
            print("Redis: No connection could be made")
            return

        timeout = 20.
        stop_time = time.time() + timeout

        while time.time() < stop_time:
            message = self.p.get_message(timeout=timeout)
            if not message:
                break
            if (message['type'] == 'message'):
                print(message)
                try:
                    data = json.loads(message['data'])
                except:
                    continue
                
                if abs(data['quantity']) < 1e-6:
                    return

                stock = Stock(data['ticker'], self.exch, self.ccy)
                if data['ticker'] in ['MNQ','NQ','MCL','QM']:
                    stock = ContFuture(data['ticker'])

                side = "BUY" if data['quantity'] > 0 else "SELL"
                data['quantity'] = abs(data['quantity'])
                
                order = LimitOrder(side, data['quantity'], data['price']) \
                        if data['order_type'] == 'Limit' \
                        else MarketOrder(side, data['quantity'])
                #order.orderId = uuid.uuid1().int  # ib reject

                if 'ready' in data:
                    ready_t = dtstr_add_tz(data.get('ready', None), self.tz)
                else:
                    ready_t = tz_min()

                self.trades[ready_t] = self.trades.get(ready_t,[]) + [[stock, order]] # !replacing existing order

    def run(self):
        #self.ib.run()
        asyncio.run(run_periodically(60, self.period_check))

if __name__ == '__main__':
    #ec = Executor(port=7497, tz=None, exch='SEHK', ccy='HKD') # paper
    ec = Executor(port=7496, tz='US/Eastern', exch='SMART', ccy='USD')

    # test cross_orders
    if 0:
        s = ContFuture('MNQ')
        ec.trades[tz_min()] = [[s,MarketOrder("BUY", 2.)],[s,MarketOrder("SELL", 1.)]] 
        ec.execute()
