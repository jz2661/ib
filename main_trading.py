import datetime as dt
from trader import Trader
from data_service import *
from cooc2 import COOC

if __name__ == '__main__':
    ps = LiveData()
    jobs = [
        (dt.time(9,32), COOC(ps,send_order=True)),
        #(None, COOC(ps,send_order=True)),
        (dt.time(17,30), COOC(ps,send_order=False)),
        #(None, COOC(ps,send_order=False)),

        #CL
        (dt.time(9,32), COOC(ps,mode='CL',send_order=True)),
        (dt.time(17,30), COOC(ps,mode='CL',send_order=False)),        
    ]
    #td = Trader([SampleStrategyHK(),COOC('2800')], tz=None)
    td = Trader(jobs, tz='US/Eastern')

    td.run()
