from executor import Executor

if __name__ == '__main__':
    #ec = Executor(port=7497, tz=None, exch='SEHK', ccy='HKD') # paper
    ec = Executor(port=7496, tz='US/Eastern', exch='SMART', ccy='USD')

    ec.run()
