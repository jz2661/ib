import pandas as pd
from ib_insync import *
import ib_insync
from datetime import datetime
from data_service import *
import pickle as pkl
import seaborn as sns
import matplotlib.pyplot as plt

PERF_UNDERLYING = 'FIFOPerformanceSummaryUnderlying'
COMMISSION = 'UnbundledCommissionDetail'

def get_report(rtype='ret', cache=True):
    cache_file = f'flexreport_{rtype}.pkl'
    fr = ib_insync.flexreport.FlexReport()
    if cache:
        fr.load(cache_file)
        return fr

    qid = {
        'agg': 721219,
        'ret': 721267,
    }

    fr.download(token=241440252490860803760250, queryId=qid[rtype])
    # slow, cache
    fr.save(cache_file)
    return fr

def ret_history(cache=True):
    fr = get_report('ret', cache=cache)
    df = fr.df('ChangeInNAV')
    df.to_excel(f'ret_history_{datetime.today().isoformat()[:10]}.xlsx')
    return df

def benchmark(cache=False):
    # entry, cache false
    rh = ret_history(cache=cache)
    ds = HistoricalData()
    ds.prices['QQQ']

    df = rh[['fromDate','twr']]
    df['Date'] = pd.to_datetime(df['fromDate'].apply(str))
    df = df.set_index('Date')

    df = df.merge(ds.prices['QQQ'],how='left',left_index=True,right_index=True).fillna(method='ffill')
    df['p'] = (df['twr']/100+1).cumprod()*100
    df['QQQ'] *= df['p'][0] / df['QQQ'][0]

    df.to_excel(f'benchmark_perf.xlsx')
    sns_ts(df[['p','QQQ']])

def sns_ts(df):
    #sns.set_theme(style="darkgrid")

    # Plot the responses for different events and regions
    sns.lineplot(data=df)
    plt.savefig('benchmark.png',dpi=900)

def commission_detail(fr):
    cf = fr.df(COMMISSION)
    cf['value'] = cf['quantity'].abs()*cf['price']
    cf['com_rate'] = cf['totalCommission'] / cf['value']
    return cf.groupby('symbol')['com_rate'].median()*1e4

if __name__ == '__main__':
    benchmark()

    #c = commission_detail(fr)