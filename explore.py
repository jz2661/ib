from data_service import *
import data_service
import pandas as pd
import numpy as np

if __name__ == '__main__':
    ds = data_service.HistoricalData(False)
    af = ds.adj_factor()
    tk = 'QQQ'
    c,o = ds.raw['Close'][tk], ds.raw['Open'][tk]
    df = pd.concat([o,c],axis=1)
    df.columns = ['o','c']
    df['co'] = df['o']/df['c'].shift(1)-1
    df['oc'] = df['c']/df['o']-1
    df['cor'] = df['co'].rank(pct=1)

    df = df.dropna()
    df0 = df.copy()

    df = df0[-500:]
    df['corg'] = df['cor'].apply(lambda x: min(int(x*10),9))

    dfg = df.groupby('corg')['oc']
    rm = dfg.median()
    rs = rm / dfg.std()
