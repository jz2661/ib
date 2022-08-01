from ib_insync import *
import yfinance as yf
import pandas as pd
from datetime import date
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import TruncatedSVD
from sklearn.model_selection import ShuffleSplit,GridSearchCV
from sklearn.ensemble import HistGradientBoostingClassifier
import pickle as pkl

class Forecast:
    def __init__(self) -> None:
        self.load_data()

    def load_data(self, cache=True, sheet='usetf.xlsx'):
        self.start_date = '2015-01-01'
        self.end_date = date.today().isoformat()

        cache_file = f'etf_prices_{end_date}.pkl'
        if cache:
            try:
                self.prices = pd.read_pickle(cache_file)  
                return
            except:
                print(f"Cache data not available for f{self.end_date}. Downloading...")

        etfs = pd.read_excel(sheet)

        data = yf.download(list(etfs['Ticker']),self.start_date,self.end_date)
        self.yahoo_to_prices(data)

        self.prices.to_pickle(cache_file)

    def yahoo_to_prices(self, data):
        # get adj close, after div tax
        #set(data.columns.get_level_values(0)) # {'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume'}
        whtax = 0.3
        self.prices = data['Adj Close'] * (1-whtax) + data['Close'] * whtax
        return self.prices

    def data_science(self, cache=True):
        pass

    def get_factors(self, cache=True):
        self.ret = self.prices.pct_change()[1:]
        #scaler = StandardScaler()
        self.retn = self.ret / self.ret.std()

        n_comp = 10
        cache_failed = True
        cache_file = f'svd.pkl'
        if cache:
            try:
                with open(cache_file, 'rb') as f:
                    svd = pkl.load(f)
                factors = svd.transform(self.retn.dropna(1))
                cache_failed = False
            except:
                print(f"Cache svd not available for f{self.end_date}. Fitting...")

        if cache_failed:
            svd = TruncatedSVD(n_components=n_comp, random_state=42)
            factors = svd.fit_transform(self.retn.dropna(1))
            with open(cache_file, 'rb') as f:
                pkl.dump(svd, f)            

        factors /= svd.singular_values_

        self.factors = pd.DataFrame(factors, index=self.retn.index, columns=[x for x in range(n_comp)])

        self.loadings = self.factors.T@self.ret.fillna(0)

    def prepare_data(self):

        X = self.factors.copy()
        X['y'] = self.ret['QQQ']
        self.Xall = X.copy()

        Xm = X.groupby(pd.Grouper(freq='m')).sum()
        Xm['y'] = Xm['y'].shift(-1)
        self.Xm = Xm

        xmi = Xm[:-1]

        self.X, self.y, self.w = xmi.drop('y',axis=1), xmi['y'] > 0, abs(xmi['y'])

    def predict(self, how='histgrad'):
        if how not in ['histgrad']:
            raise NotImplementedError()
            
        clf = HistGradientBoostingClassifier()
        param_grid = {
            'learning_rate': [.01,.1,.5],
            'random_state': [42],
            'min_samples_leaf': [1,5],
        }

        grid = GridSearchCV(
            clf,
            param_grid=param_grid,
            cv=ShuffleSplit(
                test_size=40, n_splits=10, random_state=42
            ),
        )
        grid.fit(self.X, self.y, sample_weight=self.w)

        prob = [x[1] for x in grid.predict_proba(self.Xm.drop('y',axis=1))]

        pred = pd.DataFrame(self.Xm['y'])
        pred['prob'] = prob
        self.pred = pred
        self.grid = grid