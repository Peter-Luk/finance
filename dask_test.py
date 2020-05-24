import pandas as pd
import numpy as np
import pandas_securities

import dask.dataframe as ddf
from dask import compute, delayed
from pref import periods
from nta import stepper, grabber, hsirnd

data = pandas_securities.data
eids = data.index.get_level_values('Code').unique()

class Equity(object):
    def __init__(self, code, npartitions=5):
        self._npart = npartitions
        self.data = ddf.from_pandas(data.loc[code], self._npart)

    def __del__(self):
        self.data = self._npart = None
        del self.data, self._npart

    def ema(self, period=periods['Equities']['simple'], field='Close'):
        __ = 'ema'
        _ = stepper(grabber(self.data, field).compute(), period).apply(hsirnd)
        _.name = f'{__}{period:2d}'.upper()
        return ddf.from_pandas(_, self._npart)

    def kama(self, period=periods['Equities']['kama'], field='Close'):
        __ = 'kama'
        _data = grabber(self.data, field).compute()
        change = (_data - _data.diff(1).shift(period['er'])).abs()
        volatility = _data.diff(1).abs().rolling(period['er']).sum()
        er = change / volatility
        sc = (er * (2 / (period['fast'] + 1) - 2 / (period['slow'] + 1)) + 2 / (period['slow'] + 1)) ** 2
        _ = stepper(_data, period['er'], sc).apply(hsirnd)
        _name = f"{__}{period['er']:2d}".upper()
        return ddf.from_pandas(_, self._npart)
