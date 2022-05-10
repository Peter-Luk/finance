from pandas_datareader import data
from fintools import FOA, get_periods
from finaux import roundup


class Equity(FOA):

    def __init__(self, code):
        _ = data.DataReader(code, data_source='yahoo')
        _.drop('Adj Close', axis=1, inplace=True)
        _.columns = [_.lower() for _ in _.columns]
        _.index.name = _.index.name.lower()
        self.code = code
        self.data = _
        self.date = _.index[-1].to_pydatetime()
        self.analyser = FOA(_, float)
        self.change = _.close.diff(1)[-1] / _.close[-2]
        self.periods = get_periods()

    def __call__(self):
        return self.data

    def __str__(self):
        return f"{self.date:%d-%m-%Y}: close @ {self.data.close.loc[self.date]:,.2f} ({self.change:0.3%}), rsi: {self.rsi().iloc[-1]:0.3f} and KAMA: {self.kama().iloc[-1]:,.2f}"

    def rsi(self, period=None):
        if period is None:
            period = self.periods['rsi']
        return self.analyser.rsi(period)

    def atr(self, period=None):
        if period is None:
            period = self.periods['atr']
        return self.analyser.atr(period)

    def kama(self, period=None):
        if period is None:
            period = self.periods['kama']
        return self.analyser.kama(period).astype('float64')
