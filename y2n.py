import asyncio
import pandas as pd
# import numpy as np
import sqlalchemy as db
from tqdm import tqdm
import pref

B_scale = pref.B_scale
USHK = pref.USHK

try:
    from scipy.constants import golden_ratio as gr
except ImportError:
    gr = 1.618

from datetime import datetime
from time import sleep
from utilities import YAML_PREFERENCE, filepath
from pathlib import os
from benedict import benedict
from nta import Viewer, hsirnd
from alchemy import AF, AE
from ormlib import Securities, Derivatives, trade_data, async_fetch, sync_fetch
from sqlalchemy.future import select
# pd, np, db, gr, datetime, tqdm, sleep, B_scale, USHK = pref.y2n

YAML = benedict.from_yaml(f"{os.getenv('PYTHONPATH')}{os.sep}{YAML_PREFERENCE}")
B_scale = YAML.B_scale
USHK = YAML.USHK


class Futures(AF, Viewer):
    # periods = pref.periods['Futures']
    periods = YAML.periods.Futures

    def __init__(self, code):
        # self._conf = pref.db['Futures']
        self._conf = YAML.db.Futures
        rc = entities(self._conf['name'])
        if code.upper() not in rc:
            code = rc[-1]
        self.code = code
        self._af = AF(self.code)
        self.table = self._af.table
        self.rc = self._af.columns
        self.__conn = self._af.connect
        self.data = self.combine(self._conf['freq']).sort_index()
        self.view = Viewer(self.data)
        self._date = self.data.index[-1]
        self._close = self.data['Close'][-1]

    def __del__(self):
        self._conf = self.table = self.rc = self._af = self.__conn = None
        self.code = self.data = self.view = self._date = self._close = None
        del(self._conf, self.table, self.rc, self._af, self.__conn, self.code)
        del(self.data, self.view, self._date, self._close)

    def __call__(self, date=None):
        if date is None:
            date = self._date
        _ = self.maverick(date=date, unbound=False)
        _.name = self.code
        return _

    def __str__(self):
        delta = round(self.data['Close'].diff()[-1] / self.data['Close'][-2] * 100, 3)
        return f"{self.code} close @ {self._close} ({delta}%) on {self._date:'%Y-%m-%d'}"

    def insert(self, values, conditions):
        return self._af.append(values, conditions)

    def delete(self, conditions):
        return self._af.remove(conditions)

    def update(self, values, conditions):
        return self._af.amend(values, conditions)

    def combine(self, freq='bi-daily'):
        return self._af.combine(freq)

    def mas(self, date=None, period=periods):
        if date is None:
            date = self._date
        return self.view.mas(self.data, period, date)

    def idrs(self, date=None, period=periods):
        if date is None:
            date = self._date
        return self.view.idrs(self.data, period. date)

    def sma(self, date=None, period=periods['simple'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.ma(
            self.data,
            period,
            's',
            req_field,
            date).map(hsirnd)

    def ema(self, date=None, period=periods['simple'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.ma(
            self.data, period,
            'e',
            req_field,
            date).map(hsirnd)

    def wma(self, date=None, period=periods['simple'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.ma(
            self.data,
            period,
            'w',
            req_field,
            date).map(hsirnd)

    def kama(self, date=None, period=periods['kama'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.kama(self.data, period, req_field, date).map(hsirnd)

    def atr(self, date=None, period=periods['atr']):
        if date is None:
            date = self._date
        return self.view.atr(self.data, period, date)

    def rsi(self, date=None, period=periods['rsi']):
        if date is None:
            date = self._date
        return self.view.rsi(self.data, period, date)

    def obv(self, date=None):
        if date is None:
            date = self._date
        return self.view.obv(self.data, date)

    def ovr(self, date=None, period=periods):
        if date is None:
            date = self._date
        return self.view.ovr(self.data, period, date)

    def vwap(self, date=None):
        if date is None:
            date = self._date
        return self.view.vwap(self.data, date)

    def bb(self, date=None, period=periods['simple']):
        if date is None:
            date = self._date
        return self.view.bb(self.data, period, date)

    def apz(self, date=None, period=periods['apz']):
        if date is None:
            date = self._date
        return self.view.apz(self.data, period, date)

    def dc(self, date=None, period=periods['dc']):
        if date is None:
            date = self._date
        return self.view.dc(self.data, period, date)

    def kc(self, date=None, period=periods['kc']):
        if date is None:
            date = self._date
        return self.view.kc(self.data, period, date)

    def macd(self, date=None, period=periods['macd']):
        if date is None:
            date = self._date
        return self.view.macd(self.data, period, date)

    def adx(self, date=None, period=periods['adx']):
        if date is None:
            date = self._date
        return self.view.adx(self.data, period, date)

    def soc(self, date=None, period=periods['soc']):
        if date is None:
            date = self._date
        return self.view.soc(self.data, period, date)

    def stc(self, date=None, period=periods['stc']):
        if date is None:
            date = self._date
        return self.view.stc(self.data, period, date)

    def gat(self, date=None, period=periods['atr']):
        if date is None:
            date = self._date
        return self.view.gat(self.data, period, date).apply(hsirnd, 1).unique()

    def maverick(self, date=None, period=periods, unbound=True, exclusive=True):
        if date is None:
            date = self._date
        return self.view.maverick(self.data, period, date, unbound, exclusive)


class Equities(AE, Viewer):
    periods = pref.periods['Equities']

    def __init__(self, code, adhoc=False):
        self.foreign = False
        self._conf = pref.db['Equities']
        if code not in entities(self._conf['name']):
            adhoc = True
        self.code = code
        self.data = self.fetch(self.code, adhoc=adhoc)
        if adhoc is False:
            self._ae = AE(self.code)
            self.table = self._ae.table
            self.rc = self._ae.columns
            self.__conn = self._ae.connect
        self.view = Viewer(self.data)
        self._date = self.data.index[-1]
        self._close = hsirnd(self.data['Close'].iloc[-1])

    def __del__(self):
        self.foreign = self._conf = self.rc = self._ae = self.__conn = None
        self.data = self.view = self._date = self._close = None
        del(self.foreign, self._conf, self.rc, self._ae, self.__conn)
        del(self.data, self.view, self._date, self._close)

    def __call__(self, date=None):
        if date is None:
            date = self._date
        return self.maverick(date=date, unbound=False)

    def __str__(self):
        delta = round(self.data['Close'].diff().iloc[-1] / self.data['Close'].iloc[-2] * 100, 3)
        return f"#{self.code} close @ {self._close:.02f} ({delta}%) on {self._date:'%Y-%m-%d'}"

    def insert(self, values, conditions):
        return self._ae.append(values, conditions)

    def delete(self, conditions):
        return self._ae.remove(conditions)

    def update(self, values, conditions):
        return self._ae.amend(values, conditions)

    def acquire(self, conditions, ):
        return self._ae.acquire(conditions)

    def fetch(
            self, code=None, start=None, table=pref.db['Equities']['table'],
            exclude=pref.db['Equities']['exclude'], years=4, adhoc=False):
        import yfinance as yf
        if not start:
            # start = pd.datetime(pd.datetime.now().year - years, 12, 31)
            start = datetime(datetime.now().year - years, 12, 31)
        if code:
            if isinstance(code, (int, float)):
                code = int(code)
            if code not in [
                    _ for _ in entities(self._conf['name'])
                    if _ not in exclude]:
                adhoc = True
        if adhoc:
            while adhoc:
                try:
                    _code = code.upper()
                    __ = yf.download(_code, start, group_by='ticker')
                    self.foreign = True
                except Exception:
                    _code = f'{code:04d}.HK'
                    __ = yf.download(_code, start, group_by='ticker')
                if len(__):
                    adhoc = not adhoc
                else:
                    print('Retry in 30 seconds')
                    sleep(30)
            __.xs(_code, axis=1, level='Ticker')
            # __.drop(columns=['Adj Close'])
        else:
            # from alchemy import AS
            # fields = ['date'] + pref.fields
            # sfc = ', '.join([f'{_} as {_.capitalize()}' for _ in fields])
            # qtext = f"SELECT {sfc} FROM records WHERE eid={code:d} AND \
            #         date>{start:'%Y-%m-%d'}"
            # conn = AS(self._conf['name']).connect
            # __ = pd.read_sql(
            #         qtext, conn, index_col='Date', parse_dates=['Date'])
            __ = trade_data(code, False)
            __ = __[__.index > start]
            __.columns = [_.capitalize() for _ in __.columns]
            __.index.name = __.index.name.capitalize()
        return __

    def mas(self, date=None, period=periods):
        if date is None:
            date = self._date
        return self.view.mas(self.data, period, date)

    def idrs(self, date=None, period=periods):
        if date is None:
            date = self._date
        return self.view.idrs(self.data, period. date)

    def sma(self, date=None, period=periods['simple'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.ma(
            self.data, period, 's', req_field, date).map(hsirnd)

    def ema(self, date=None, period=periods['simple'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.ma(
            self.data, period, 'e', req_field, date).map(hsirnd)

    def wma(self, date=None, period=periods['simple'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.ma(
            self.data, period, 'w', req_field, date).map(hsirnd)

    def kama(self, date=None, period=periods['kama'], req_field='c'):
        if date is None:
            date = self._date
        return self.view.kama(self.data, period, req_field, date).map(hsirnd)

    def atr(self, date=None, period=periods['atr']):
        if date is None:
            date = self._date
        return self.view.atr(self.data, period, date)

    def rsi(self, date=None, period=periods['rsi']):
        if date is None:
            date = self._date
        return self.view.rsi(self.data, period, date)

    def obv(self, date=None):
        if date is None:
            date = self._date
        return self.view.obv(self.data, date)

    def ovr(self, date=None, period=periods):
        if date is None:
            date = self._date
        return self.view.ovr(self.data, period, date)

    def vwap(self, date=None):
        if date is None:
            date = self._date
        return self.view.vwap(self.data, date)

    def bb(self, date=None, period=periods['simple']):
        if date is None:
            date = self._date
        return self.view.bb(self.data, period, date)

    def apz(self, date=None, period=periods['apz']):
        if date is None:
            date = self._date
        return self.view.apz(self.data, period, date)

    def dc(self, date=None, period=periods['dc']):
        if date is None:
            date = self._date
        return self.view.dc(self.data, period, date)

    def kc(self, date=None, period=periods['kc']):
        if date is None:
            date = self._date
        return self.view.kc(self.data, period, date)

    def macd(self, date=None, period=periods['macd']):
        if date is None:
            date = self._date
        return self.view.macd(self.data, period, date)

    def adx(self, date=None, period=periods['adx']):
        if date is None:
            date = self._date
        return self.view.adx(self.data, period, date)

    def soc(self, date=None, period=periods['soc']):
        if date is None:
            date = self._date
        return self.view.soc(self.data, period, date)

    def stc(self, date=None, period=periods['stc']):
        if date is None:
            date = self._date
        return self.view.stc(self.data, period, date)

    def gat(self, date=None, period=periods['atr']):
        if date is None:
            date = self._date
        if self.foreign:
            return self.view.gat(self.data, period, date).round(2).unique()
        return self.view.gat(self.data, period, date).apply(hsirnd, 1).unique()

    def maverick(self, date=None, period=periods, unbound=True, exclusive=True):
        if date is None:
            date = self._date
        return self.view.maverick(self.data, period, date, unbound, exclusive)


def bqo(el, action='buy', bound=True, adhoc=False):
    dl, il = [], []
    if isinstance(el, (int, str)):
        el = [el]
    for _ in el:
        if isinstance(_, int):
            pE = pref.db['Equities']
            if _ in [
                    __ for __ in entities(pE['name'])
                    if __ not in pE['exclude']]:
                o_ = Equities(_, adhoc)
        if isinstance(_, str):
            pF = pref.db['Futures']
            if _.upper() in entities(pF['name']):
                o_ = Futures(_.upper())
        val = o_.maverick(unbound=True).T[action][o_._date]
        if bound:
            val = o_().T[action][o_._date]
        dl.append(val)
        il.append(o_.code)
    _ = pd.DataFrame({action: dl}, index=il)
    return _.T


def entities(dbname: str = None):
    if dbname is None:
        dbname = YAML.db.Equities.name
    if dbname == YAML.db.Equities.name:
        query = select(Securities.eid.distinct()).order_by(db.asc(Securities.eid))
        __ = sync_fetch(query, dbname)
        exclude = YAML.db.Equities.exclude
        res =  [_[0] for _ in __ if _[0] not in exclude]
    if dbname == YAML.db.Futures.name:
        query = select(Derivatives.code.distinct()).order_by(db.asc(Derivatives.code))
        __ = sync_fetch(query, dbname)
        res =  [_[0] for _ in __]
    return res

"""
def entities(dbname=None, series=False):
    pF, pE = pref.db['Futures'], pref.db['Equities']
    if not dbname:
        dbname = pE['name']
    engine = db.create_engine(f'sqlite+pysqlite:///{filepath(dbname)}')
    meta, conn = db.MetaData(), engine.connect()
    if dbname == pF['name']:
        rc = db.Table(
            pF['table'],
            meta,
            autoload=True,
            autoload_with=engine).columns
        query = db.select([rc.code.distinct()]).order_by(db.asc(rc.code))
    if dbname == pE['name']:
        rc = db.Table(
            pE['table'],
            meta,
            autoload=True,
            autoload_with=engine).columns
        query = db.select([rc.eid.distinct()]).order_by(db.asc(rc.eid))
    # __ = [_[0] for _ in conn.execute(query).fetchall()]
    res = [
            __ for __ in [_[0] for _ in conn.execute(query).fetchall()]
            if __ not in pE['exclude']]
    if series:
        return pd.Series(res)
    return res
"""

def compose(code=None):
    def grab(c):
        e = Equities(c)
        rd = e.data
        pdhr = pd.concat(
            [e.rsi(), rd.High.sub(rd.Low),
                rd.Close.diff(), e.atr(),
                e.adx()[f"ADX{pref.periods['Equities']['adx']}"].diff()],
            axis=1)
        pdhr.columns = ['RSI', 'dHL', 'dpC', 'ATR', 'dADX']
        return pdhr

    if code is None:
        code = entities(pref.db['Equities']['name'])
    if isinstance(code, (int, float)):
        code = [int(code)]
    if isinstance(code, list):
        try:
            code = [int(_) for _ in code]
        except Exception:
            pass
        return pd.concat(
            [grab(_) for _ in tqdm(code)],
            keys=code,
            names=['Code', 'Data'],
            axis=1)


def strayed(df, date, buy=True):
    if isinstance(date, str):
        try:
            date = datetime.strptime(date, "%Y%m%d")
        except Exception:
            pass
    if isinstance(date, datetime):
        txr = df.reorder_levels(['Data', 'Code'], 1)
        rtr = txr.loc[date, 'RSI']
        hdr = []
        if buy:
            rl = rtr[
                (rtr < (1 - 1 / gr) * 100) &
                (txr.loc[date, 'dpC'].abs() > txr.loc[date, 'ATR'])
                ].index.tolist()
            if rl:
                hdr.extend([
                    Equities(_).maverick(
                        date=date, unbound=False).loc["buy", date]
                    for _ in tqdm(rl)])
                return pd.Series(hdr, index=rl, name='buy')
        else:
            rl = rtr[
                (rtr > 1 / gr * 100) &
                (txr.loc[date, 'dpC'] > txr.loc[date, 'ATR'])].index.tolist()
            if rl:
                hdr.extend([
                    Equities(_).maverick(
                        date=date, unbound=False).loc["sell", date]
                    for _ in tqdm(rl)])
                return pd.Series(hdr, index=rl, name='sell')


def _press(__):
    _ = Equities(__)
    return f'\n{_}\n{_()}\n{_.gat()}'


def adhoc(__):
    _ = Equities(__, True)
    print(f'{_}\n{_()}\n{_.gat()}')


def summary(__):
    if __ is None:
        __ = entities()
    if not isinstance(__, (list, tuple)):
        __ = list(__)
    _ = [_press(_) for _ in __]
    # print('\n'.join(_))
    return '\n'.join(_)


def A2B(_):
    _ = _.upper()
    if _ in B_scale.keys():
        __ = Equities(_)
        r = __.gat()
        return [hsirnd(x * B_scale[_] * USHK) for x in [
            r[0],
            r[1],
            __._close,
            r[-2],
            r[-1]]]
