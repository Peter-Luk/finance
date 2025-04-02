""" async sqlalchemy class (sqlite) trial """
import asyncio
import datetime
import random
from typing import Any, Coroutine, Iterable, List, Dict, Final, Union
from pathlib import os
import pandas as pd
import yfinance as yf
import numpy as np
from pprint import pprint
from tqdm import tqdm
from tqdm.asyncio import tqdm as atq
from asyncinit import asyncinit
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.future import select
from sqlalchemy import Column, Integer, Float, Date, text
from fintools import FOA, get_periods, hsirnd
from utilities import filepath, gslice, getcode
from ormlib import async_fetch
from pandasql import sqldf
from benedict import benedict
from finaux import roundup
from nta import Viewer
from pref import fields

YAML_PREFERENCE: Final[str] = 'pref.yaml'
YAML: Final[str] = benedict.from_yaml(f"{os.getenv('PYTHONPATH')}{os.sep}{YAML_PREFERENCE}")
Base = declarative_base()
DB_NAME: Final[str] = YAML.db.Equities.name
DB_PATH: Final[str] = filepath(DB_NAME)


async def get_data(
        ticker: Any = 9988,
        boarse: str = 'HKEx',
        capitalize: bool = False
        ) -> pd.DataFrame:
    _ = getcode(ticker, boarse)
    df = yf.download(_, interval='1d', period='max')
    return df.xs(_, axis=1, level='Ticker')


class Record(Base):
    """ basic sqlalchemy record object """
    __tablename__ = YAML.db.Equities.table
    id = Column(Integer, primary_key=True)
    eid = Column(Integer)
    _create_at = datetime.datetime.now()
    date = Column(
            Date,
            default=_create_at.date(),
            server_default=text(str(_create_at.date()))
            )
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)


async def stored_entities() -> List:
    """ fetching stored records """
    sql_stmt = select(Record.eid.distinct()).order_by(Record.eid)
    entities = [_[0] for _ in await async_fetch(sql_stmt, DB_NAME) if _ not in YAML.db.Equities.exclude]
    return entities

STORED: Final[List] = asyncio.run(stored_entities())
fields = ['date']
fields.extend(YAML.fields)
data_fields = [eval(f"Record.{_}") for _ in fields]
param = YAML.periods.Equities
INDICATORS: Final[List] = YAML.indicators
b_scale = YAML.B_scale


@asyncinit
# class Equity(FOA):
# class Equity(Viewer):
class Equity(FOA, Viewer):
    """ base object (trial) """
    async def __init__(
            self,
            code: Union[int, str],
            boarse: str = 'HKEx',
            static: bool = True,
            capitalize: bool = True
            ) -> Coroutine:
        self.periods = get_periods()
        self.code = code
        self.exchange = boarse
        match self.exchange:
            case 'HKEx':
                self.currency = 'HKD'
            case 'NYSE':
                self.currency = 'USD'
            case 'DAX':
                self.currency = 'EUR'
            case 'TSE':
                self.currency = 'JPY'
            case 'LSE':
                self.currency = 'GBP'
        self.yahoo_code = getcode(self.code, self.exchange)
        self.__capitalize = capitalize
        await self.compose(static)
        # self.view = Viewer(self.__data)

    def __str__(self):
        return f"{self.yahoo_code} {self.date:%d-%m-%Y}: close @ {self.currency} {self.close:,.2f} ({self.change:0.3%}), rsi: {self.rsi().iloc[-1]:0.3f}, sar: {self.sar().iloc[-1]:,.2f} and KAMA: {self.kama().iloc[-1]:,.2f}"

    def __call__(self, static: bool = True) -> pd.Series:
        return self.maverick(date=self.date) if self.__capitalize else self.optinum(date=self.date)
        # return self.__data

    async def compose(self, static: bool) -> pd.DataFrame:
        if self.exchange == 'HKEx':
            if static:
                if self.code in STORED:
                    today = datetime.date.today()
                    start = datetime.date(today.year - random.choice(range(5, 16)), 1, 1)
                    self.__data = pd.DataFrame(
                            await self.fetch(start),
                            columns=fields
                            )
                    if self.__capitalize:
                        self.__data.columns = [_.capitalize() for _ in self.__data.columns]
                        pd.DatetimeIndex(self.__data.Date)
                    else:
                        pd.DatetimeIndex(self.__data.date)
                else:
                    self.__data = await get_data(self.code, self.exchange, self.__capitalize)
            else:
                self.__data = await get_data(self.code, self.exchange, self.__capitalize)
        else:
            self.__data = await get_data(self.code, self.exchange, self.__capitalize)
        self.date = self.__data.index[-1]
        if self.__capitalize:
            self.change = self.__data.Close.pct_change(fill_method=None).iloc[-1]
            self.close = self.__data.Close.iloc[-1]
        else:
            self.change = self.__data.close.pct_change(fill_method=None).iloc[-1]
            self.close = self.__data.close.iloc[-1]
        super().__init__(self.__data, float)

    async def fetch(self, start: datetime.date) -> List:
        """ fetch records from local db """
        sql_stmt = select(*data_fields).filter(Record.eid == self.code).filter(
                Record.date >= start)
        return await async_fetch(sql_stmt, DB_NAME)

# """
    def sma(self, period: int = param.get('simple'), data: Any = None) -> pd.DataFrame:
        # sma 
        return super().sma(period, data).astype('float64')

    def ema(self, period: int = param.get('simple'), data: Any = None) -> pd.DataFrame:
        #  ema 
        return super().ema(period, data).astype('float64')

    def atr(self, period: int = param.get('atr'), data: Any = None) -> pd.DataFrame:
        #  atr 
        return super().atr(period, data).astype('float64')

    def kama(self, period: Dict = param.get('kama'), data: Any = None) -> pd.DataFrame:
        return super().kama(period, data).astype('float64')

    def adx(self, period: int = param.get('adx'), astype: str = 'float64') -> pd.DataFrame:
        #  adx 
        return super().adx(period).astype(astype)

    def apz(self, period: int = param.get('apz'), astype: str = 'float64') -> pd.DataFrame:
        #  apz 
        return super().apz(period).astype(astype)

    def dc(self, period: int = param.get('dc'), astype: str = 'float64') -> pd.DataFrame:
        #  dc 
        return super().dc(period).astype(astype)

    def kc(self, period: Dict = param.get('kc'), astype: str = 'float64') -> pd.DataFrame:
        #  kc 
        return super().kc(period).astype(astype)

    def obv(self, astype: str = 'float64') -> pd.DataFrame:
        #  obv 
        return super().obv().astype(astype)

    def macd(self, period: Dict = param.get('macd'), astype: str = 'float64') -> pd.DataFrame:
        #  macd 
        return super().macd(period).astype(astype)

    def rsi(self, period: int = param.get('rsi'), astype: str = 'float64') -> pd.DataFrame:
        #  rsi 
        return super().rsi(period).astype(astype)

    def sar(self, period: Dict = param.get('sar'), astype: str = 'float64') -> pd.DataFrame:
        #  ta-lib sar 
        return super().sar(
                period.get('acceleration'),
                period.get('maximum')
                ).astype(astype)

    def stc(self, period: Dict = param.get('stc'), astype: str = 'float64') -> pd.DataFrame:
        #  stc 
        return super().stc(period).astype(astype)

    def soc(self, period: Dict = param.get('soc'), astype: str = 'float64') -> pd.DataFrame:
        #  soc 
        return super().soc(period).astype(astype)

    def maverick(self,
            date=None,
            period=param,
            # period=yaml_get('periods',
            #     YAML_PREFERENCE).get('Equities'),
            unbound=True,
            exclusive=True):
        if date is None:
            date = self.date
        return self.view.maverick(self.__data, period, date, unbound, exclusive)

    def optinum(self, date: str = None,
                base: pd.DataFrame = None,
                delta: pd.DataFrame = None) -> pd.DataFrame:
        if date is None:
            date = self.date
        if base is None:
            try:
                base = self.__data.query(f'date <= "{date}"')
            except:
                base = self.__data.query(f'Date <= "{date}"')
        if delta is None:
            delta = self.atr(self.periods['atr'])

        def _patr(raw, date):
            lc, lr = raw.close.loc[date], delta.loc[date]
            _ = np.arange(lc - lr, lc + lr, lr).tolist()
            _.extend(gslice([lc + lr, lc]))
            _.extend(gslice([lc, lc - lr]))
            return _

        def _pgap(pivot, raw):
            gap = pivot - raw.close.iloc[-1]
            _ = gslice([pivot + gap, pivot])
            _.extend(gslice([pivot, pivot - gap]))
            return _
        hdr = []
        res = pd.DataFrame(dict(
            Buy=pd.Series([]),
            Sell=pd.Series([])))
        try:
            [hdr.extend(_pgap(_, base)) for _ in _patr(base, date)]
            hdr.sort()

            kc = self.kc(self.periods['kc']).loc[date]
            buy = [round(_, 2) for _ in hdr if _ < kc.Lower and _ < self.__data.close.loc[date]]
            sell = [round(_, 2) for _ in hdr if _ > kc.Upper and _ > self.__data.close.loc[date]]
            if self.exchange == 'TSE':
                buy = [round(_) for _ in hdr if _ < kc.Lower and _ < self.__data.close.loc[date]]
                sell = [round(_) for _ in hdr if _ > kc.Upper and _> self.__data.close.loc[date]]
            if self.exchange == 'HKEx':
                buy = [roundup(_) for _ in hdr if _ < kc.Lower and _ < self.__data.close.loc[date]]
                sell = [roundup(_) for _ in hdr if _ > kc.Upper and _ > self.__data.close.loc[date]]

            res['Buy'] = pd.Series(buy).unique().tolist() if buy else np.NaN
            res['Sell'] = pd.Series(sell).unique().tolist() if sell else np.NaN
        except:
            pass
        return res
# """


param = YAML.periods.Futures
class Futures(FOA, Viewer):
    def __init__(self, code):
        self.code = code.upper()
        self.compose()
        self.periods = YAML.periods.Futures
        self.close = self.__data.Close.iloc[-1]
        
    def compose(self):
        db_name = YAML.db.Futures.name
        engine = create_engine(f"sqlite+pysqlite:///{filepath(db_name)}")
        raw_data = pd.read_sql(f"SELECT date, session, open, high, low, close, volume  FROM records WHERE code='{self.code}'",
                engine, index_col=['date', 'session'],
                parse_dates={'date': {format: '%Y-%m-%d'}})
        trade_date = sqldf("SELECT DISTINCT date FROM raw_data").date.tolist()
        res = {'Date': [datetime.datetime.strptime(_, '%Y-%m-%d') for _ in trade_date]}
        _open, _high, _low, _close, _volume = [], [], [], [], []

        for td in trade_date:
            hdr = raw_data.xs(td, axis=0, level='date')
            if len(hdr) == 1:
                _open.append(hdr.open.iloc[0])
                _high.append(hdr.high.iloc[0])
                _low.append(hdr.low.iloc[0])
                _close.append(hdr.close.iloc[0])
                _volume.append(hdr.volume.iloc[0])
            else:
                _open.append(hdr.open['M'])
                _high.append(hdr.high.max())
                _low.append(hdr.low.min())
                _close.append(hdr.close['A'])
                _volume.append(hdr.volume.sum())
        res['Open'] = _open
        res['High'] = _high
        res['Low'] = _low
        res['Close'] = _close
        res['Volume'] = _volume

        df = pd.DataFrame.from_dict(res, orient='columns')
        df = df.set_index(pd.DatetimeIndex(df['Date']))
        df.drop(['Date'], axis=1, inplace=True)
        self.__data = df
        super().__init__(self.__data, float)

    def sma(self, period: int = param.simple, data: Any = None) -> pd.DataFrame:
        # sma 
        return super().sma(period, data).astype('float64')

    def ema(self, period: int = param.simple, data: Any = None) -> pd.DataFrame:
        #  ema 
        return super().ema(period, data).astype('float64')

    def atr(self, period: int = param.atr, data: Any = None) -> pd.DataFrame:
        #  atr 
        return super().atr(period, data).astype('float64')

    def kama(self, period: Dict = param.kama, data: Any = None) -> pd.DataFrame:
        return super().kama(period, data).astype('float64')

    def adx(self, period: int = param.adx, astype: str = 'float64') -> pd.DataFrame:
        #  adx 
        return super().adx(period).astype(astype)

    def apz(self, period: int = param.apz, astype: str = 'float64') -> pd.DataFrame:
        #  apz 
        return super().apz(period).astype(astype)

    def dc(self, period: int = param.dc, astype: str = 'float64') -> pd.DataFrame:
        #  dc 
        return super().dc(period).astype(astype)

    def kc(self, period: Dict = param.kc, astype: str = 'float64') -> pd.DataFrame:
        #  kc 
        return super().kc(period).astype(astype)

    def obv(self, astype: str = 'float64') -> pd.DataFrame:
        #  obv 
        return super().obv().astype(astype)

    def macd(self, period: Dict = param.macd, astype: str = 'float64') -> pd.DataFrame:
        #  macd 
        return super().macd(period).astype(astype)

    def rsi(self, period: int = param.rsi, astype: str = 'float64') -> pd.DataFrame:
        #  rsi 
        return super().rsi(period).astype(astype)

    def sar(self, period: Dict = param.sar, astype: str = 'float64') -> pd.DataFrame:
        #  ta-lib sar 
        return super().sar(
                period.get('acceleration'),
                period.get('maximum')
                ).astype(astype)

    def stc(self, period: Dict = param.stc, astype: str = 'float64') -> pd.DataFrame:
        #  stc 
        return super().stc(period).astype(astype)

    def soc(self, period: Dict = param.soc, astype: str = 'float64') -> pd.DataFrame:
        #  soc 
        return super().soc(period).astype(astype)


async def fetch_data(entities: Iterable, indicator: str = '') -> zip:
    """ fetch entities stored records """
    async def get(code: int) -> Coroutine:
        return await Equity(code)

    ent_list = [get(_) for _ in entities]
    name_list = [f'{_:04d}.HK' for _ in entities]
    # name_list = [f'{_:04d}.HK' for _ in entities if _ in STORED]

    result = await atq.gather(*ent_list)
    res_list = [eval(f"_.{indicator}()") for _ in tqdm(result)] \
        if (indicator in INDICATORS) else result
    return zip(name_list, res_list)


async def get_summary(code: str, boarse: str) -> str:
    _ = await Equity(code, boarse)
    return f'{_}'
    # return {_.yahoo_code: f'{_}'}
    # print(f'{_.yahoo_code} - {_}')


async def close_at(code: str, boarse: str) -> dict:
    _ = await Equity(code, boarse, static=False)
    return {_.code: _.close}


async def summary(target: str = 'nato_defence') -> list:
    match target:
        case 'nato_defence':
            subject = eval(f'YAML.listing.{target}')
        case 'B_shares':
            _ = YAML.B_scale.keys()
            subject = dict(zip(_, ['NYSE' for __ in _]))
        case 'TSE':
            _ = eval(f'YAML.prefer_stock.{target}.keys()')
            subject = dict(zip(_, [target for __ in _]))
    return [await f for f in atq.as_completed([get_summary(c, b) for c, b in list(subject.items())])]


async def daily_close(
        client_no: str,
        boarse: str = 'HKEx'):
    result = {}
    PORTFOLIO = benedict.from_yaml(f"{os.getenv('PYTHONPATH')}{os.sep}portfolio.yaml")
    _ = eval(f'PORTFOLIO.client_no.{boarse}')
    subject = dict(zip(_, [boarse for __ in _]))
    for f in asyncio.as_completed([close_at(c, b) for c, b in tqdm(list(subject.items()))]):
        holder = await f
        result[list(holder.keys()).pop()] = list(holder.values()).pop()
    res = ['Close price']
    keys = list(result.keys())
    keys.sort()
    for _ in keys:
        v_for = '0.2' if result[_] > .5 else '0.3'
        res.append(f'{_} {result[_]:{v_for}f}')
    print(f'{client_no}\n' + ', '.join(res))


async def A2B(
        entities: Iterable = b_scale.keys(),
        date: str = None) -> dict:

    async def get(
            code: str) -> tuple:
        return (code, await Equity(code, boarse='NYSE', capitalize=False))

    xhg = YAML.USHK
    code_ = []
    opt_ = []
    ent_ = [get(_) for _ in entities]
    res = dict(await atq.gather(*ent_))

    for k, v in res.items():
        yk = b_scale.get(k)
        ratio = yk.get('ratio')
        code_.append(f'{yk.get("code")}.HK')
        hdr = v.maverick(date)
        for k in hdr.keys():
            hdr[k] = hdr[k] if len(hdr[k]) == 0 else (hdr[k] * xhg * ratio).apply(hsirnd)
        opt_.append(hdr)

    return dict(zip(code_, opt_))


if __name__ == "__main__":
    sector = input('Sector: ')
    _ = asyncio.run(summary(sector))
    pprint(_, width=50)
