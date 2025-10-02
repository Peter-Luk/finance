""" async sqlalchemy class (sqlite) trial """
import asyncio
import datetime
import random
from pathlib import os, functools
from rich import print
# from rich.console import Console
from rich.table import Table
import pandas as pd
import yfinance as yf
# import shutil
import numpy as np
from typing import Any, Coroutine, Iterable, List, Dict, Final, Union
# from functools import partial
# from pprint import pprint
from tqdm import tqdm
from tqdm.asyncio import tqdm as atq
from asyncinit import asyncinit
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.future import select
from sqlalchemy import Column, Integer, Float, Date, text
from fintools import FOA, hsirnd, construct_report
from utilities import filepath, gslice, getcode, YAML, PORTFOLIO_PATH
from ormlib import async_fetch
from benedict import benedict
from finaux import roundup
from nta import Viewer

Base = declarative_base()
DB_NAME: Final[str] = YAML.db.Equities.name
DB_PATH: Final[str] = filepath(DB_NAME)


async def get_data(
        ticker: Any = 9988,
        boarse: str = 'HKEx',
        capitalize: bool = False
        ) -> pd.DataFrame:
    _ = getcode(ticker, boarse)
    df = yf.download(_, interval='1d', period='max', auto_adjust=False)
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
        self.periods = YAML.periods.Equities
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
        self.view = Viewer(self.__data)

    def __str__(self):
        latest = functools.partial(self.summary, date=self.date, return_type='str')
        return latest()

    def summary(self, date: datetime.datetime,
            return_type: str='rich'):
        close = self.__data.Close
        change = close.pct_change(fill_method=None)
        if return_type == 'rich':
        # console = Console()
            table = Table(title=f"{self.yahoo_code} {date:%Y-%m-%d} ({self.currency})")
            table.add_column("Close", style="cyan")
            table.add_column("RSI", justify="right", style="magenta")
            table.add_column("SAR", justify="right", style="magenta")
            table.add_column("KAMA", justify="right", style="magenta")
            table.add_row(f"{close[date]:,.2f} ({change[date]:+0.3%})",
                f"{self.rsi()[date]:0.3f}",
                f"{self.sar()[date]:,.2f}",
                f"{self.kama()[date]:,.2f}")
            return table
        # console.print(table)
        if return_type == 'str':
            return f"{self.yahoo_code} {date:%Y-%m-%d}: close @ {self.currency} {close[date]:,.2f} ({change[date]:+0.3%}), rsi: {self.rsi()[date]:0.3f}, sar: {self.sar()[date]:,.2f} and KAMA: {self.kama()[date]:,.2f}"

    def __call__(self, date=None) -> pd.Series:
        date = self.date if (date is None) else date
        return self.maverick(date) if self.__capitalize else self.optinum(date)
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
        self.trade_date = self.__data.index
        self.date = self.trade_date[-1]
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
                period.acceleration,
                period.maximum
                ).astype(astype)

    def stc(self, period: Dict = param.stc, astype: str = 'float64') -> pd.DataFrame:
        #  stc 
        return super().stc(period).astype(astype)

    def soc(self, period: Dict = param.soc, astype: str = 'float64') -> pd.DataFrame:
        #  soc 
        return super().soc(period).astype(astype)

    def gat(self, date=None, period=param.atr):
        date = self.date if (date is None) else date
        return self.view.gat(self.__data, period, date).apply(hsirnd).unique()

    def neighbour(self,
            citeria:np.float64|None = None,
            date:datetime.datetime|None = None,
            direction:str = 'up') -> Iterable:
        if date is None:
            date = self.date
        if citeria is None:
            citeria = self.__data.Close[date]
        index = self.trade_date.tolist().index(date)
        _ = self.gat(self.trade_date[index - 1])
        result = iter(_[_ > nearest(_, citeria)])
        if direction == 'down':
            result = iter(_[_ < nearest(_, citeria)][::-1])
        return result

    def maverick(self,
            date=None,
            period=param,
            unbound=True,
            exclusive=True):
        date = self.date if (date is None) else date
        return self.view.maverick(self.__data, period, date, unbound, exclusive)

    def optinum(self, date: str = None,
                base: pd.DataFrame = None,
                delta: pd.DataFrame = None) -> pd.DataFrame:
        date = self.date if (date is None) else date
        if base is None:
            try:
                base = self.__data.query(f'date <= "{date}"')
            except Exception:
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
        except Exception:
            pass
        return res


param = YAML.periods.Futures
# @asyncinit
class Futures(FOA, Viewer):
    def __init__(self, code):
        self.code = code.upper()
        # await self.compose()
        self.compose()
        self.periods = YAML.periods.Futures
        self.view = Viewer(self.__data)
        self.change = 0
        if self.__data.shape[0] > 1:
            self.change = self.__data.Close.diff(1).iloc[-1] / self.__data.Close.iloc[-2]
        self.trade_date = self.__data.index
        self.date = self.trade_date[-1]
        # self.date = self.__data.index[-1]
        self.close = self.__data.Close.iloc[-1]
        # qstr = select(*[text(_) for _ in fields]).select_from(text('records')).where(text(f"code='{self.code}'"))

    def __str__(self):
        return f"{self.code} {self.date:%d-%m-%Y}: close @ {self.close:d} ({self.change:0.3%}), rsi: {self.rsi().iloc[-1]:0.3f}, SAR: {round(self.sar().iloc[-1], 0)} and KAMA: {round(self.kama().iloc[-1], 0)}"

    def __call__(self, date=None):
        if date is None:
            date = self.date
        _ = self.maverick(date=date, unbound=False)
        _.name = self.code
        return _
        
    def compose(self):
        def fetch(fields, code, name):
            engine = create_engine(f"sqlite+pysqlite:///{filepath(name)}")
            qstr = select(*[text(_) for _ in fields]).select_from(text('records')).where(text(f"code='{code}'"))
            return pd.read_sql(qstr, engine, index_col=['date', 'session'], parse_dates={'date': {format: '%Y-%m-%d'}})

        db_name = YAML.db.Futures.name
        fields = ['date', 'session']
        fields.extend(YAML.fields)
        raw_data = fetch(fields, self.code, db_name)
        trade_date = raw_data.index.get_level_values('date').unique().tolist()
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
                period.acceleration,
                period.maximum
                ).astype(astype)

    def stc(self, period: Dict = param.stc, astype: str = 'float64') -> pd.DataFrame:
        #  stc 
        return super().stc(period).astype(astype)

    def soc(self, period: Dict = param.soc, astype: str = 'float64') -> pd.DataFrame:
        #  soc 
        return super().soc(period).astype(astype)

    def gat(self, date=None, period=param.atr):
        date = self.date if (date is None) else date
        return self.view.gat(self.__data, period, date).apply(hsirnd).unique()

    def neighbour(self,
            citeria:np.float64|None = None,
            date:datetime.datetime|None = None,
            direction:str = 'up') -> Iterable:
        if date is None:
            date = self.date
        if citeria is None:
            citeria = self.__data.Close[date]
        index = self.trade_date.tolist().index(date)
        _ = self.gat(self.trade_date[index - 1])
        result = iter(_[_ > nearest(_, citeria)])
        if direction == 'down':
            result = iter(_[_ < nearest(_, citeria)][::-1])
        return result

    def maverick(self, date=None, period=param, unbound=True, exclusive=True):
        date = self.date if (date is None) else date
        return self.view.maverick(self.__data, period, date, unbound, exclusive)


def main(target: str) -> None:
# def main(target: str) -> str:
    # result = []
    tp = []
    match target:
        case 'nato_defence':
            el = [getcode(k, boarse=v) for k, v in YAML.listing.nato_defence.items()]
        case 'B_shares':
            el = list(YAML.B_scale.keys())
            tp = yf.download(el, interval='1d', period='max', threads=True, group_by='ticker', auto_adjust=False)
        case 'TSE':
            tse = functools.partial(getcode, boarse=target)
            el = [tse(k) for k in YAML.prefer_stock.TSE.keys()]
            tp = yf.download(el, interval='1d', period='max', threads=True, group_by='ticker', auto_adjust=False)

    if type(tp) is list:
        for item in el:
        # for item in tqdm(el):
            tp = yf.download(item, interval='1d', period='max', threads=True, group_by='ticker', auto_adjust=False)
            print(construct_report(tp, item))
            # result.append(construct_report(tp, item))
    else:
        try:
            for item in el:
                print(construct_report(tp, item))
            # result = [construct_report(tp, item) for item in tqdm(el)]
        except ValueError:
            print(f'Content {tp} not recognized')
    # return os.linesep.join(result)


async def fetch_data(entities: Iterable, indicator: str = '') -> zip:
    """ fetch entities stored records """
    async def get(code: int) -> Coroutine:
        return await Equity(code)

    ent_list = [get(_) for _ in entities]
    name_list = [f'{_:04d}.HK' for _ in entities]

    result = await atq.gather(*ent_list)
    res_list = [eval(f"_.{indicator}()") for _ in tqdm(result)] \
        if (indicator in INDICATORS) else result
    return zip(name_list, res_list)


async def get_summary(code: str, boarse: str) -> str:
    _ = await Equity(code, boarse)
    return f'{_}'


async def close_at(code: str, boarse: str) -> dict:
    _ = await Equity(code, boarse, static=False)
    return {_.code: _.close}


async def daily_close(
        client_no: str,
        boarse: str = 'HKEx'):
    result = {}
    portfolio = benedict.from_yaml(PORTFOLIO_PATH)
    _ = portfolio.client_no.boarse
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


async def summary(entities: Iterable,
        boarse: str = 'HKEx') -> None:
    res = []
    _ = functools.partial(Equity, boarse=boarse,
            static=False)
    holder = {getcode(__, boarse): await _(__) for __ in entities}
    res = [f'{k}:{os.linesep}{v.gat()}' for k, v in holder.items()]
    print(os.linesep.join(res))


async def adhoc(entity: str, date=None) -> None:
    _ = await Equity(entity, static=False)
    date = _.date if (date is None) else date
    print(_.summary(date))
    print(f'{_(date)}\n{_.gat(date)}')
    # print(f'{_.summary(date)}\n{_(date)}\n{_.gat(date)}')


async def A2B(entity: str = None) -> Iterable:
    entity = entity.upper()
    if entity in YAML.B_scale.keys():
        __ = await Equity(entity, boarse='NYSE')
        r = __.gat()
        return [hsirnd(x * YAML.B_scale[entity].ratio * YAML.USHK) for x in [
            r[0],
            r[1],
            __.close,
            r[-2],
            r[-1]]]


nyse = functools.partial(Equity, boarse='NYSE', static=False)
tse = functools.partial(Equity, boarse='TSE', static=False)
lse = functools.partial(Equity, boarse='LSE', static=False)
dax = functools.partial(Equity, boarse='DAX', static=False)
hkex = functools.partial(Equity, boarse='HKEx', static=False)

def nearest(n_range: np.ndarray, citeria: np.float64) -> np.float64:
    _ = abs(n_range / citeria - 1)
    return n_range[_.tolist().index(_.min())]

if __name__ == "__main__":
    import pzp
    sectors = ['nato_defence', 'B_shares', 'TSE']
    select_item = pzp.pzp(sectors)
    # eval(f"main('{select_item}')")
    print(eval(f"main('{select_item}')"))
    # columns, __ = shutil.get_terminal_size()
    # pprint(main(sector), width=columns)
