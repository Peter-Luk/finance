""" async sqlalchemy class (sqlite) trial """
import asyncio
import datetime
import random
from typing import Any, Coroutine, Iterable, List, Dict, Final
import pandas as pd
import numpy as np
# import yfinance as yf
from tqdm import tqdm
from tqdm.asyncio import tqdm as atq
from asyncinit import asyncinit
from sqlalchemy.orm import declarative_base
from sqlalchemy.future import select
from sqlalchemy import Column, Integer, Float, Date, text
from finance.fintools import FOA, get_periods, hsirnd
from finance.utilities import yaml_get, yaml_db_get, filepath, gslice
from finance.ormlib import async_fetch
from finance.finaux import roundup
from nta import Viewer
from asyahoo import get_data

YAML_PREFERENCE: Final[str] = 'pref.yaml'
Base = declarative_base()


# def yaml_get(field: str, file: str) -> Any:
#     """ Basic yaml config reader """
#     if file.split('.')[-1] == 'yaml':
#         import yaml
#         fpaths = [os.getcwd()]
#         fpaths.extend(PYTHON_PATH)
#         for f_p in fpaths:
#             _f = f'{f_p}{sep}{file}'
#             if os.path.isfile(_f):
#                 with open(_f, encoding='utf-8') as y_f:
#                     _ = yaml.load(y_f, Loader=yaml.FullLoader)
#                 res = _.get(field)
#     return res
#
#
# def yaml_db_get(
#         field: str,
#         entity: str = 'Equities',
#         file: str = YAML_PREFERENCE) -> Any:
#     """ yaml db config reader """
#     _ = yaml_get('db', file)
#     return _.get(entity).get(field)


DB_NAME: Final[str] = yaml_db_get('name')
DB_PATH: Final[str] = filepath(DB_NAME)


class Record(Base):
    """ basic sqlalchemy record object """
    __tablename__ = yaml_db_get('table')
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
    entities = [_ for _ in await async_fetch(sql_stmt, DB_NAME) if _ not in yaml_db_get('exclude')]
    return entities

STORED: Final[List] = asyncio.run(stored_entities())
fields = ['date']
fields.extend(yaml_get('fields', YAML_PREFERENCE))
data_fields = [eval(f"Record.{_}") for _ in fields]
param = yaml_get('periods', YAML_PREFERENCE).get('Equities')
INDICATORS: Final[List] = yaml_get('indicators', YAML_PREFERENCE)
b_scale = yaml_get('B_scale', YAML_PREFERENCE)


@asyncinit
class Equity(FOA, Viewer):
    """ base object (trial) """
    async def __init__(self, code: int, boarse: str = 'HKEx', static: bool = True) -> Coroutine:
        self.periods = get_periods()
        self.exchange = boarse
        self.code = code
        await self.compose(static)
        self.view = Viewer(self.__data)

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
                    self.__data.set_index('date', inplace=True)
                else:
                    self.__data = await get_data(self.code, self.exchange)
            else:
                self.__data = await get_data(self.code, self.exchange)
        else:
            self.__data = await get_data(self.code, self.exchange)
        self.__data.index = self.__data.index.astype('datetime64[ns]')
        self.change = self.__data.close.pct_change()[-1] * 100
        self.date = self.__data.index[-1]
        self.close = self.__data.close[-1]
        super().__init__(self.__data, float)

    async def fetch(self, start: datetime.date) -> List:
        """ fetch records from local db """
        sql_stmt = select(*data_fields).filter(Record.eid == self.code).filter(
                Record.date >= start)
        return await async_fetch(sql_stmt, DB_NAME)

    def sma(self, param: int = param.get('simple'), data: Any = None) -> pd.DataFrame:
        """ sma """
        return super().sma(param, data).astype('float64')

    def ema(self, param: int = param.get('simple'), data: Any = None) -> pd.DataFrame:
        """ ema """
        return super().ema(param, data).astype('float64')

    def atr(self, param: int = param.get('atr'), data: Any = None) -> pd.DataFrame:
        """ atr """
        return super().atr(param, data).astype('float64')

    def kama(self, param: Dict = param.get('kama'), data: Any = None) -> pd.DataFrame:
        return super().kama(param, data).astype('float64')

    def adx(self, param: int = param.get('adx'), astype: str = 'float64') -> pd.DataFrame:
        """ adx """
        return super().adx(param).astype(astype)

    def apz(self, param: int = param.get('apz'), astype: str = 'float64') -> pd.DataFrame:
        """ apz """
        return super().apz(param).astype(astype)

    def dc(self, param: int = param.get('dc'), astype: str = 'float64') -> pd.DataFrame:
        """ dc """
        return super().dc(param).astype(astype)

    def kc(self, param: Dict = param.get('kc'), astype: str = 'float64') -> pd.DataFrame:
        """ kc """
        return super().kc(param).astype(astype)

    def macd(self, param: Dict = param.get('macd'), astype: str = 'float64') -> pd.DataFrame:
        """ macd """
        return super().macd(param).astype(astype)

    def rsi(self, param: int = param.get('rsi'), astype: str = 'float64') -> pd.DataFrame:
        """ rsi """
        return super().rsi(param).astype(astype)

    def sar(self, param: Dict = param.get('sar'), astype: str = 'float64') -> pd.DataFrame:
        """ ta-lib sar """
        return super().sar(
                param.get('acceleration'),
                param.get('maximum')
                ).astype(astype)

    def stc(self, param: Dict = param.get('stc'), astype: str = 'float64') -> pd.DataFrame:
        """ stc """
        return super().stc(param).astype(astype)

    def soc(self, param: Dict = param.get('soc'), astype: str = 'float64') -> pd.DataFrame:
        """ soc """
        return super().soc(param).astype(astype)

    def maverick(self,
            date=None,
            period=yaml_get('periods',
                YAML_PREFERENCE).get('Equities'),
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
            base = self.__data.query(f'date <= "{date}"')
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


async def A2B(
        entities: Iterable = b_scale.keys(),
        date: str = None) -> dict:

    async def get(
            code: str) -> tuple:
        return (code, await Equity(code, boarse='NYSE'))

    xhg = yaml_get('USHK', YAML_PREFERENCE)
    code_ = []
    opt_ = []
    ent_ = [get(_) for _ in entities]
    res = dict(await atq.gather(*ent_))

    for k, v in res.items():
        yk = b_scale.get(k)
        ratio = yk.get('ratio')
        code_.append(f'{yk.get("code")}.HK')
        hdr = v.optinum(date)
        for k in hdr.keys():
            hdr[k] = hdr[k] if len(hdr[k]) == 0 else (hdr[k] * xhg * ratio).apply(hsirnd)
        opt_.append(hdr)

    return dict(zip(code_, opt_))
