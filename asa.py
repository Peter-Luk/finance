""" async sqlalchemy class (sqlite) trial """
import asyncio
import datetime
import random
from typing import Any, Coroutine, Iterable, List, Dict, Final
import pandas as pd
# import yfinance as yf
from tqdm import tqdm
from tqdm.asyncio import tqdm as atq
from asyncinit import asyncinit
from sqlalchemy.orm import declarative_base
from sqlalchemy.future import select
from sqlalchemy import Column, Integer, Float, Date, text
from finance.fintools import FOA
from finance.utilities import filepath, PYTHON_PATH, os, sep, async_fetch
from asyahoo import get_data

YAML_PREFERENCE: Final[str] = 'pref.yaml'
Base = declarative_base()


def yaml_get(field: str, file: str) -> Any:
    """ Basic yaml config reader """
    if file.split('.')[-1] == 'yaml':
        import yaml
        fpaths = [os.getcwd()]
        fpaths.extend(PYTHON_PATH)
        for f_p in fpaths:
            _f = f'{f_p}{sep}{file}'
            if os.path.isfile(_f):
                with open(_f, encoding='utf-8') as y_f:
                    _ = yaml.load(y_f, Loader=yaml.FullLoader)
                res = _.get(field)
    return res


def yaml_db_get(
        field: str,
        entity: str = 'Equities',
        file: str = YAML_PREFERENCE) -> Any:
    """ yaml db config reader """
    _ = yaml_get('db', file)
    return _.get(entity).get(field)


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
    entities = [_[0] for _ in await async_fetch(sql_stmt, DB_NAME) if _[0] not in yaml_db_get('exclude')]
    return entities

STORED: Final[List] = asyncio.run(stored_entities())
fields = ['date']
fields.extend(yaml_get('fields', YAML_PREFERENCE))
data_fields = [eval(f"Record.{_}") for _ in fields]
param = yaml_get('periods', YAML_PREFERENCE).get('Equities')
INDICATORS: Final[List] = yaml_get('indicators', YAML_PREFERENCE)


@asyncinit
class Equity(FOA):
    """ base object (trial) """
    async def __init__(self, code: int, boarse: str = 'HKEx') -> Coroutine:
        today = datetime.date.today()
        start = datetime.date(today.year - random.choice(range(5, 16)), 1, 1)
        self.code = code
        if boarse == 'HKEx' and code in STORED:
            # self.code = code
            self.__data = pd.DataFrame(
                    await self.fetch(start),
                    columns=fields
                    )
            self.__data.set_index('date', inplace=True)
        else:
            self.__data = await get_data(code, boarse)
            # self.__data = yf.download(f'{self.code:04d}.HK', period='max', group_by='ticker')
            # self.__data.drop('Adj Close', axis=1, inplace=True)
            # self.__data.columns = [_.lower() for _ in self.__data.columns]
            # self.__data.index.name = self.__data.index.name.lower()

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
