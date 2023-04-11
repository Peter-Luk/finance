""" async sqlalchemy class (sqlite) trial """
import asyncio
import datetime
import random
from typing import Any, Coroutine, Iterable, List, Dict
import pandas as pd
import yfinance as yf
from tqdm import tqdm
from tqdm.asyncio import tqdm as atq
from asyncinit import asyncinit
from sqlalchemy.orm import declarative_base
from sqlalchemy.future import select
from sqlalchemy import Column, Integer, Float, Date, text
from finance.fintools import FOA
from finance.utilities import filepath, PYTHON_PATH, os, sep, async_fetch


YAML_PREFERANCE = 'pref.yaml'
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
        file: str = YAML_PREFERANCE) -> Any:
    """ yaml db config reader """
    _ = yaml_get('db', file)
    return _.get(entity).get(field)


DB_NAME = yaml_db_get('name')
DB_PATH = filepath(DB_NAME)


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

STORED = asyncio.run(stored_entities())
fields = ['date']
fields.extend(yaml_get('fields', YAML_PREFERANCE))
data_fields = [eval(f"Record.{_}") for _ in fields]
param = yaml_get('periods', YAML_PREFERANCE).get('Equities')
supported_indicators = yaml_get('indicators', YAML_PREFERANCE)


@asyncinit
class Equity(FOA):
    """ base object (trial) """
    async def __init__(self, code: int) -> Coroutine:
        today = datetime.date.today()
        start = datetime.date(today.year - random.choice(range(5, 16)), 1, 1)
        self.code = code
        if code in STORED:
            # self.code = code
            self.__data = pd.DataFrame(
                    await self.fetch(start),
                    columns=fields
                    )
            self.__data.set_index('date', inplace=True)
        else:
            self.__data = yf.download(self.code, period='max', group_by='ticker')
            self.__data.drop('Adj Close', axis=1, inplace=True)
            self.__data.columns = [_.lower() for _ in self.__data.columns]
            self.__data.index.name = self.__data.index.name.lower()

        self.change = self.__data.close.diff(1)[-1] / self.__data.close[-2]
        self.date = self.__data.index[-1]
        self.close = self.__data.close[-1]
        super().__init__(self.__data, float)

    async def fetch(self, start: datetime.date) -> List:
        """ fetch records from local db """
        sql_stmt = select(*data_fields).filter(Record.eid == self.code).filter(
                Record.date >= start)
        return await async_fetch(sql_stmt, DB_NAME)

    def sma(self, param: int = param.get('simple'), data: Any = None):
        """ sma """
        return super().sma(param, data).astype('float64')

    def ema(self, param: int = param.get('simple'), data: Any = None):
        """ ema """
        return super().ema(param, data).astype('float64')

    def atr(self, param: int = param.get('atr'), data: Any = None):
        """ atr """
        return super().atr(param, data).astype('float64')

    def kama(self, param: Dict = param.get('kama'), data: Any = None):
        """ kama """
        return super().kama(param, data).astype('float64')

    def adx(self, param: int = param.get('adx'), astype: str = 'float64'):
        """ adx """
        return super().adx(param).astype(astype)

    def apz(self, param: int = param.get('apz'), astype: str = 'float64'):
        """ apz """
        return super().apz(param).astype(astype)

    def dc(self, param: int = param.get('dc'), astype: str = 'float64'):
        """ dc """
        return super().dc(param).astype(astype)

    def kc(self, param: Dict = param.get('kc'), astype: str = 'float64'):
        """ kc """
        return super().kc(param).astype(astype)

    def macd(self, param: Dict = param.get('macd'), astype: str = 'float64'):
        """ macd """
        return super().macd(param).astype(astype)

    def rsi(self, param: int = param.get('rsi'), astype: str = 'float64'):
        """ rsi """
        return super().rsi(param).astype(astype)

    def sar(self, param: Dict = param.get('sar'), astype: str = 'float64'):
        """ ta-lib sar """
        return super().sar(
                param.get('acceleration'),
                param.get('maximum')
                ).astype(astype)

    def stc(self, param: Dict = param.get('stc'), astype: str = 'float64'):
        """ stc """
        return super().stc(param).astype(astype)

    def soc(self, param: Dict = param.get('soc'), astype: str = 'float64'):
        """ soc """
        return super().soc(param).astype(astype)


async def fetch_data(entities: Iterable, indicator: str = '') -> zip:
    """ fetch entities stored records """
    async def get(code: int) -> Coroutine:
        return await Equity(code)

    ent_list = [get(_) for _ in entities]
    name_list = [f'{_:04d}.HK' for _ in entities if _ in STORED]

    result = await atq.gather(*ent_list)
    res_list = [eval(f"_.{indicator}()") for _ in tqdm(result)] \
        if (indicator in supported_indicators) else result
    return zip(name_list, res_list)
