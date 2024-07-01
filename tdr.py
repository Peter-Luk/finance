#!/data/data/com.termux/files/usr/bin/python
import asyncio
import pandas as pd
from tqdm import tqdm
from asyncinit import asyncinit
from typing import Coroutine, Dict, List, Final, Union
from utilities import yaml_get, getcode
from fintools import hsirnd
from asyahoo import get_data

YAML_PREFERENCE: Final[str] = 'pref.yaml'
clients = iter(('M213423', 'M241238', 'P724059', 'P772215', 'P851223', 'P854787'))

@asyncinit
class Equity():
    async def __init__(
            self,
            code: Union[int, str],
            boarse: str = 'HKEx',
            capitalize: bool = True
            ) -> Coroutine:
        self.code = code
        self.exchange = boarse
        self.yahoo_code = getcode(self.code, self.exchange)
        self.__capitalize = capitalize
        await self.compose()

    def __str__(self):
        return f"{self.code} {hsirnd(self.close):0.2f}"

    async def compose(self) -> pd.DataFrame:
        self.__data = await get_data(self.code, self.exchange, self.__capitalize)
        self.__data.index = self.__data.index.astype('datetime64[ns]')
        self.date = self.__data.index[-1]
        if self.__capitalize:
            self.change = self.__data.Close.pct_change(fill_method=None).iloc[-1]
            self.close = self.__data.Close.iloc[-1]
        else:
            self.change = self.__data.close.pct_change(fill_method=None).iloc[-1]
            self.close = self.__data.close.iloc[-1]

async def daily_close(
        client_no: str,
        boarse: str = 'HKEx') -> None:
    result: List = []
    _: List = yaml_get(client_no, 'portfolio.yaml').get(boarse)
    subject: Dict = dict(zip(_, [boarse for __ in _]))
    for f in asyncio.as_completed([Equity(c, b) for c, b in tqdm(list(subject.items()))]):
        holder = await f
        result.append(f'{holder}')
    print(f"{client_no}\nClose price, {', '.join(result)}")

async def main():
    for f in asyncio.as_completed([daily_close(_, 'HKEx') for _ in tqdm(list(clients))]):
        await f

if __name__ == "__main__":
    asyncio.run(main())
