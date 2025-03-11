"""
#!/data/data/com.termux/files/usr/bin/python
#!/bin/python3
"""
import asyncio
from benedict import benedict
from pathlib import os
from typing import Coroutine, Dict, List, Iterable, Final, Union
import yfinance as yf
# from pstock import BarsMulti
from utilities import getcode
from fintools import hsirnd

YAML_PREFERENCE: Final[str] = f'{os.sep}portfolio.yaml'
clients: list = ['M213423', 'P241238', 'P724059', 'P772215', 'P851223']


async def daily_close(
        client_no: str,
        boarse: str = 'HKEx') -> None:
    _: List = Portfolio(client_no, boarse)()
    _y = [getcode(__, boarse) for __ in _]
    data = yf.download(_y, period='5d', interval='1d', auto_adjust=False)
    result = [f'{i} {hsirnd(data.xs(getcode(i, boarse),axis=1,level="Ticker").Close.iloc[-1]):0.2f}' for i in _]
    # data = await BarsMulti.get(_y, period='5d', interval='1d')
    # result = [f'{i} {hsirnd(data[getcode(i, boarse)].df.close.iloc[-1]):0.2f}' for i in _]
    return {client_no: f"Close price, {', '.join(result)}"}

async def main():
    holder = {}
    for c in clients:
        _ = await daily_close(c, 'HKEx')
        for k, v in _.items():
            holder[k] = v
    for k, v in holder.items():
        print(f'{k}\n{v}')

class Portfolio():
    def __init__(self, client: str, boarse: str = 'HKEx'):
        self.client = client.upper()
        self.boarse = boarse
        self.f_ = f"{os.getenv('PYTHONPATH')}{YAML_PREFERENCE}"
        self._ = f'{self.client}.{self.boarse}'
        d = benedict.from_yaml(self.f_)
        self.holdings = d.get_list(self._)

    def __add__(self, another):
        res = list(set(self.holdings + another.holdings))
        return sorted(res)

    def __len__(self):
        return len(self.holdings)

    def __call__(self):
        d = benedict.from_yaml(self.f_)
        return d.get_list(self._)

    def add(self,
            code: Union[int, str, Iterable]) -> None:
        self.update_(code, add=True)
        result = f'{code}' if isinstance(code, (int, str)) else ', '.join([f'{_}' for _ in code])
        return f'updated, {result} added.'

    def remove(self,
               code: Union[int, str, Iterable]) -> None:
        self.update_(code, add=False)
        result = f'{code}' if isinstance(code, (int, str)) else ', '.join([f'{_}' for _ in code])
        return f'updated, {result} removed.'

    def update_(self,
                target: Union[int, str, Iterable],
                add: bool = True) -> None:
        if isinstance(target, str):
            target = [int(target)] if self.boarse in ['HKEx', 'TSE'] else [target]
        else:
            target = [target] if isinstance(target, int) else target
        d = benedict.from_yaml(self.f_)
        ent_list = d.get_list(self._)
        if add:
            for t in target:
                if t not in ent_list:
                    ent_list.append(t)
            ent_list.sort()
        else:
            for t in target:
                if t in ent_list:
                    ent_list.remove(t)
        self.holdings = ent_list
    
        d[self._] = ent_list
        d.to_yaml(filepath=self.f_)

if __name__ == "__main__":
    asyncio.run(main())
