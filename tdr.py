"""
#!/data/data/com.termux/files/usr/bin/python
#!/bin/python3
"""
import asyncio
from tqdm import tqdm
from benedict import benedict
from pathlib import os
from typing import Coroutine, Dict, List, Iterable, Final, Union
from pstock import BarsMulti
from utilities import getcode
from fintools import hsirnd

YAML_PREFERENCE: Final[str] = f'{os.sep}portfolio.yaml'
clients = iter(('M213423', 'M241238', 'P724059', 'P772215', 'P851223'))


async def daily_close(
        client_no: str,
        boarse: str = 'HKEx') -> None:
    _: List = Portfolio(client_no, boarse)()
    _y = [getcode(__, boarse) for __ in tqdm(_)]
    data = await BarsMulti.get(_y, period='5d', interval='1d')
    result = [f'{i} {hsirnd(data[getcode(i, boarse)].df.close.iloc[-1]):0.2f}' for i in _]
    print(f"{client_no}\nClose price, {', '.join(result)}")

async def main():
    for f in asyncio.as_completed([daily_close(_, 'HKEx') for _ in clients]):
        await f

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

    def remove(self,
               code: Union[int, str, Iterable]) -> None:
        self.update_(code, add=False)

    def update_(self,
                target: Union[int, str, Iterable],
                add: bool = True) -> None:
        if isinstance(target, str):
            target = [int(target)] if boarse in ['HKEx', 'TSE'] else [target]
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
                ent_list.remove(t)
    
        d[self._] = ent_list
        d.to_yaml(filepath=self.f_)

if __name__ == "__main__":
    asyncio.run(main())
