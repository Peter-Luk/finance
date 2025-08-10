#!/bin/python3

"""
#!/data/data/com.termux/files/usr/bin/python
#!/bin/python3
"""

import asyncio
from typing import List
from rich import print
import yfinance as yf
from utilities import getcode
from fintools import hsirnd, Portfolio

clients: list = ['M213423', 'P241238', 'P724059', 'P772215', 'P851223']


async def daily_close(
        client_no: str,
        boarse: str = 'HKEx') -> None:
    _: List = Portfolio(client_no, boarse)()
    data = yf.download([getcode(__, boarse) for __ in _], period='5d', interval='1d', auto_adjust=False)
    result = [f'{i} {hsirnd(data.xs(getcode(i, boarse),axis=1,level="Ticker").Close.iloc[-1]):0.2f}' for i in _]
    return {client_no: f"Close price, {', '.join(result)}"}


async def main():
    _ = await asyncio.gather(*[daily_close(c, 'HKEx') for c in iter(clients)])
    # _ = iter([await daily_close(c, 'HKEx') for c in iter(clients)])
    for item in _:
        for k, v in item.items():
            print(f'{k}\n{v}')


if __name__ == "__main__":
    asyncio.run(main())
