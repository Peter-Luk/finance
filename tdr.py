#!/bin/python3

"""
#!/data/data/com.termux/files/usr/bin/python
#!/bin/python3
"""

import asyncio
from typing import List
from rich import print
import yfinance as yf
from utilities import getcode, YAML
from fintools import hsirnd, Portfolio
from functools import partial

yfd = partial(yf.download, period='5d', interval='1d', auto_adjust=False)
clients: list = YAML.prefer_customers


async def daily_close(
        client_no: str,
        boarse: str = 'HKEx') -> None:
    _: List = Portfolio(client_no, boarse)()
    codes = [getcode(__, boarse) for __ in _]
    # task = asyncio.create_task(asyncio.to_thread(yfd, codes))
    # data = await task

    # tasks = [asyncio.create_task(asyncio.to_thread(yfd, code)) for code in codes]
    # data = await asyncio.gather(*tasks, return_exceptions=True)

    data = yfd(codes)

    # data = yf.download([getcode(__, boarse) for __ in _], period='5d', interval='1d', auto_adjust=False)
    result = []
    for i in _:
        close = data.xs(getcode(i, boarse),axis=1,level="Ticker").Close.iloc[-1]
        item = f'{i} {close:0.3f}' if close < .1 else f'{i} {hsirnd(close):0.2f}'
        result.append(item)
    return {client_no: f"Close price, {', '.join(result)}"}


async def main():
    dc = partial(daily_close, boarse='HKEx')
    # tasks = [asyncio.create_task(asyncio.to_thread(dc, client)) for client in clients]
    # _ = await asyncio.gather(*tasks, return_exceptions=True)
    _ = await asyncio.gather(*[dc(c) for c in iter(clients)])
    for item in _:
        for k, v in item.items():
            print(f'{k}\n{v}')


if __name__ == "__main__":
    asyncio.run(main())
