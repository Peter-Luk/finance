import time
import aiohttp
import asyncio
import pandas as pd
from io import StringIO
from typing import Any, Final, Dict
from utilities import getcode

class StockCodeError(Exception):
    'Raised when no valid stock code matched'
YAHOO_URL: Final[str] = 'https://query1.finance.yahoo.com/v7/finance/download'
# ticker = 'BTC-USD'
# url = f'{YAHOO_URL}/{ticker}?'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ApplSeWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.57'
}
# url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?'

params = {
    'period1': 0,
    'period2': int(time.time()),
    'interval': '1d',  # change interval to '1d' for daily data
    'events': 'history'
}


#     async with session.get(url, params=params, headers=headers) as response:
#         return await response.text()
#
#
async def get_data(ticker: Any = 9988,
        boarse: str = 'HKEx') -> pd.DataFrame:
    url = f'{YAHOO_URL}/{getcode(ticker, boarse)}?'


    async def fetch(session, url: str, params: Dict, headers: Dict):
        async with session.get(url, params=params, headers=headers) as response:
            return await response.text()


    async with aiohttp.ClientSession() as session:
        try:
            response = await fetch(session, url, params, headers)
            df = pd.read_csv(StringIO(response))
            df.drop('Adj Close', axis=1, inplace=True)
            df.columns = [_.lower() for _ in df.columns]
            df.set_index('date', inplace=True)
            return df
        except StockCodeError:
            pass

if __name__ == "__main__":
    print(asyncio.run(get_data()))
