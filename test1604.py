import aiohttp
import asyncio
import pandas as pd
from io import StringIO

YOHOO_URL = 'https://query1.finance.yahoo.com/v7/finance/download'
ticker = 'BTC-USD'
url = f'{YAHOO_URL}/{ticker}?'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.57'
}
# url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker}?'

params = {
    'period1': 0,
    'period2': int(time.time()),
    'interval': '1d',  # change interval to '1d' for daily data
    'events': 'history'
}


async def fetch(session, url, params, headers):
    async with session.get(url, params=params, headers=headers) as response:
        return await response.text()


async def get_data():
    async with aiohttp.ClientSession() as session:
        response = await fetch(session, url, params, headers)
        df = pd.read_csv(StringIO(response))
        print(df)


asyncio.run(get_data())
