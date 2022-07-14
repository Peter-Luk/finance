import asyncio
# import re
import json
import pathlib
import time
import random
import functools
import contextvars
from datetime import datetime
import pandas as pd
import yaml
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from finance.utilities import PYTHON_PATH, os, sep


fname = 'pref.yaml'
fpaths = [os.getcwd()]
fpaths.extend(PYTHON_PATH)
for fp in fpaths:
    _f = f'{fp}{os.sep}{fname}'
    if os.path.isfile(_f):
        break
with open(_f, encoding='utf-8') as f:
    _ = yaml.load(f, Loader=yaml.FullLoader)
    _site = _.get('website')


async def to_thread(func, /, *args, **kwargs):
    if pathlib.sys.version_info.major > 2:
        if pathlib.sys.version_info.minor > 8:
            return await asyncio.to_thread(func, *args, **kwargs)
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        func_call = functools.partial(ctx.run, func, *args, **kwargs)
        return await loop.run_in_executor(None, func_call)


async def grab(site: str='Yahoo'):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context()
        page = await context.new_page()
        if site in _site.keys():
            await page.goto(f"https://{_site[site]['address']}", timeout=_site[site]['timeout'], wait_until='load')
            html = await to_thread(BeautifulSoup, await page.content(), 'html.parser')
            # html = await asyncio.get_running_loop().run_in_executor(None, BeautifulSoup, await page.content(), 'html.parser')
            # html = BeautifulSoup(await page.content(), 'html.parser')
        await browser.close()
    return html


def fetch(html, site: str='Yahoo') -> json:
    market = {}
    if site in _site.keys():
        if site == 'Yahoo':
            symbols = _site[site]['symbol']
            for k, v in _site[site]['fields'].items():
                if k in ['latest', 'change']:
                    market[k] = [float(html.find(attrs={'data-symbol': x, 'data-field':f'regularMarket{v}'}).text.replace(',', '')) for x in symbols.values()]
                if k == '% change':
                    market[k] = [float(html.find(attrs={'data-symbol': x, 'data-field':f'regularMarket{v}'}).text.strip('()%')) for x in symbols.values()]
            hdr = pd.DataFrame(market)
            hdr.index = list(symbols.keys())

        if site == 'Nikkei':
            fields = _site[site]['fields']
            # css selector #price
            latest = float(html.select(f"#{fields['latest']}")[0].text.strip('\n ').replace(',',''))
            # css selector #diff
            if fields['change'] == fields['% change']:
                change, pct_change = [float(_) for _ in html.select(f"#{fields['change']}")[0].text.strip(')%\n\xa0').split(' (')]
            # css selector #datedtime
            status = 'Close'
            time_string = html.select(f"#{fields['time']}")[0].text.strip('\n ')
            if time_string.find('Close') == -1:
                time = datetime.strptime(time_string, '%b/%d/%Y(%H:%M)')
                status = time.time()
            hdr = pd.DataFrame.from_dict({'Nikkei': {'latest': latest, 'change': change, '% change': pct_change, 'status':status}}, orient='index', columns=['latest', 'change', '% change', 'status'])
        return hdr.to_json()


if __name__ == '__main__':
    # site = 'Nikkei'
    site = 'Yahoo'
    if len(pathlib.sys.argv) > 1:
        site = pathlib.sys.argv[1]
    while True:
        print(f'{datetime.now().time()} Fetching data from {site}')
        try:
            page = asyncio.run(grab(site))
            dt = json.loads(fetch(page, site))
            print(datetime.now().time())
            print(pd.DataFrame.from_dict(dt, orient="index").T)
        except Exception as e:
            print(datetime.now().time())
            print(f'Fetching error: {e}')
        print()
        time.sleep(random.randint(55, 105))
