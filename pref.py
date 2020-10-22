# import pandas, numpy, sqlalchemy, fix_yahoo_finance
import pandas, numpy, sqlalchemy, yfinance
from scipy.constants import golden_ratio
from datetime import datetime
from tqdm import tqdm
from pathlib import Path, os, sys, functools
from time import sleep

driver = dict(
    Chrome=dict(
        name='chromedriver.exe',
        path=['browser', 'driver']),
    Firefox=dict(
        name='geckodriver.exe',
        path=['browser', 'driver']),
    Ie=dict(
        name='IEDriverServer.exe',
        path=['browser', 'driver'])
)

subject = {
    'Peter Luk': dict(
        mobile=dict(
            primary=63344427,
            secondary=90757228,
            home=27600722),
        whatsapp=dict(
            input={90757228: '_3FRCZ'},
            alias='"陸永原"')),
    'Milly Ling': dict(
        mobile=dict(
            primary=90107008,
            home=21750098),
        whatsapp=dict(
            alias='"凌月明Milly"')),
    'Mickey Wong': dict(
        mobile=dict(
            primary='+65 83826103'),
        whatsapp=dict(
            alias='"Wong Mickey"')),
    'Chan Cheung Yiu': dict(
        mobile=dict(
            primary=91031418),
        whatsapp=dict(
            alias='"陳長耀"')),
    'Josephine Yuen': dict(
        mobile=dict(
            primary=98682629),
        whatsapp=dict(
            alias='"袁淑兒Josephine"'))
    }

source = dict(
    SINA='http://finance.sina.com.cn/realstock/company/sh000001/nc.shtml',
    NIKKEI='https://indexes.nikkei.co.jp/en/nkave/index/profile?idx=nk225',
    CNBC='https://www.cnbc.com/pre-markets/',
    WhatsApp='https://web.whatsapp.com',
    SMS='https://messages.google.com/web',
    Gold='https://www.gold.org/')

fields = ['open', 'high', 'low', 'close', 'volume']
B_scale = dict(
    BABA=1 / 8,
    YUMC=1,
    NTES=1 / 5,
    # NTES=1 / 25,
    JD=1 / 2)

USHK = 7.75
public_holiday = {2015: {
    1: (1,),
    2: (19, 20),
    4: (3, 6, 7),
    5: (25,),
    7: (1,),
    9: (28,),
    10: (1, 21),
    12: (25,)}}
public_holiday[2016] = {
        1: (1,),
        2: (8, 9, 10),
        3: (25, 28),
        4: (4,),
        5: (2,),
        6: (9,),
        7: (1,),
        9: (16,),
        10: (10,),
        12: (26, 27)}
public_holiday[2017] = {
        1: (2, 30, 31),
        4: (4, 14, 17),
        5:  (1, 3, 30),
        10: (2, 5),
        12: (25, 26)}
public_holiday[2018] = {
        1: (1,),
        2: (16, 19),
        3: (30,),
        4: (2, 5),
        5: (1, 22),
        6: (18,),
        7: (2,),
        9: (25,),
        10: (1, 17),
        12: (25, 26)}
public_holiday[2019] = {
        1: (1,),
        2: (5, 6, 7),
        4: (5, 19, 20, 22),
        5: (1, 12, 13),
        6: (7,),
        7: (1,),
        9: (14,),
        10: (1, 7),
        12: (25, 26)}
public_holiday[2020] = {
        1: (1, 25, 27, 28),
        2: (14,),
        3: (20,),
        4: (4, 10, 11, 12, 13, 30),
        5: (1, 10),
        6: (20, 21, 25),
        7: (1,),
        9: (22,),
        10: (1, 2, 26),
        12: (21, 25, 26)}
public_holiday[2021] = {
        1: (1,),
        2: (12, 13, 14, 15),
        4: (2, 3, 4, 5, 6),
        5: (1, 19),
        6: (14,),
        7: (1,),
        9: (22,),
        10: (1, 14),
        12: (25, 27)}
y2n = [
        pandas, numpy, sqlalchemy, yfinance, golden_ratio, datetime, tqdm,
        sleep, B_scale, USHK]
nta = [pandas, numpy, datetime, golden_ratio]
alchemy = [
        sys.platform, os.environ, os.sep, os.listdir, Path, sqlalchemy,
        pandas, datetime]
utils = [
        os.sep, os.environ, os.linesep, sys.platform, sys.version_info,
        Path, sqlalchemy, yfinance, golden_ratio, sleep, datetime, driver,
        functools.reduce, public_holiday]
periods = dict(
    Futures=dict(
        macd=dict(
            fast=5,
            slow=10,
            signal=3),
        soc=dict(
            K=14,
            D=3),
        stc=dict(
            fast=5,
            slow=12,
            K=3,
            D=3),
        atr=7,
        rsi=7,
        kama=dict(
            er=7,
            fast=2,
            slow=12),
        kc=dict(
            kama=dict(
                er=5,
                fast=2,
                slow=12),
            atr=7),
        adx=7,
        simple=12,
        dc=12,
        apz=5),
    Equities=dict(
        macd=dict(
            fast=12,
            slow=26,
            signal=9),
        soc=dict(
            K=14,
            D=3),
        stc=dict(
            fast=23,
            slow=50,
            K=10,
            D=10),
        atr=14,
        rsi=14,
        kama=dict(
            er=10,
            fast=2,
            slow=30),
        kc=dict(
            kama=dict(
                er=5,
                fast=2,
                slow=20),
            atr=10),
        adx=14,
        simple=20,
        dc=20,
        apz=5)
)
db = dict(
    Equities=dict(
        name='Securities',
        table='records',
        index='date',
        freq='daily',
        exclude=[122, 368, 805, 1828, 8509]),
    Futures=dict(
        name='Futures',
        table='records',
        index='date',
        freq='bi-daily')
)


def div_input(_):
    mb = subject[_]['mobile']['secondary']
    keys = ['class', 'data-tab', 'dir', 'spellcheck', 'contenteditable']
    values = [
        f"{subject[_]['whatsapp']['input'][mb]} copyable-text selectable-text",
        '6', 'ltr', 'true', 'true']
    return ''.join(
        [f'[@{k}="{v}"]' for k, v in dict(zip(keys, values)).items()])
