import pandas, pytz, numpy, sqlalchemy
# from scipy.constants import golden_ratio
from datetime import datetime
from tqdm import tqdm
from pathlib import os, sys, functools, posixpath, Path
from time import sleep
# try:
#     from scipy.constants import golden_ratio
# except ImportError:
#     golden_ratio = 1.618

driver = dict(
    Chrome=dict(
        name='chromedriver.exe',
        path=['browser', 'driver']),
    Firefox=dict(
        name='geckodriver.exe',
        path=['browser', 'driver']),
    Ie=dict(
        name='IEDriverServer.exe',
        path=['browser', 'driver']))

subject = {
    'Peter Luk': dict(
        mobile=dict(
            primary=63344427,
            secondary=90757228,
            home=27600722),
        whatsapp=dict(
            input={90757228: '_2_1wd'},
            alias='"陸永原"')),
    'Grace Wu': dict(
        mobile=dict(
            primary=54761657,
            home=26150456),
        whatsapp=dict(
            alias='"胡碧茜Grace"')),
    'Elsa Fung': dict(
        mobile=dict(
            primary=93556260),
        whatsapp=dict(
            alias='"馮麗金"')),
    'Wong Kit Ching': dict(
        mobile=dict(
            primary=98804808),
        whatsapp=dict(
            alias='"柳太黃"')),
    'Milly Ling': dict(
        mobile=dict(
            primary=90107008,
            home=21750098),
        whatsapp=dict(
            alias='"凌月明"')),
    'Mickey Wong': dict(
        mobile=dict(
            primary='+65 83826103'),
        whatsapp=dict(
            alias='"Wong Mickey"')),
    'Niana Mak': dict(
        mobile=dict(
            primary=91096866),
        whatsapp=dict(
            alias='"Ms Niana Siu Chu Mak"')),
    'Vincent Wong': dict(
        mobile=dict(
            primary=66501982),
        whatsapp=dict(
            alias='"Yau Chi Vincent Wong"')),
    'Eddie Chum': dict(
        mobile=dict(
            primary='+853 65858338'),
        whatsapp=dict(
            alias='"沈志明"')),
    'Ho Hung Sung': dict(
        mobile=dict(
            primary=90818157),
        whatsapp=dict(
            alias='"何雄紳"')),
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
    SINA=dict(
        hyperlink='http://finance.sina.com.cn/realstock/company/' +
        'sh000001/nc.shtml',
        tz=pytz.timezone('Asia/Shanghai')),
    NIKKEI=dict(
        hyperlink='https://indexes.nikkei.co.jp/en/nkave/index/' +
        'profile?idx=nk225',
        tz=pytz.timezone('Asia/Tokyo')),
#     CNBC=dict(
#         hyperlink='https://www.cnbc.com/pre-markets/',
#         tz=pytz.timezone('US/Eastern'),
#         # xpath_base='/html/body/div[2]/div/div[1]/div[3]/div[2]/div/div/' +
#         xpath_base='/html/body/div[2]/div/div[1]/div[3]/div/div/div/' +
#         'div[3]/div[1]/div/div[1]/'),
    WhatsApp=dict(
        hyperlink='https://web.whatsapp.com'),
    SMS=dict(
        hyperlink='https://messages.google.com/web'),
    Gold=dict(
        hyperlink='https://www.gold.org/'))

fields = ['open', 'high', 'low', 'close', 'volume']
# xvsa = {'class': 'selectable-text copyable-text'}
xvsa = dict(zip(
    ['role', 'data-tab', 'data-lexical-editor', 'spellcheck', 'contenteditable'],
    ['textbox', '10', 'true', 'true', 'true']))
# xvsa = dict(zip(
#     ['class', 'role', 'data-tab', 'dir', 'spellcheck', 'contenteditable'],
#     ['copyable-text selectable-text', 'textbox', '10', 'ltr', 'true', 'true']))
B_scale = dict(
    BABA=1 / 8,
    BIDU=1 / 8,
    BILI=1,
    TCOM=1,
    XPEV=.5,
    YUMC=1,
    NTES=1 / 5,
    JD=1 / 2,
    WB=1,
    ZLAB=11 / 100)

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
public_holiday[2022] = {
        1: (1,),
        2: (1, 2, 3),
        4: (5, 15, 16, 18),
        5: (2, 9),
        6: (3,),
        7: (1,),
        9: (12,),
        10: (1, 4),
        12: (26, 27)}
public_holiday[2023] = {
        1: (2, 23, 24, 25),
        4: (5, 7, 8, 10),
        5: (1, 26),
        6: (22,),
        7: (1,),
        9: (30,),
        10: (2, 23),
        12: (25, 26)}
public_holiday[2024] = {
        1: (1,),
        2: (10, 12, 13),
        3: (29, 30),
        4: (1, 4),
        5: (1, 15),
        6: (10,),
        7: (1,),
        9: (18,),
        10: (1, 11),
        12: (25, 26)}
public_holiday['2025'] = {
        '1': [1, 29, 30, 31],
        '4': [4, 18, 19, 21],
        '5': [1, 5, 31],
        '7': [1],
        '10': [1, 7, 29],
        '12': [25, 26]}
public_holiday['2026'] = {
        '1': [1],
        '2': [17, 18, 19],
        '4': [3, 4, 5, 6, 7],
        '5': [1, 24, 25],
        '6': [19],
        '7': [1],
        '9': [26],
        '10': [1, 18, 19],
        '12': [25, 26]}
# y2n = [
#         pandas, numpy, sqlalchemy, golden_ratio, datetime, tqdm,
#         sleep, B_scale, USHK]
# nta = [pandas, numpy, datetime, golden_ratio]
# alchemy = [
#         sys.platform, os.environ, os.sep, os.listdir, sqlalchemy,
#         pandas, datetime]
# utils = [
#         os.sep, os.environ, posixpath, Path, os.linesep, sys.platform,
#         sys.version_info, sqlalchemy, golden_ratio, sleep, datetime, driver,
#         functools.reduce, public_holiday]
        # functools.reduce, public_holiday, subject]
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
        exclude=[122, 368, 417, 543, 611, 805, 1212, 1337, 1573, 1828, 2303, 2606, 8509]),
    Futures=dict(
        name='Futures',
        table='records',
        index='date',
        freq='bi-daily')
)
prefer_stock = dict(
    TSE=dict(
        omron=6645,
        sony=6758,
        te=8035)
)

