from sys import platform, version_info
from os import sep, environ
import socket
gr = 1.61803399

ltd = {"2015":(30, 27, 31, 30, 29, 30, 31, 31, 30, 30, 30, 31)}
ltd["2016"] = (29, 29, 31, 29, 31, 30, 29, 31, 30, 31, 30, 30)
ltd["2017"] = (31, 28, 31, 28, 31, 30, 31, 31, 29, 31, 30, 29)

futures_type, month_initial = ('HSI', 'MHI', 'HHI', 'MCH'), {'January':'F', 'February':'G', 'March':'H', 'April':'J', 'May':'K', 'June':'M', 'July':'N', 'August':'Q', 'September':'U', 'October':'V', 'November':'X', 'December':'Z'}
avail_indicators, cal_month = ('wma','kama','ema','hv'), (3, 6, 9, 12)

def filepath(name, drive='C'):
    if platform == 'win32':file_drive, file_path = '%s:'%drive, sep.join(('Users', 'Peter Luk', 'data', 'sqlite3'))
    if platform == 'linux-armv7l':file_drive, file_path = '', sep.join(('mnt', 'sdcard', 'data', 'sqlite3'))
    if platform in ('linux', 'linux2'):
        file_drive, place = '', 'shared'
        if ('EXTERNAL_STORAGE' in environ.keys()) and ('/' in environ['EXTERNAL_STORAGE']):place = 'external'
        file_path = sep.join(('data', 'data', 'com.termux', 'files', 'home', 'storage', place, 'data', 'sqlite3'))
    return sep.join((file_drive, file_path, name))

def rnd(n, decimal_place=0):
    try:
        if decimal_place:return round(n, decimal_place)
        elif version_info.major > 2:return round(n)
        else:return int(round(n))
    except:pass

def gratio(n1, n2, ratio=None, enhanced=False):
    if not(ratio):ratio = gr
    try:
        res = []
        if n1 > n2:t = [n1, n2]
        try:
            t.reverse()
            n1, n2 = t
        except:pass
        delta = n2 - n1
        if enhanced:
            trange = [rnd(delta / ratio), rnd(delta * (1 - 1 / ratio)), -rnd(delta * (1 - 1 / ratio))]
            res += [n1 - i for i in trange]
            res += [n2 + i for i in trange]
            res.sort()
        else:
            trange = [rnd(delta * (1 - 1 / ratio)), rnd(delta / ratio)]
            res += [n1 + i for i in trange]
        return tuple(res)
    except:pass

def average(v):return sum(v) / len(v)

def product(v):
    a = 1
    for i in v:a *= i
    return a

class IP():
    def __init__(self, mode=None):
        if mode:
            try:
                self.mode = mode.lower()
            except:pass
        self.address = None
        try:
            if self.mode == 'public':
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                self.address = s.getsockname()[0]
        except:pass
    def __del__(self):
        self.address = self.mode = None
        del(self.mode)
        del(self.address)
