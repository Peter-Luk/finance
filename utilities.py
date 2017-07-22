e = getattr(__import__('handy'),'encoder')
rim = {'socket':(), 'datetime':('datetime',)}
rim['sys'] = ('platform', 'version_info')
rim['os'] = ('linesep', 'sep', 'environ')
__ = e(rim)
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))
gr = 1.61803399

today = datetime.today()
year, month, month_string = '%i' % today.year, today.month, today.strftime('%B')

ph = {'2015':{'1':(1,), '2':(19, 20), '4':(3, 6, 7), '5':(25,), '7':(1,), '9':(28,), '10':(1, 21), '12':(25,)}}
ph['2016'] = {'1':(1,), '2':(8, 9, 10), '3':(25, 28), '4':(4,), '5':(2,), '6':(9,), '7':(1,), '9':(16,), '10':(10,), '12':(26, 27)}
ph['2017'] = {'1':(2, 30, 31), '4':(4, 14, 17), '5':(1, 3, 30), '10':(2, 5), '12':(25, 26)}
ph['2018'] = {'1':(1,), '2':(16, 19), '3':(30,), '4':(2, 5), '5':(1, 22), '6':(18,), '7':(2,), '9':(25,), '10':(1, 17), '12':(25, 26)}

def ltd(year=year, month=month, excluded={}):
    t, ld = 0, [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if not(year % 4):ld[1] += 1
    day = ld[month - 1]
    if excluded:
        if '%'%year in excluded.keys():
            if '%s'%month in excluded['%s'%year].keys():
                if day not in excluded['%s'%year]['%s'%month]:t += 1
    while t < 1:
        if datetime(year, month, day).weekday() < 5:
            if '%s'%month in ph['%s'%year].keys():
                 if day not in ph['%s'%year]['%s'%month]:t += 1
            else:t += 1
        day -= 1
    return day + 1

futures_type, month_initial = ('HSI', 'MHI', 'HHI', 'MCH'), {'January':'F', 'February':'G', 'March':'H', 'April':'J', 'May':'K', 'June':'M', 'July':'N', 'August':'Q', 'September':'U', 'October':'V', 'November':'X', 'December':'Z'}
avail_indicators, cal_month = ('wma','kama','ema','hv'), (3, 6, 9, 12)

def get_month(index):
    if index.upper() in month_initial.values():
        for i in list(month_initial.items()):
            if i[-1] == index.upper(): return i[0]

def dex(n=0):
    if n in range(12):
        n_month, n_year = month + n, today.year
        if n_month > 12 and n_month != n_month % 12:n_month, n_year = n_month % 12, n_year + 1
        return month_initial[datetime(n_year, n_month, 1).strftime('%B')] + ('%i' % n_year)[-1]
    else:print("Out of range (0 - 11)")

def waf(delta=0):
    futures = [''.join((f,dex(delta))) for f in futures_type[:-2]]
    futures += [''.join((f,dex(delta+1))) for f in futures_type[:-2]]
    return tuple(futures)

def filepath(*args, **kwargs):
    name, file_type, data_path = args[0], 'data', 'sqlite3'
    if 'type' in list(kwargs.keys()): file_type = kwargs['type']
    if 'subpath' in list(kwargs.keys()): data_path = kwargs['subpath']
    if platform == 'win32':
        file_drive, file_path = environ['HOMEDRIVE'], environ['HOMEPATH']
#         reqval = ('drive', 'path')
#         for i in reqval:
#             if i in args.keys():exec("file_%s = '%s'" % (i, args[i]))
        file_path = sep.join((file_drive, file_path, file_type, data_path))
    if platform == 'linux-armv7l':file_drive, file_path = '', sep.join(('mnt', 'sdcard', file_type, data_path))
    if platform in ('linux', 'linux2'):
        place = 'shared'
        if 'ACTUAL_HOME' in environ.keys():file_path = sep.join((environ['HOME'], file_type, data_path))
        elif ('EXTERNAL_STORAGE' in environ.keys()) and ('/' in environ['EXTERNAL_STORAGE']):
            place = 'external-1'
            file_path = sep.join((environ['HOME'], 'storage', place, file_type, data_path))
    return sep.join((file_path, name))

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

def dvs(d):
    res, values = [], list(d.values())
    values.sort()
    for v in values: res.extend([i[0] for i in list(d.keys()) if d[i] == v])
    return ''.join(res)
