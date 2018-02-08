him = getattr(__import__('handy'), 'him')
iml = [{'utilities':('filepath',)}, {'datetime':('datetime',)}, {'os':('listdir', 'linesep', 'sep', 'path', 'remove')}, {'sys':('platform',)}, {'pandas_datareader':('data',)}, {'functools':('reduce',)}, ({'sqlite3':()}, "alias='lite'"), ({'pandas':()}, "alias='pd'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']" % (_, _))

db_name, db_table, datafields = 'Securities', 'records', ['open', 'high', 'low', 'close', 'volume']

def u2lite(*args, **kwargs):
    """
Update SQLite db with dict object, all three arguments are compulsory,
1.) 'code' or 'eid' (Equities ID), type int, affected,
2.) source, type dict, and
3.) SQLite db, type connection.
    """
    if isinstance(args[0], int): code = args[0]
    if isinstance(args[1], dict): s_dict = args[1]
    counter, conn = 0, args[2]
    qstr = "UPDATE {} SET {{}} WHERE eid={:d} AND date='{{:%Y-%m-%d}}'".format(db_table, code)
    for _ in list(s_dict.keys()):
        sl = []
        try:
            for __ in datafields:
                if __ == 'volume': sl.append('{}={:d}'.format(__, int(s_dict[_][__.capitalize()])))
                else: sl.append('{}={:.3f}'.format(__, s_dict[_][__.capitalize()]))
            conn.cursor().execute(qstr.format(', '.join(sl), _))
            counter += 1
            conn.commit()
        except: pass
    return counter

def dictfcomp(*args, **kwargs):
    res = {}
    if isinstance(args[0], dict): ad = args[0]
    if isinstance(args[1], dict): rd = args[1]
    for _ in list(rd.keys()):
        if not reduce((lambda x, y: x and y), ['{:.3f}'.format(ad[_][__]) == '{:.3f}'.format(rd[_][__]) for __ in list(rd[_].keys())]):
            res[_] = ad[_]
    return res

def pstored(*args, **kwargs):
    if args:
        if isinstance(args[0], str):
            try: a0 = int(args[0])
            except: pass
        if isinstance(args[0], int): a0 = args[0]
    scon = lite.connect(filepath(db_name))
    end = datetime.today()
    start = get_start(1)
    qstr = "SELECT {} FROM {} WHERE eid={{:d}} AND date BETWEEN '{{:%Y-%m-%d}}' AND '{{:%Y-%m-%d}}' ORDER BY date ASC".format(', '.join(datafields + ['date']), db_table)
    d = pd.read_sql_query(qstr.format(a0, start, end), scon, parse_dates={'date':'%Y-%m-%d'}).transpose().to_dict().values()
    if d:
        vd = {}
        for _ in d: vd[_.pop('date')] = _
        for _ in list(vd.keys()):
            temp = {}
            for __ in list(vd[_].keys()): temp[__.capitalize()] = vd[_][__]
            vd[_] = temp
        return vd

def stored_data(*args, **kwargs):
    res, where, fields, lk = [], [], ['date'] + datafields, list(kwargs.keys())
    try:
        a0 = args[0]
        if isinstance(a0, str): a0 = int(float(a0))
        if isinstance(a0, int): where.append('eid={:d}'.format(a0))
    except: pass
    lk = list(kwargs.keys())
    if 'where' in lk:
        if isinstance(kwargs['where'], list): where.extend(kwargs['where'])
        elif isinstance(kwargs['where'], str): where.append(kwargs['where'])
        elif isinstance(kwargs['where'], dict):
            lkw = list(kwargs['where'].keys())
            try:
                where.extend(['{}={{{}}}'.format(_, _).format(**kwargs['where']) for _ in lkw])
            except: pass
    conn = lite.connect(filepath(db_name))
    conn.row_factory = lite.Row
    qstr = "SELECT {{{}}} FROM {}".format('', db_table)
    if where:
        qstr = ' WHERE '.join([qstr, ' and '.join(where)])
        fields.append('id')
        res.extend(conn.cursor().execute(qstr.format(','.join(fields))).fetchall())
    conn.close()
    return res

def get_stored_eid():
    conn = lite.connect(filepath(db_name))
    conn.row_factory = lite.Row
    res = [_['eid'] for _ in conn.cursor().execute("SELECT DISTINCT {0} FROM {1} ORDER BY {0} ASC".format('eid', db_table)).fetchall()]
    conn.close()
    return res

def find_csv_path(*args, **kwargs):
    folder, sd = 'csv', filepath(db_name)
    if args:
        folder = args[0]
    if 'folder' in list(kwargs.keys()):
        if isinstance(kwargs['folder'], str): folder = kwargs['folder']
    sdl = sd.split(sep)
    cp = sdl[:-2]
    cp.append(folder)
    if platform == 'linux':
        si = sdl.index('storage')
        cp = sdl[:si+1]
        cp.extend(['shared', 'Download'])
        cp = sep.join(cp)
        cpl = len(listdir(cp))
        if cpl == 0:
            cp = sdl[:-2]
            cp.append(folder)
            cp = sep.join(cp)
            cpl = len(listdir(cp))
            if cpl == 0: return False
        return cp

def c2d(*args, **kwargs):
    lines, tmp = args[0], []
    fields = lines[0].split(',')
    for l in lines[1:]:
        try:
            hdr, dl = {}, l.split(',')
            for f in fields:
                if f != 'Adj Close':
                    if f == 'Date': hdr[f.lower()] = '{}'.format(dl[fields.index(f)])
                    elif f == 'Volume': hdr[f.lower()] = int(float(dl[fields.index(f)]))
                    elif f in ['Open', 'High', 'Low', 'Close']: hdr[f.lower()] = float(dl[fields.index(f)])
            tmp.append(hdr)
        except: pass
    return tmp

def get_start(*args, **kwargs):
    end, period, mode, lk = datetime.today(), args[0], 'm', list(kwargs.keys())
    if len(args) > 1:
        if isinstance(args[1], str): mode = args[1][0].lower()
    if len(args) > 2: end = args[2]
    if 'end_date' in lk:
        if isinstance(kwargs['end_date'], datetime): end = kwargs['end_date']
        elif isinstance(kwargs['end_date'], str):
            try:
                end = datetime.strptime(kwargs['end_date'], '%Y-%m-%d')
            except: pass
    if 'mode' in lk:
        if isinstance(kwargs['mode'], str): mode = kwargs['mode'][0].lower()
    if 'period' in lk:
        if isinstance(kwargs['period'], int): period = kwargs['period']
        elif isinstance(kwargs['period'], float): period = int(kwargs['period'])
        elif isinstance(kwargs['period'], str):
            try:
                period = int(float(kwargs['period']))
            except: pass
    if mode == 'y':
        period = int(round(period * 12, 0))
        mode = 'm'
    if mode == 'm':
        y, m = end.year, end.month
        if period > 12:
            y -= int(period / 12)
            period %= 12
        if m - period <= 0:
            y -= 1
            m += 12
        m -= period
        return datetime.strptime('{}-{}-01'.format(y, m), '%Y-%m-%d')
    if mode == 'd':
        eo = end.toordinal()
        return datetime.fromordinal(eo - period)

def web_collect(*args, **kwargs):
    src, lk, period, end, res = 'yahoo', list(kwargs.keys()), 1, datetime.today(), {}
    if args:
        code = args[0]
        if len(args) > 1: period = args[1]
    if 'code' in lk: code = kwargs['code']
    if 'period' in lk: period = kwargs['period']
    if 'source' in lk: src = kwargs['source']
    if src == 'yahoo':
        if isinstance(code, int):code = '{:04d}.HK'.format(code)
        elif isinstance(code, list):code = ['{:04d}.HK'.format(_) for _ in code]
    start = get_start(period)
    if start:
        if isinstance(code, str):
            dp = data.DataReader(code, src, start, end)
            res[code] = dp.transpose().to_dict()
        elif isinstance(code, list):
            dp = data.DataReader(code, src, start, end)
            for c in code:
                res[c] = dp.minor_xs(c).transpose().to_dict()
        return res

def wap(*args, **kwargs):
    """
Obtain daily update thru API (Yahoo) and update 'local' database.
    """
    ic, aid, kw = 0, get_stored_eid(), kwargs
    lkw = list(kw.keys())
    conn = lite.connect(filepath(db_name))
    conn.row_factory = lite.Row
    istr = "INSERT INTO {} ({}) VALUES ({})"
    if args:
        if isinstance(args[0], int): aid = [args[0]]
        elif isinstance(args[0], str):
            try: aid = [int(float(args[0]))]
            except: pass
        elif isinstance(args[0], float): aid = [int(args[0])]
        elif isinstance(args[0], list): aid = args[0]
    if 'equities_id' in lkw:
        if isinstance(kw['equities_id'], int): aid = [kw['equities_id']]
        elif isinstance(kw['equities_id'], str):
            try: aid = [int(float(kw['equities_id']))]
            except: pass
        elif isinstance(kw['equities_id'], float): aid = [int(kw['equities_id'])]
        elif isinstance(kw['equities_id'], list): aid = kw['equities_id']
    wdp = web_collect(aid)
    for _ in aid:
        idate = []
        atd = [i['date'] for i in conn.cursor().execute("SELECT {} FROM {} WHERE eid={:d}".format(', '.join(datafields + ['date']), db_table, _)).fetchall()]
        iw = wdp['{:04d}.HK'.format(_)]
        tdl = list(iw.keys())
        idate.extend([i for i in tdl if '{:%Y-%m-%d}'.format(i) not in atd])
        if idate:
            for i in idate:
                wi = wdp['{:04d}.HK'.format(_)][i]
                try:
                # if wi['Volume']:
                    im = {'eid':_}
                    im['date'] = "'{:%Y-%m-%d}'".format(i)
                    for f in datafields:
                        if f == 'volume':
                            im[f] = int(wi[f.capitalize()])
                        else:
                            im[f] = wi[f.capitalize()]
                    if wi['Volume'] >= 0:
                        imk = list(im.keys())
                        conn.cursor().execute(istr.format(db_table, ','.join(imk), ','.join(['{{{}}}'.format(j) for j in imk])).format(**im))
                        conn.commit()
                        ic += 1
                except: pass
    conn.close()
    return ic

def wam(*args, **kwargs):
    count, ae = 0, get_stored_eid()
    we, conn = web_collect(ae), lite.connect(filepath('Securities'))
    for _ in ae:
        df = dictfcomp(we['{:04d}.HK'.format(_)], pstored(_))
        if df: count += u2lite(_, df, conn)
    conn.close()
    return count

def append(*args, **kwargs):
    """
    Sequential update database with 'folder' directory.
    First positional or 'folder' argument is sub-folder name (default: 'csv'), and
    'wipe' argument initiate plunge file after new value stored (default: True).
    Two ways to use this method to append data to sqlite3 database,
    1). Download .csv format data file with corresponding varied stock code for finance.yahoo.com search to 'Download' folder, file will automatically 'wipe' off from 'Download' folder, and
    2). Same as the second way, but with 'csv' folder instead of 'Download' folder.
    """
    folder, wipe = 'csv', True
    if args:
        if isinstance(args[0], str): folder = args[0]
    try:
        lkkeys = list(kwargs.keys())
        if 'folder' in lkkeys: folder = kwargs['folder']
        if 'wipe' in lkkeys: wipe = kwargs['wipe']
    except: pass
    cp = find_csv_path()
    if cp:
        nr, af = 0, []
        for _ in listdir(cp):
            if path.isfile(sep.join((cp, _))):
                tfl = _.split('.')
                if tfl[-1] == folder and len(tfl[:-1]) == 2: af.append(_)
        for _ in af:
            d = Equities('.'.join(_.split('.')[:-1]))
            ld = len(d._Equities__data)
            if ld != 0:
                nr += d.store()
                if wipe: remove(sep.join((cp, _)))
        return nr

class Equities(object):
    """
    Worker object for manipulate and transfer data from file system to database.
    """
    def __init__(self, *args, **kwargs):
        if args:
            if isinstance(args[0], str): self.code = args[0]
        try:
            lkkeys = list(kwargs.keys())
            if 'code' in lkkeys: self.code = kwargs['code']
            self.__db_fullpath = filepath(db_name)
            self.__csv_fullpath = find_csv_path()
            if 'path' in lkkeys:
                kpath = kwargs['path']
                if isinstance(kpath, str): self.__csv_fullpath = kpath
                if isinstance(kpath, dict):
                    if 'db' in list(kpath.keys()): self.__db_fullpath = kpath['db']
                    if 'csv' in list(kpath.keys()): self.__csv_fullpath = kpath['csv']
        except: pass
        self.conn = lite.connect(self.__db_fullpath)
        self.conn.row_factory = lite.Row
        self.fields, self.values = [], []
        self.__data = self.get(self.code)
        self.__stored_data = self.conn.cursor().execute("SELECT * FROM %s WHERE eid=%i ORDER BY date ASC" % (db_table, int(self.code.split('.')[0]))).fetchall()

    def __del__(self):
        self.conn.close()
        self.conn = self.fields = self.values = self.code = self.__data = self.__stored_data = self.__csv_fullpath = self.__db_fullpath = None
        del self.fields, self.values, self.conn, self.code, self.__data, self.__stored_data, self.__csv_fullpath, self.__db_fullpath

    def get(self, *args, **kwargs):
        """
Extract data in csv from file system,
first positional or 'code' named is name in file system.
        """
        if args: code = args[0]
        if 'code' in list(kwargs.keys()): code = kwargs['code']
        if self.__csv_fullpath:
            try:
                tmp = open(sep.join((self.__csv_fullpath, '.'.join((code, 'csv')))))
                rdata = [_[:-1] for _ in tmp.readlines()]
                tmp.close()
            except: pass
        else:
            end = datetime.today()
            m, y = end.month, end.year
            if m == 1:
                m += 12
                y -= 1
            m -= 1
            start = datetime.strptime('{0}-{1}-01'.format(y, m), '%Y-%m-%d')
            df = data.DataReader(code, 'yahoo', start, end)
            rdata = df.to_csv().split(linesep)[:-1]
        fields = rdata[0].split(',')
        i, values = 1, []
        while i < len(rdata):
            tmp = rdata[i].split(',')
            try:
                tmp[0] = datetime.strptime(tmp[0], '%Y-%m-%d').date()
                tmp[-1] = int(tmp[-1])
                j = 1
                while j < len(tmp) - 1:
                    tmp[j] = float(tmp[j])
                    j += 1
                values.append(tmp)
            except: pass
            i += 1
        el  = ['Adj Close']
        self.fields.extend([_ for _ in fields if _ not in el])
        i, tmp  = 0, []
        while i < len(values):
            self.values.append(values[i])
            hdr = {}
            for _ in self.fields:
                j = fields.index(_)
                hdr[fields[j].lower()] = values[i][j]
            tf, c, _ = [hdr[_] for _ in ['open', 'high', 'low', 'close']], [], 0
            while _ < len(tf) - 1:
                r = False
                if tf[_] == tf[_+1]: r = True
                c.append(r)
                _ += 1
            c.append(hdr['volume'] == 0)
            if not reduce((lambda x, y: x and y), c): tmp.append(hdr)
            i += 1
        return tmp

    def store(self, *args, **kwargs):
        """
Transfer data to first positional argument database 'table_name' (default: 'db_table').
        """
        eid, table_name = int(self.code.split('.')[0]), db_table
        if args: table_name = args[0]
        if 'name' in list(kwargs.keys()): table_name = kwargs['name']
        i, sd, nd = 0, [datetime.strptime(_['date'], '%Y-%m-%d').date() for _ in self.__stored_data], []
        fields = [_.lower() for _ in self.fields]
        fields.insert(0, 'eid')
        rfields = ['%s=?'% _ for _ in fields[2:]]
        iqstr = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ','.join(fields), ','.join(['?' for _ in fields]))
        while i < len(self.__data):
            _, tmp = self.__data[i], {}
            if _['date'] in sd:
                tmp['table'] = 'records'
                hdr = self.conn.cursor().execute("SELECT * FROM {table} WHERE eid={eid} AND date='{date}'".format(**{'table':tmp['table'], 'eid':eid, 'date':_['date']})).fetchone()
                tmp['id'] = hdr['id']
                ufvd = [(j, _[j]) for j in list(_.keys()) if j not in ['eid', 'date', 'id']]
                hd = [(j, hdr[j]) for j in list(hdr.keys()) if j not in ['eid', 'date', 'id']]
                if not hd == ufvd:
                    tmp['set'] = ','.join(['{0}={1}'.format(*j) for j in ufvd])
                    self.conn.execute("UPDATE {table} SET {set} WHERE id={id}".format(**tmp))
                    self.conn.commit()
            else: nd.append(_)
            i += 1
        dl = []
        for i in nd:
            tmp = (int(self.code.split('.')[0]),)
            for j in fields[1:]:
                if j == 'date': tmp += (i[j].strftime('%Y-%m-%d'),)
                else: tmp += (i[j],)
            dl.append(tmp)
        self.conn.cursor().executemany(iqstr, dl)
        self.conn.commit()
        return len(nd)
