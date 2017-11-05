db_name, db_table = 'Securities', 'records'
him = getattr(__import__('handy'), 'him')
iml = [{'utilities':('filepath',)}, {'datetime':('datetime',)}, {'os':('listdir', 'linesep', 'sep', 'path', 'remove')}, {'sys':('platform',)}, {'pandas_datareader':('data',)}, {'functools':('reduce',)}, ({'sqlite3':()}, "alias='lite'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']" % (_, _))

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

def insert(*args, **kwargs):
    pass

def amend(* args, **kwargs):
    counter = 0
    conn = lite.connect(filepath('Securities'))
    conn.row_factory = lite.Row
    end = datetime.today()
    m, y = end.month, end.year
    if m == 12:
        y -= 1
        m += 12
    m -= 1
    start = datetime.strptime('{}-{}-01'.format(y, m), '%Y-%m-%d')
    rid = ['{:04d}.HK'.format(_['eid']) for _ in conn.cursor().execute("SELECT DISTINCT {0} FROM {1} ORDER BY {0} ASC".format('eid', 'records')).fetchall()]
    dp = data.DataReader(rid, 'yahoo', start, end)
    for r in rid:
        rh = dp.minor_xs(r).to_csv().split(linesep)[:-2]
        fields = rh[0].split(',')
        dv = []
        for rhs in rh[1:]:
            hdr, rv = {}, rhs.split(',')
            for f in fields:
                if f == 'date': hdr[f] = '{}'.format(rv[fields.index(f)])
                elif f == 'volume': hdr[f] = int(float(rv[fields.index(f)]))
                else: hdr[f] = float(rv[fields.index(f)])
            dv.append(hdr)
        for d in dv:
            sid = conn.cursor().execute("SELECT {} FROM {} WHERE date='{}' AND eid={:d}".format('id', 'records', d['date'], int(r.split('.')[0]))).fetchone()['id']
            if sid:
                datafields = ['open', 'high', 'low', 'close', 'volume']
                sv = conn.cursor().execute("SELECT {} FROM {} WHERE id={:d}".format(','.join(datafields), 'records', sid)).fetchone()
                if not reduce((lambda x, y: x and y), [sv[_] == d[_] for _ in datafields]):
                    uvstr = ','.join(['{0}={{{0}}}'.format(_) for _ in datafields])
                    conn.cursor().execute("UPDATE {} SET {} WHERE id={}".format('records', uvstr, sid).format(d))
                    conn.commit()
                    counter += 1
    return counter

def append(*args, **kwargs):
    """
    Sequential update database with 'folder' directory.
    First positional or 'folder' argument is sub-folder name (default: 'csv'), and
    'wipe' argument initiate plunge file after new value stored (default: True).
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
    else:
        i_counter = 0
        conn = lite.connect(filepath(db_name))
        conn.row_factory = lite.Row
        end = datetime.today()
        m, y = end.month, end.year
        if m == 1:
            y -= 1
            m += 12
        m -= 1
        start = datetime.strptime('{}-{}-01'.format(y, m), '%Y-%m-%d')
        ae = conn.cursor().execute("SELECT DISTINCT {0} FROM {1} ORDER BY {0} ASC".format('eid', db_table)).fetchall()
        te = conn.cursor().execute("SELECT DISTINCT {0} FROM {1} WHERE date='{2}'".format('eid', db_table, end.strftime('%Y-%m-%d'))).fetchall()
        ae = ['{:04d}.HK'.format(_['eid']) for _ in ae]
        te = ['{:04d}.HK'.format(_['eid']) for _ in te]
        lo = [_ for _ in ae if _ not in te]
        df = data.DataReader(lo, 'yahoo', start, end)
        hdr = {}
        for _ in lo:
            all_record = conn.cursor().execute("SELECT date FROM {0} WHERE eid={1} ORDER BY date DESC".format(db_table, int(_.split('.')[0]))).fetchall()
            hdr[_] = df.minor_xs(_).to_csv().split(linesep)[:-2]
        for _ in list(hdr.keys()):
            tmp = hdr[_][0].split(',')
            fields = [_.lower() for _ in tmp]
            fields.append('eid')
            rfields = [_ for _ in fields if _ != 'adj close']
            vfields = []
            for j in rfields:
                if j == 'date': vfields.append("'{{{}}}'".format(j))
                else: vfields.append('{{{}}}'.format(j))
            uqstr, iqstr, values = "UPDATE {} SET {{}} WHERE id={{:d}}".format(db_table), "INSERT INTO {} ({}) VALUES ({})".format(db_table, ','.join(rfields), ','.join(vfields)), []
            for i in hdr[_][1:]:
                value, temp = i.split(','), {}
                for j in fields:
                    if j == 'date': temp[j] = value[fields.index(j)]
                    elif j in ['eid', 'volume']:
                        if j == 'eid': temp[j] = int(_.split('.')[0])
                        else: temp[j] = int(float(value[fields.index(j)]))
                    elif j in ['open', 'high', 'low', 'close']: temp[j] = float(value[fields.index(j)])
                if datetime.strptime(temp['date'], '%Y-%m-%d') in [datetime.strptime(_['date'], '%Y-%m-%d') for _ in all_record]:
                    dfields = ['id', 'open', 'high', 'low', 'close', 'volume']
                    sdata = conn.cursor().execute("SELECT {} FROM {} WHERE date='{}' AND eid={:d}".format(','.join(dfields), db_table, temp['date'], int(_.split('.')[0]))).fetchone()
                    rid = sdata['id']
                    tc = reduce((lambda x, y: x and y), [temp[_] == sdata[_] for _ in dfields if _ != 'id'])
                    ustr = ','.join(['{0}={{{0}}}'.format(_) for _ in dfields if _ != 'id'])
                    uqstr = uqstr.format(ustr, rid)
                    if not tc:
                        conn.cursor().execute(uqstr.format(**temp))
                        conn.commit()
                else: values.append(temp)
            for b in values:
                conn.cursor().execute(iqstr.format(**b))
                conn.commit()
                i_counter += 1
        return i_counter

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
