db_name, db_table = 'Securities', 'records'
him = getattr(__import__('handy'), 'him')
iml = [{'utilities':('filepath',)}, {'datetime':('datetime',)}, {'os':('listdir', 'sep', 'path', 'remove')}, {'functools':('reduce',)}, ({'sqlite3':()}, "alias='lite'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']" % (_, _))

def update(*args, **kwargs):
    """
    Sequential update database with 'folder' directory.
    First positional or 'folder' argument is sub-folder name (default: 'csv'), and
    'wipe' argument initiate plunge file after new value stored (default: False).
    """
    folder, wipe = 'csv', False
    if args:
        if isinstance(args[0], str): folder = args[0]
    if 'folder' in list(kwargs.keys()): folder = kwargs['folder']
    if 'wipe' in list(kwargs.keys()): wipe = kwargs['wipe']
    nr, sd = 0, filepath(db_name)
    sdl = sd.split(sep)
    cp = sdl[:-2]
    cp.append(folder)
    if platform == 'linux':
        si = sdl.index('storage')
        cp = sdl[:si+1]
        cp.extend(['shared', 'Download'])
        cpl = listdir(cp)
        if cpl == 0:
            cp = sdl[:-2]
            cp.append(folder)
    cp = sep.join(cp)
    af = [_ for _ in listdir(cp) if path.isfile(sep.join((cp, _)))]
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
        if args: self.code = args[0]
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        self.conn = lite.connect(filepath(db_name))
        self.conn.row_factory = lite.Row
        self.fields, self.values = [], []
        self.__data = self.get(self.code)
        self.__stored_data = self.conn.cursor().execute("SELECT * FROM %s WHERE eid=%i ORDER BY date ASC" % (db_table, int(self.code.split('.')[0]))).fetchall()

    def __del__(self):
        self.conn.close()
        self.conn = self.fields = self.values = self.code = self.__data = self.__stored_data = None
        del self.fields, self.values, self.conn, self.code, self.__data, self.__stored_data

    def get(self, *args, **kwargs):
        """
Extract data in csv from file system,
first positional or 'code' named is name in file system.
        """
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        if args: code = args[0]
        if 'code' in list(kwargs.keys()): code = kwargs['code']
        tmp = open(filepath('.'.join((code, 'csv')), subpath='csv'))
        data = [_[:-1] for _ in tmp.readlines()]
        tmp.close()
        fields = data[0].split(',')
        i, values = 1, []
        while i < len(data):
            tmp = data[i].split(',')
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
            # tmp.append(hdr)
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
        iqstr = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ','.join(fields), ','.join(['?' for _ in fields]))
        while i < len(self.__data):
            _ = self.__data[i]
            if _['date'] not in sd: nd.append(_)
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
