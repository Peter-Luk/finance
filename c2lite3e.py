db_name, db_table = 'Securities', 'records'
him = getattr(__import__('handy'), 'him')
iml = [{'utilities':('filepath',)}, {'datetime':('datetime',)}, ({'sqlite3':()}, "alias='lite'")]
__ = him(iml)
for _ in list(__.keys()):exec("%s=__['%s']" % (_, _))
import os

def update(*args, **kwargs):
    nr, sd = 0, filepath('Securities')
    cp = sd.split(os.sep)[:-2]
    cp.append('csv')
    af = os.listdir(os.sep.join(cp))
    for i in af:
        d = Equities('.'.join(i.split('.')[:-1]))
        if len(d._Equities__data): nr += d.store()
    return nr

class Equities(object):
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
            tmp.append(hdr)
            i += 1
        return tmp

    def store(self, *args, **kwargs):
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
