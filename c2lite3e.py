from utilities import filepath
from datetime import datetime
import sqlite3 as lite

db_name, db_table = 'Securities', 'records'

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
        fields, vh = ['eid'], []
        fields.extend([_.lower() for _ in self.fields])
        for j in range(len(fields)):
            if fields[j] == 'date': vh.append("'%s'")
            elif fields[j] in ('volume', 'eid'): vh.append('%i')
            else: vh.append('%f')
        sqlstr = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ','.join(fields), ','.join(vh))
        while i < len(self.__data):
            _ = self.__data[i]
            if _['date'] not in sd: nd.append(_)
            i += 1
        i, vs = 0, []
        while i < len(nd):
            v = [eid]
            v.extend([_ for _ in list(nd[i].values())])
            for j in range(len(fields)):
                if fields[j] == 'date':
                    v[j] = v[j].strftime('%Y-%m-%d')
            vs.append(tuple(v))
            i += 1
        if vs: [self.conn.cursor().execute(sqlstr % _) for _ in vs]
        self.conn.commit()
        return i
