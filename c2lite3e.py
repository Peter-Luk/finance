from utilities import filepath
from datetime import datetime
import sqlite3 as lite

db_name, db_table = 'Securities', 'records'
conn = lite.connect(filepath(db_name))
conn.row_factory = lite.Row

class Equities(object):
    def __init__(self, *args, **kwargs):
        self.digits = 4
        if args: self.code = args[0]
        if len(args) > 1: self.digits = int(args[1])
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        if 'digits' in list(kwargs.keys()): self.digits = int(kwargs['digits'])
        self.__data = self.get(self.code, self.digits)
        self.__stored_data = conn.cursor().execute("SELECT * FROM %s WHERE eid=%i ORDER BY date ASC" % (db_table, int(self.code.split('.')[0]))).fetchall()

    def __del__(self):
        self.code = self.digits = self.__data = self.__stored_data = None
        del self.code, self.digits, self.__data, self.__stored_data

    def get(self, *args, **kwargs):
        digits = 2
        if 'code' in list(kwargs.keys()): self.code = kwargs['code']
        if args: code = args[0]
        if len(args) > 1: digits = int(args[1])
        if 'code' in list(kwargs.keys()): code = kwargs['code']
        if 'digits' in list(kwargs.keys()): digits = int(kwargs['digits'])
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
                    tmp[j] = round(float(tmp[j]), digits)
                    j += 1
                values.append(tmp)
            except: pass
            i += 1
        el  = ['Adj Close']
        rfl = [_ for _ in fields if _ not in el]
        i, tmp  = 0, []
        while i < len(values):
            hdr = {}
            for _ in rfl:
                j = fields.index(_)
                hdr[fields[j]] = values[i][j]
            tmp.append(hdr)
            i += 1
        return tmp

    def put(self, *args, **kwargs):
        table_name = db_table
        if args: table_name = args[0]
        if 'name' in list(kwargs.keys()): table_name = kwargs['name']
        isql, i = [], 0
        while i < len(self.__data):
            k, v, s = [_.lower() for _ in list(self.__data[i].keys())], list(self.__data[i].values()), []
            for j in range(len(k)):
                if k[j] == 'date':
                    v[j] = v[j].strftime('%Y-%m-%d')
                    s.append("'%s'")
                elif k[j] == 'volume': s.append('%i')
                else: s.append('%f')
            k.append('eid')
            v.append(int(self.code.split('.')[0]))
            s.append('%i')
            tl = [table_name, ','.join(k)]
            tl.extend(v)
            isql.append("INSERT INTO %s (%s) VALUES (" + ','.join(s) + ")" % tuple(tl))
            i += 1
        return isql
