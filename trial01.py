from sqlite3 import connect
from datetime import datetime
from statistics import mean
from utilities import gr, rnd, filepath
from SQLiteHelper import Query

db_name, db_table, req_fields = 'Futures', 'records', ('date','session','open','high','low','close')

class I2(Query):
    def __init__(self, **args):
        self.__Query, self.__period, self.__db, self.__table = Query, rnd(20 / gr), db_name, db_table
        if 'code' in args.keys():self.__code = args['code']
        if 'period' in args.keys():self.__period = args['period']
        if 'db_name' in args.keys():self.__db = args['db_name']
        if 'db_table' in args.keys():self.__table = args['db_table']

        self.__futures = self.__Query(connect(filepath(self.__db)))
        self.__data = self.__futures.show(self.__table, fields=req_fields, criteria={'code':self.__code.upper()})

    def __del__(self):
        self.__data = self.__futures = self.__code = self.__period = self.__db = self.__table = self.__Query = None
        del(self.__data)
        del(self.__futures)
        del(self.__code)
        del(self.__period)
        del(self.__db)
        del(self.__table)
        del(self.__Query)

    def atr(self, **args):
        date = datetime.today().strftime('%Y-%m-%d')
        if 'date' in args.keys():date = args['date']
        res, r_date, tr, i, hdr = {}, [], [], 0, {}

        tr.append([self.__data[0][0], self.__data[0][1], self.__data[0][-3] - self.__data[0][-2]])
        i += 1
        while i < len(self.__data):
            if self.__data[i][0] == self.__data[i - 1][0]:
                if self.__data[i][-3] > self.__data[i - 1][-3]:ma = self.__data[i][-3]
                else:ma = self.__data[i - 1][-3]
                if self.__data[i][-2] < self.__data[i - 1][-2]:mi = self.__data[i][-2]
                else:mi = self.__data[i - 1][-2]
            else:
                if self.__data[i][-3] > self.__data[i - 1][-1]:ma, mi = self.__data[i][-3], self.__data[i - 1][-1]
                elif self.__data[i][-2] < self.__data[i - 1][-1]:mi, ma = self.__data[i][-2], self.__data[i - 1][-1]
            tr.append([self.__data[i][0], self.__data[i][1], ma - mi])
            i += 1 
        i = 0
        while i < len(tr):
            hdr[tr[i][0]] = tr[i][-1]
            if tr[i][0] not in r_date:r_date.append(tr[i][0])
            i += 1

        res[r_date[self.__period - 1]] = mean([hdr[x] for x in r_date[:self.__period]])
        for d in r_date[self.__period:]:res[d] = (res[r_date[r_date.index(d) - 1]] * (self.__period - 1) + hdr[d]) / self.__period

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]
