import sqlite3 as lite
from utilities import gr, rnd, filepath
from datetime import datetime
from statistics import mean

db_name, db_table = 'Futures', 'records'

class I2:
    def __init__(self, **args):
        self.trade_day = []
        self.__period, self.__db, self.__table = rnd(20 / gr), db_name, db_table
        if 'code' in args.keys():self.__code = args['code']
        if 'period' in args.keys():self.__period = args['period']
        if 'db_name' in args.keys():self.__db = args['db_name']
        if 'db_table' in args.keys():self.__table = args['db_table']

        self.__conn = lite.connect(filepath(self.__db))
        self.__conn.row_factory = lite.Row
        self.__data = self.__conn.cursor().execute("SELECT * FROM %s WHERE code='%s'" % (self.__table, self.__code.upper())).fetchall()
        for i in range(len(self.__data)):
            if self.__data[i]['date'] not in self.trade_day:self.trade_day.append(self.__data[i]['date'])

    def __del__(self):
        self.__data = self.__data = self.__code = self.__period = self.__db = self.__table = self.trade_day = None
        del(self.__data)
        del(self.__code)
        del(self.__period)
        del(self.__db)
        del(self.__table)
        del(self.trade_day)

    def ATR(self, **args):
        date, period = datetime.today().strftime('%Y-%m-%d'), self.__period
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        res, r_date, tr, i, hdr = {}, [], [], 0, {}

        tr.append([self.__data[0]['date'], self.__data[0]['session'], self.__data[0]['high'] - self.__data[0]['low']])
        i += 1
        while i < len(self.__data):
            if self.__data[i]['date'] == self.__data[i - 1]['date']:
                if self.__data[i]['high'] > self.__data[i - 1]['high']:ma = self.__data[i]['high']
                else:ma = self.__data[i - 1]['high']
                if self.__data[i]['low'] < self.__data[i - 1]['low']:mi = self.__data[i]['low']
                else:mi = self.__data[i - 1]['low']
            else:
                if self.__data[i]['high'] > self.__data[i - 1]['close']:ma, mi = self.__data[i]['high'], self.__data[i - 1]['close']
                elif self.__data[i]['low'] < self.__data[i - 1]['close']:mi, ma = self.__data[i]['low'], self.__data[i - 1]['close']
            tr.append([self.__data[i]['date'], self.__data[i]['session'], ma - mi])
            i += 1

        i = 0
        while i < len(tr):
            hdr[tr[i][0]] = tr[i][-1]
            if tr[i][0] not in r_date:r_date.append(tr[i][0])
            i += 1

        res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
        for d in r_date[period:]:res[d] = (res[r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]) / period

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def EMA(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, tr, i, hdr = {}, [], [], 0, {}

        while i < len(self.__data):
            if option.upper() == 'C':tr.append([self.__data[i]['date'], self.__data[i]['session'], self.__data[i]['close']])
            if option.upper() == 'HL':tr.append([self.__data[i]['date'], self.__data[i]['session'], mean([self.__data[i]['high'], self.__data[i]['low']])])
            if option.upper() == 'F':tr.append([self.__data[i]['date'], self.__data[i]['session'], mean([self.__data[i]['open'], self.__data[i]['high'], self.__data[i]['low'], self.__data[i]['close']])])
            i += 1

        i = 0
        while i < len(tr):
            hdr[tr[i][0]] = tr[i][-1]
            if tr[i][0] not in r_date:r_date.append(tr[i][0])
            i += 1

        res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
        for d in r_date[period:]:res[d] = (res[r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]) / period

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def SMA(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, tr, i, hdr = {}, [], [], 0, {}

        while i < len(self.__data):
            if option.upper() == 'C':tr.append([self.__data[i]['date'], self.__data[i]['session'], self.__data[i]['close']])
            if option.upper() == 'HL':tr.append([self.__data[i]['date'], self.__data[i]['session'], mean([self.__data[i]['high'], self.__data[i]['low']])])
            if option.upper() == 'F':tr.append([self.__data[i]['date'], self.__data[i]['session'], mean([self.__data[i]['open'], self.__data[i]['high'], self.__data[i]['low'], self.__data[i]['close']])])
            i += 1

        i = 0
        while i < len(tr):
            hdr[tr[i][0]] = tr[i][-1]
            if tr[i][0] not in r_date:r_date.append(tr[i][0])
            i += 1

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = mean([hdr[r_date[x]] for x in range(i, period + i)])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def KC(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        width, base = self.ATR(date=date, period=period), self.EMA(date=date, period=period, option=option)
        return (rnd(base - width * gr), rnd(base + width * gr))

    def daatr(self, **args):
        date, period = None, 5
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if date:return self.ATR(date=date, period=period) - self.ATR(date=date)
        else: return self.ATR(period=period) - self.ATR()
