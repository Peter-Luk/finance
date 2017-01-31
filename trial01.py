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

    def __gt0(self, x):
        if x > 0:return True
        return False

    def __lt0(self, x):
        if x < 0:return True
        return False

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
        res, r_date, i, hdr = {}, [], 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] in r_date:
                if option.upper() == 'C':hdr[self.__data[i]['date']] = self.__data[i]['close']
                if option.upper() == 'HL':
                    hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low']])
                if option.upper() == 'F':
                    hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']])
            else:
                r_date.append(self.__data[i]['date'])
                if option.upper() == 'C':hdr[self.__data[i]['date']] = self.__data[i]['close']
                if option.upper() == 'HL':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low']])
                if option.upper() == 'F':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']])
            i += 1

        res[r_date[period - 1]] = mean([hdr[x] for x in r_date[:period]])
        for d in r_date[period:]:res[d] = (res[r_date[r_date.index(d) - 1]] * (period - 1) + hdr[d]) / period

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def KAMA(self, **args):
        date, slow, period = datetime.today().strftime('%Y-%m-%d'), rnd(self.__period * gr), self.__period
        fast = rnd(period / gr)
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'slow' in args.keys():slow = args['slow']
        if 'fast' in args.keys():fast = args['fast']
        res, trade_day, tr, i, hdr, er, sc = {}, [], {}, 0, {}, {}, {}

        while i < len(self.__data):
            if self.__data[i]['date'] in trade_day:
                if self.__data[i]['session'] == 'A':tr[self.__data[i]['date']] = self.__data[i]['close']
            else:
                trade_day.append(self.__data[i]['date'])
                tr[self.__data[i]['date']] = self.__data[i]['close']
            i += 1

        for i in range(period, len(trade_day)):
            ch, vo = abs(tr[trade_day[i]] - tr[trade_day[i-period]]), sum([abs(tr[trade_day[x]] - tr[trade_day[x - 1]]) for x in range(i-period, i)])
            er[trade_day[i]] = ch / float(vo)

        for i in range(slow, len(trade_day)):
            sfc, ssc = 2. / (fast + 1), 2. / (slow + 1)
            sc[trade_day[i]] = (er[trade_day[i]] * (sfc - ssc) + ssc) ** 2

        for i in range(slow, len(trade_day)):
            if i == slow:res[trade_day[i]] = self.EMA(date=trade_day[i])
            else:res[trade_day[i]] = res[trade_day[i - 1]] + sc[trade_day[i]] * (tr[trade_day[i]] - res[trade_day[i - 1]])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def RSI(self, **args):
        date, period = datetime.today().strftime('%Y-%m-%d'), self.__period
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        res, trade_day, tr, i, hdr = {}, [], {}, 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] in trade_day:
                if self.__data[i]['session'] == 'A':tr[self.__data[i]['date']] = self.__data[i]['close']
            else:
                trade_day.append(self.__data[i]['date'])
                tr[self.__data[i]['date']] = self.__data[i]['close']
            i += 1

        i = 1
        while i < len(trade_day):
            hdr[trade_day[i]] = tr[trade_day[i]] - tr[trade_day[i - 1]]
            i += 1

        i, ag, al = period, {}, {}
        trade_day = list(hdr.keys())
        trade_day.sort()
        for i in range(period, len(trade_day)):
            if i == period:
                ag[trade_day[i]], al[trade_day[i]] = sum(list(filter(self.__gt0, [hdr[trade_day[j]] for j in range(i - period, i)]))) / period, abs(sum(list(filter(self.__lt0, [hdr[trade_day[j]] for j in range(i - period, i)])))) / period
            else:
                if hdr[trade_day[i]] > 0:
                    ag[trade_day[i]] = (ag[trade_day[i - 1]] * (period - 1) + hdr[trade_day[i]]) / period
                    al[trade_day[i]] = al[trade_day[i - 1]] * (period - 1) / period
                if hdr[trade_day[i]] < 0:
                    ag[trade_day[i]] = ag[trade_day[i - 1]] * (period - 1) / period
                    al[trade_day[i]] = (al[trade_day[i - 1]] * (period - 1) + abs(hdr[trade_day[i]])) / period
            res[trade_day[i]] = 100 - 100 / ( 1 + float(ag[trade_day[i]]) / al[trade_day[i]])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def SMA(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, i, hdr = {}, [], 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] in r_date:
                if option.upper() == 'C':hdr[self.__data[i]['date']] = self.__data[i]['close']
                if option.upper() == 'HL':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low']])
                if option.upper() == 'F':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']])
            else:
                r_date.append(self.__data[i]['date'])
                if option.upper() == 'C':hdr[self.__data[i]['date']] = self.__data[i]['close']
                if option.upper() == 'HL':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low']])
                if option.upper() == 'F':hdr[self.__data[i]['date']] = mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']])
            i += 1

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = mean([hdr[r_date[x]] for x in range(i, period + i)])

        rkeys = list(res.keys())
        rkeys.sort()
        if date in rkeys:return res[date]
        return res[rkeys[-1]]

    def WMA(self, **args):
        date, period, option = datetime.today().strftime('%Y-%m-%d'), self.__period, 'C'
        if 'date' in args.keys():date = args['date']
        if 'period' in args.keys():period = args['period']
        if 'option' in args.keys():option = args['option']
        res, r_date, i, hdr = {}, [], 0, {}

        while i < len(self.__data):
            if self.__data[i]['date'] in r_date:
                if option.upper() == 'C':hdr[self.__data[i]['date']] = (self.__data[i]['close'], self.__data[i]['volume'])
                if option.upper() == 'HL':hdr[self.__data[i]['date']] = (mean([self.__data[i]['high'], self.__data[i]['low']]), self.__data[i]['volume'])
                if option.upper() == 'F':hdr[self.__data[i]['date']] = (mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']]), self.__data[i]['volume'])
            else:
                r_date.append(self.__data[i]['date'])
                if option.upper() == 'C':hdr[self.__data[i]['date']] = (self.__data[i]['close'], self.__data[i]['volume'])
                if option.upper() == 'HL':hdr[self.__data[i]['date']] = (mean([self.__data[i]['high'], self.__data[i]['low']]), self.__data[i]['volume'])
                if option.upper() == 'F':hdr[self.__data[i]['date']] = (mean([self.__data[i]['high'], self.__data[i]['low'],self.__data[i]['open'], self.__data[i]['close']]), self.__data[i]['volume'])
            i += 1

        for i in range(len(r_date) - period):
            res[r_date[period + i]] = sum([hdr[r_date[x]][0] * hdr[r_date[x]][-1] for x in range(i, period + i)]) / float(sum([hdr[r_date[x]][-1] for x in range(i, period + i)]))

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

def summary(**args):
    o_format = 'raw'
    if 'format' in args.keys():
        o_format = args['format'].lower()
        from tags import HTML, TITLE, TABLE, TH, TR, TD
    if 'code' in args.keys():
        f_code = args['code'].upper()
        if o_format == 'html':hdr = TITLE("`%s` analyse" % code)
        mf = I2(code=f_code)
        period, tday = mf._I2__period, mf.trade_day
        ltd = len(tday)
        if ltd > rnd(period * gr):
            i_fields = ('Date', 'SMA', 'EMA', 'WMA', 'KAMA', 'RSI')
            if o_format == 'html':
                th = TH(TR('\n'.join([str(TD(x)) for x in i_fields])))
            else:hdr = '\t\t'.join(i_fields)
            for i in range(rnd(period * gr), ltd):
                i_values, trs = [], []
                for x in i_fields[1:]:
                    i_values.append(eval('mf.%s(date="%s")' % (x, tday[i])))
                if o_format == 'html':
                    trs.append(TR('\n'.join([str(TD(x)) for x in ((tday[i],) + tuple(i_values))])))
                else:
                    hdr += '\t'.join(('\n%s:',) + tuple(['%0.3f' for k in i_fields[1:]])) % ((tday[i],) + tuple(i_values))
        else:
            i_fields = ('Date', 'SMA', 'EMA', 'WMA', 'RSI')
            if o_format == 'html':
                th = TH(TR('\n'.join([str(TD(x)) for x in i_fields])))
            else:hdr = '\t\t'.join(i_fields)
            for i in range(period, ltd):
                i_values, trs = [], []
                for x in i_fields[1:]:
                    i_values.append(eval('mf.%s(date="%s")' % (x, tday[i])))
                if o_format == 'html':
                    trs.append(TR('\n'.join([str(TD(x)) for x in ((tday[i],) + tuple(i_values))])))
                else:
                    hdr += '\t'.join(('\n%s:',) + tuple(['%0.3f' for k in i_fields[1:]])) % ((tday[i],) + tuple(i_values))
        return hdr
