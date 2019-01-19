import pref
pd, np, db, yf, datetime, sleep = pref.y2n
from utilities import filepath, mtf
from nta import Viewer, hsirnd
from alchemy import AS

class Futures(AS, Viewer):
    periods = pref.periods['Futures']
    def __init__(self, code):
        # pF = pref.db['Futures']
        # ae = AS(pF['name'])
        # self.columns = ae.columns
        # self.connect = ae.connect
        # self.table = ae.table
        # self.code = code
        self._conf = pref.db['Futures']
        # engine = db.create_engine(f"sqlite:///{filepath(self._conf['name'])}")
        ae = AS(self._conf['name'])
        self.table = ae.table
        # self.rc = db.Table(self._conf['table'], db.MetaData(), autoload=True, autoload_with=engine).columns
        self.rc = ae.columns
        # self.__conn = engine.connect()
        self.__conn = ae.connect
        rc = entities(self._conf['name'])
        if code.upper() not in rc: code = rc[-1]
        self.code = code
        self.data = self.combine(code, self._conf['freq'])
        self.view = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self._conf = self.rc = self.__conn = self.data = self.view = self.date = self.close = None
        del(self._conf, self.rc, self.__conn, self.data, self.view, self.date, self.close)

    def __call__(self, enhanced=True):
        if enhanced: return pd.DataFrame({'proposed':[self.best_quote(), self.best_quote('sell')]}, index=['buy','sell'])
        return self.close

    def append(self, values, conditions):
        hdr = {self.rc.code:self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.rc]: hdr[_] = values[_]
        query = self.table.insert()
        trans = self.__conn.begin()
        self.__conn.execute(query, [hdr])
        trans.commit()

    def remove(self, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.rc]:
                    if _ == 'date': hdr.append(f"self.rc.{_}=='{conditions[_]}'")
                    else: hdr.append(f"self.rc.{_}=={conditions[_]}")
            query = db.select([self.rc.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.__conn.execute(query).scalar()
            except: pass
        query = self.table.delete().where(self.rc.id==obtain_id(conditions))
        trans = self.__conn.begin()
        self.__conn.execute(query)
        trans.commit()

    def amend(self, values, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.rc]:
                    if _ in ['date', 'code', 'session']: hdr.append(f"self.rc.{_}=='{conditions[_]}'")
                    else: hdr.append(f"self.rc.{_}=={conditions[_]}")
            query = db.select([self.rc.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.__conn.execute(query).scalar()
            except: pass
        hdr = {}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.rc]: hdr[_] = values[_]
        query = self.table.update().values(hdr).where(self.rc.id==obtain_id(conditions))
        trans = self.__conn.begin()
        self.__conn.execute(query)
        trans.commit()

    def ma(self, raw=None, period=periods['simple'], favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self.view.ma(raw, period, favour, req_field, programmatic)

    def kama(self, data=None, period=periods):
        if not data: data = self.data
        return self.view.kama(data, period)

    def bbw(self, raw=None, period=periods['simple'], req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self.view.bbw(raw, period, req_field, programmatic)

    def bb(self, raw=None, period=periods['simple'], req_field='c', programmatic=False):
        if not raw: raw = self.data
        return self.view.bb(raw, period, req_field, programmatic)

    def rsi(self, raw=None, period=periods['simple']):
        if not raw: raw = self.data
        return self.view.rsi(raw, period)

    def kc(self, data=None, period=periods):
        if not data: data = self.data
        return self.view.kc(data, period)

    def atr(self, raw=None, period=periods['atr'], programmatic=False):
        if not raw: raw = self.data
        return self.view.atr(raw, period, programmatic)

    def trp(self, data=None, period=periods['atr']):
        if not data: data = self.data
        return self.view.trp(data, period)

    def adx(self, data=None, period=periods['adx']):
        if not data: data = self.data
        return self.view.adx(data, period)

    def mas(self, period=periods):
        # if not data: data = self.data
        return self.view.mas(period)

    def idrs(self, period=periods):
        # if not data: data = self.data
        return self.view.idrs(period)

    def apz(self, raw=None, period=periods, df=None, programmatic=False):
        if not raw: raw = self.data
        return self.view.apz(raw, period, df, programmatic)

    def ovr(self, raw=None, period=periods,date=datetime.today().date()):
        if not raw: raw = self.data
        return self.view.ovr(raw, period, date)

    def ratr(self, raw=None, period=periods['atr']):
        if not raw: raw = self.data
        return self.view.ratr(raw, period)

    def combine(self, code=None, freq='bi-daily', dataframe=False):
        if not code: code = self.code
        if freq.lower() == 'bi-daily':
            res = []
            for _ in [__[0] for __ in self.__conn.execute(db.select([self.rc.date.distinct()]).where(self.rc.code==code).order_by(db.asc(self.rc.date))).fetchall()]:
                tmp = {}
                __ = self.__conn.execute(db.select([self.rc.session, self.rc.open, self.rc.high, self.rc.low, self.rc.close, self.rc.volume]).where(db.and_(self.rc.code==code, self.rc.date==_))).fetchall()
                p_ = pd.DataFrame(__)
                p_.columns = __[0].keys()
                p_.set_index('session', inplace=True)
                if len(p_) == 1:
                    try:
                        tmp['open'] = p_['open']['M']
                        tmp['high'] = p_['high']['M']
                        tmp['low'] = p_['low']['M']
                        tmp['close'] = p_['close']['M']
                        tmp['volume'] = p_['volume']['M']
                    except:
                        tmp['open'] = p_['open']['A']
                        tmp['high'] = p_['high']['A']
                        tmp['low'] = p_['low']['A']
                        tmp['close'] = p_['close']['A']
                        tmp['volume'] = p_['volume']['A']
                elif len(p_) == 2:
                    tmp['open'] = p_['open']['M']
                    tmp['high'] = p_['high'].max()
                    tmp['low'] = p_['low'].min()
                    tmp['close'] = p_['close']['A']
                    tmp['volume'] = p_['volume'].sum()
                tmp['date'] = _
                res.append(tmp)
            _ = pd.DataFrame(res)
            _.set_index(self._conf['index'], inplace=True)
            p_ = pd.DataFrame([_['open'], _['high'], _['low'], _['close'], _['volume']]).T
            if dataframe: return p_
            return {'Date': list(p_.index), 'Data': p_.values}

class Equities(Viewer):
    periods = pref.periods['Equities']
    def __init__(self, code, adhoc=False):
        self._conf = pref.db['Equities']
        engine = db.create_engine(f"sqlite:///{filepath(self._conf['name'])}")
        self.rc = db.Table(self._conf['table'], db.MetaData(), autoload=True, autoload_with=engine).columns
        self.__conn = engine.connect()
        if code not in entities(self._conf['name']): adhoc = True
        self.data = self.fetch(code, adhoc=adhoc)[code]
        self._v = Viewer(self.data)
        self.date = self.data['Date'][-1]
        self.close = self.data['Data'][-1, -2]

    def __del__(self):
        self._conf = self.rc = self.__conn = self.data = self._v = self.date = self.close = None
        del(self._conf, self.rc, self.__conn, self.data, self._v, self.date, self.close)

    def __call__(self, enhanced=True):
        if enhanced: return pd.DataFrame({'proposed':[self.best_quote(), self.best_quote('sell')]}, index=['buy','sell'])
        return self.close

    def fetch(self, code=None, start=None, table=pref.db['Equities']['table'], exclude=pref.db['Equities']['exclude'], years=4, adhoc=False, series=False):
        res = {}
        if not start:
            start = pd.datetime(pd.datetime.now().year - years, 1, 1)
        aid = [_ for _ in entities(self._conf['name']) if _ not in exclude]
        if code:
            if isinstance(code, (tuple, list)): aid = list(code)
            if isinstance(code, (int, float)):
                if int(code) == 5: aid = [11, int(code)]
                else: aid = [5, int(code)]
        if adhoc:
            while adhoc:
                ar = yf.download([f'{_:04d}.HK' for _ in aid], start, group_by='ticker')
                if len(ar): adhoc = not adhoc
                else:
                    print('Retry in 30 seconds')
                    sleep(30)
            for _ in aid:
                hdr = {}
                rd = ar[f'{_:04d}.HK']
                d = rd.T
                hdr['Date'] = [__.to_pydatetime() for __ in rd.index]
                hdr['Data'] = np.asarray(pd.concat([rd[__] for __ in [___ for ___ in d.index if ___ not in ['Adj Close']]], axis=1, join='inner', ignore_index=False))
                res[_] = hdr
        else:
            for _ in aid:
                query = db.select([self.rc.date, self.rc.open, self.rc.high, self.rc.low, self.rc.close, self.rc.volume]).where(self.rc.eid==_)
                if start:
                    query = db.select([self.rc.date, self.rc.open, self.rc.high, self.rc.low, self.rc.close, self.rc.volume]).where(db.and_(self.rc.eid==_, self.rc.date > start))
                hdr = self.__conn.execute(query).fetchall()
                p_ = pd.DataFrame(hdr)
                p_.columns = hdr[0].keys()
                p_.set_index(self._conf['index'], inplace=True)
                pp_ = pd.DataFrame([p_['open'], p_['high'], p_['low'], p_['close'], p_['volume']]).T
                res[_] = {'Date': list(pp_.index), 'Data': pp_.values}
        if series: return pd.Series(res)
        return res

    def ma(self, raw=None, period=periods['simple'], favour='s', req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self._v.ma(raw, period, favour, req_field, programmatic)

    def kama(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.kama(data, period)

    def bbw(self, raw=None, period=periods['simple'], req_field='close', programmatic=False):
        if not raw: raw = self.data
        return self._v.bbw(raw, period, req_field, programmatic)

    def bb(self, raw=None, period=periods['simple'], req_field='c', programmatic=False):
        if not raw: raw = self.data
        return self._v.bb(raw, period, req_field, programmatic)

    def rsi(self, raw=None, period=periods['simple']):
        if not raw: raw = self.data
        return self._v.rsi(raw, period)

    def kc(self, data=None, period=periods):
        if not data: data = self.data
        return self._v.kc(data, period=period)

    def atr(self, raw=None, period=periods['atr'], programmatic=False):
        if not raw: raw = self.data
        return self._v.atr(raw, period, programmatic)

    def trp(self, data=None, period=periods['atr']):
        if not data: data = self.data
        return self._v.trp(data, period)

    def adx(self, data=None, period=periods['adx']):
        if not data: data = self.data
        return self._v.adx(data, period)

    # def mas(self, data=None, period=periods):
    #     if not data: data = self.data
    #     return self._v.mas(data, period)
    def mas(self, period=periods):
        # if not data: data = self.data
        return self._v.mas(period)

    def idrs(self, period=periods):
        # if not data: data = self.data
        return self._v.idrs(period)

    def apz(self, raw=None, period=periods, df=None, programmatic=False):
        if not raw: raw = self.data
        return self._v.apz(raw, period, df, programmatic)

    def ovr(self, raw=None, period=periods,date=datetime.today().date()):
        if not raw: raw = self.data
        return self._v.ovr(raw, period, date)

    def ratr(self, raw=None, period=periods['atr']):
        if not raw: raw = self.data
        return self._v.ratr(raw, period)

def bqo(el, action='buy', adhoc=False, bound=True):
    def __process(e, action, bound):
        er, eo = e.ratr(), e.ovr()
        _ = er[(er > eo['min'].min()) & (er < eo['max'].max())]
        if action == 'buy':
            if bound:
                if _.size > 0:
                    if e.close < _.min(): return pd.Series([__ for __ in er if __ < e.close])
                    return pd.Series([__ for __ in _ if __ < e.close])
                return np.nan
            return er.min()
        if action == 'sell':
            if bound:
                if _.size > 0:
                    if e.close > _.max(): return pd.Series([__ for __ in er if __ > e.close])
                    return pd.Series([__ for __ in _ if __ > e.close])
                return np.nan
            return er.max()

    pF = pref.db['Futures']
    if isinstance(el, str) and el.upper() in entities(pF['name']):
        hdr = __process(Futures(el.upper()), action, bound)
        if action == 'buy':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.head()
            except: return hdr
        if action == 'sell':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.tail()
            except: return hdr

    if isinstance(el, int):
        hdr = __process(Equities(el, adhoc), action, bound)
        if action == 'buy':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.head()
            except: return hdr
        if action == 'sell':
            if isinstance(hdr, (float, int)): return hdr
            try: return hdr.tail()
            except: return hdr

    res = {}
    if isinstance(el, (list, tuple)):
        if bound:
            for _ in el:
                hdr = np.nan
                if isinstance (_, str):
                    if _.upper() in entities(pF['name']):
                        hdr = __process(Futures(_.upper()), action, bound)
                elif isinstance(_, int):
                    hdr = __process(Equities(_, adhoc), action, bound)
                try: res[_] = hdr.head()
                except: pass
                if action == 'sell':
                    try: res[_] = hdr.tail()
                    except: pass
            hdr = pd.DataFrame(res)
        else:
            il, __ = [], []
            for _ in el:
                if isinstance (_, str):
                    if _.upper() in entities(pF['name']):
                        __.append(__process(Futures(_.upper()), action, bound))
                        il.append(_)
                elif isinstance(_, int):
                    try:
                        __.append(__process(Equities(_, adhoc), action, bound))
                        il.append(_)
                    except: pass
            hdr = pd.DataFrame({action:__}, index=il)

        if hdr.empty: return None
        return hdr

def entities(dbname=None, series=False):
    pF, pE = pref.db['Futures'], pref.db['Equities']
    if not dbname: dbname = pE['name']
    engine = db.create_engine(f'sqlite:///{filepath(dbname)}')
    conn = engine.connect()
    if dbname == pF['name']:
        rc = db.Table(pF['table'], db.MetaData(), autoload=True, autoload_with=engine).columns
        query = db.select([rc.code.distinct()]).order_by(db.asc(rc.code))
    if dbname == pE['name']:
        rc = db.Table(pE['table'], db.MetaData(), autoload=True, autoload_with=engine).columns
        query = db.select([rc.eid.distinct()]).order_by(db.asc(rc.eid))
    __ = [_[0] for _ in conn.execute(query).fetchall()]
    if series: return pd.Series(__)
    return __
