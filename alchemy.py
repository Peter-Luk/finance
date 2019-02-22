import pref
Path, os, sys, db, pd, datetime = pref.alchemy
from sqlalchemy.orm import mapper, sessionmaker
from utilities import filepath

eb, fb = pref.db['Equities'], pref.db['Futures']
if sys.platform == 'win32': home = str(Path.home())
if sys.platform in ['linux', 'linux2']:
    subpath = 'shared'
    if os.environ['EXTERNAL_STORAGE']: subpath = 'external-1'
    home = os.sep.join([str(Path.home()), 'storage', subpath])

def get_db(*args, **kwargs):
    if isinstance(args[0], str): db_path = os.sep.join([home, args[0]])
    if isinstance(args[0], tuple): db_path = os.sep.join((home,) + args[0])
    if isinstance(args[0], list):
        args[0].insert(0, home)
        db_path = os.sep.join(args[0])
    if kwargs:
        try:
            if isinstance(kwargs['exclude'], str): exclude = [kwargs['exclude']]
            if isinstance(kwargs['exclude'], tuple): exclude = list(kwargs['exclude'])
            if isinstance(kwargs['exclude'], list): exclude = kwargs['exclude']
            if exclude: return [f for f in os.listdir(db_path) if not ('.git' in f or f in exclude)]
        except: pass
    return [f for f in os.listdir(db_path) if '.git' not in f]

def load(*args):
    res, db_path, exclude = {}, ('data','sqlite3'), 'Sec12'
    db_name = get_db(db_path, exclude=exclude)
    if args:
        if isinstance(args[0], str): db_name = [args[0]]
        if isinstance(args[0], list): db_name = args[0]
        if isinstance(args[0], tuple): db_name = list(args[0])
    for dbn in db_name:
        exec('class {}(object): pass'.format(dbn))
        engine = db.create_engine('sqlite:///{}'.format(os.sep.join((home,) + db_path + (dbn,))))
        metadata = db.MetaData(engine)
        records = db.Table('records', metadata, autoload=True)
        eval('mapper({}, records)'.format(dbn))
        Session = sessionmaker(bind=engine)
        session = Session()
        if dbn in ['Futures', 'Securities']:
            ifs = ['date', 'open', 'high', 'low', 'close', 'volume']
            if dbn == 'Futures':
                [ifs.insert(0, _) for _ in ['session', 'code']]
            else:
                [ifs.insert(0, _) for _ in ['eid']]
        else: ifs = ['subject_id', 'date', 'time', 'sys', 'dia', 'pulse', 'remarks']
        ifs = ['.'.join([dbn, _]) for _ in ifs]
        data = eval("session.query({}).all()".format(','.join(ifs)))
        res[dbn] = {'engine':engine, 'table':records, 'session':session, 'data':data}
    return res

class AS(object):
    def __init__(self, name):
        self.db = name
        engine = db.create_engine(f"sqlite:///{filepath(self.db)}")
        self.__meta = db.MetaData()
        if self.db == fb['name']:
            self.table = db.Table(fb['table'], self.__meta, autoload=True, autoload_with=engine)
        else:
            self.table = db.Table(eb['table'], self.__meta, autoload=True, autoload_with=engine)
        self.columns = self.table.columns
        self.connect = engine.connect()

    def __del__(self):
        self.__meta = self.table = self.columns = self.connect = None
        del(self.__meta, self.table, self.columns, self.connect)

class AE(AS):
    def __init__(self, eid):
        pE = pref.db['Equities']
        ae = AS(pE['name'])
        self.columns = ae.columns
        self.connect = ae.connect
        self.table = ae.table
        self.code = eid
        self.data = self.acquire({'date':'>datetime(datetime.today().year - 4, 12, 31).date()'})
        self.close = self.data['Data'][-1, -2]
        self.yahoo_code = f'{eid:04d}.HK'

    def __del__(self):
        self.columns = self.connect = self.table = self.code = self.data = self.close  = self.yahoo_code = None
        del(self.columns, self.connect, self.table, self.code, self.data, self.close, self.yahoo_code)

    def __call__(self, dataframe=True):
        return self.acquire({'date':'>datetime(datetime.today().year - 4, 12, 31).date()'}, dataframe)

    def append(self, values, conditions):
        hdr = {self.columns.eid:self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.insert()
        trans = self.connect.begin()
        self.connect.execute(query, [hdr])
        trans.commit()

    def remove(self, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['eid'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    cs = f"self.columns.{_}=={conditions[_]}"
                    if _ == 'date': cs = f"self.columns.{_}=='{conditions[_]}'"
                    hdr.append(cs)
                    # if _ == 'date': hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    # else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        query = self.table.delete().where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def amend(self, values, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['eid'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    cs = f"self.columns.{_}=={conditions[_]}"
                    if _ == 'date': cs = f"self.columns.{_}=='{conditions[_]}'"
                    hdr.append(cs)
                    # if _ == 'date': hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    # else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        hdr = {}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.update().values(hdr).where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def acquire(self, conditions, dataframe=False):
        hdr = {'eid':f'=={self.code}'}
        for _ in conditions.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = conditions[_]
        query = db.select([self.columns.date, self.columns.open, self.columns.high, self.columns.low, self.columns.close, self.columns.volume]).where(eval('db.and_(' + ', '.join([f"self.columns.{_}{hdr[_]}" for _ in hdr.keys()]) + ')'))
        res = pd.DataFrame(self.connect.execute(query).fetchall(), columns=[__.capitalize() for __ in [f'{_}'.split('.')[-1] for _ in self.columns] if __ not in ['eid', 'id']])
        res.set_index('Date', inplace=True)
        if dataframe: return res
        return {'Date': list(res.index), 'Data': res.values}

class AF(AS):
    def __init__(self, code):
        self._conf = pref.db['Futures']
        ae = AS(self._conf['name'])
        self.columns = ae.columns
        self.connect = ae.connect
        self.table = ae.table
        self.code = code

    def __del__(self):
        self.columns = self.connect = self.table = self.code = None
        del(self.columns, self.connect, self.table, self.code)

    def __call__(self):
        return self.combine()

    def append(self, values, conditions):
        hdr = {self.columns.code:self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.insert()
        trans = self.connect.begin()
        self.connect.execute(query, [hdr])
        trans.commit()

    def remove(self, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    cs = f"self.columns.{_}=={conditions[_]}"
                    if _  in ['code', 'date']: cs = f"self.columns.{_}=='{conditions[_]}'"
                    hdr.append(cs)
                    # if _ in ['code', 'date']: hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    # else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        query = self.table.delete().where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def amend(self, values, conditions):
        def obtain_id(conditions=None):
            if not conditions: conditions = {'date':datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    cs = f"self.columns.{_}=={conditions[_]}"
                    if _ in ['code', 'session' 'date']: cs = f"self.columns.{_}=='{conditions[_]}'"
                    hdr.append(cs)
                    # if _ in ['date', 'code', 'session']: hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    # else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(eval('db.and_(' + ', '.join(hdr) +')'))
            try: return self.connect.execute(query).scalar()
            except: pass
        hdr = {}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]: hdr[_] = values[_]
        query = self.table.update().values(hdr).where(self.columns.id==obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def combine(self, freq='bi-daily', dataframe=True):
        code = self.code
        if freq.lower() == 'bi-daily':
            res = []
            for _ in [__[0] for __ in self.connect.execute(db.select([self.columns.date.distinct()]).where(self.columns.code==code).order_by(db.asc(self.columns.date))).fetchall()]:
                tmp = {}
                __ = self.connect.execute(db.select([self.columns.session, self.columns.open, self.columns.high, self.columns.low, self.columns.close, self.columns.volume]).where(db.and_(self.columns.code==code, self.columns.date==_))).fetchall()
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
            p_.index.name = p_.index.name.capitalize()
            p_.columns = [_.capitalize() for _ in p_.columns]
            if dataframe: return p_
            return {'Date': list(p_.index), 'Data': p_.values}
