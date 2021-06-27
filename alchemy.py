import pref
platform, environ, sep, listdir, db, pd, datetime = pref.alchemy
from utilities import filepath
from sqlalchemy.orm import mapper, sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime, text

Base = declarative_base()
Session = sessionmaker()
eb, fb = pref.db['Equities'], pref.db['Futures']
if platform == 'win32':
    home = environ.get('HOME')
if platform in ['linux', 'linux2']:
    subpath, home = 'shared', f"{environ.get('HOME')}"
    if 'EXTERNAL_STORAGE' in environ.keys():
        subpath = 'external-1'
        home = sep.join([environ.get('HOME'), 'storage', subpath])


def get_db(*args, **kwargs):
    if isinstance(args[0], str):
        db_path = sep.join([home, args[0]])
    if isinstance(args[0], tuple):
        db_path = sep.join((home,) + args[0])
    if isinstance(args[0], list):
        args[0].insert(0, home)
        db_path = sep.join(args[0])
    if kwargs:
        try:
            if isinstance(kwargs['exclude'], str):
                exclude = [kwargs['exclude']]
            if isinstance(kwargs['exclude'], tuple):
                exclude = list(kwargs['exclude'])
            if isinstance(kwargs['exclude'], list):
                exclude = kwargs['exclude']
            if exclude:
                return [f for f in listdir(
                    db_path) if not ('.git' in f or f in exclude)]
        except Exception:
            pass
    return [f for f in listdir(db_path) if '.git' not in f]


def load(*args):
    res, db_path, exclude = {}, ('data', 'sqlite3'), 'Sec12'
    db_name = get_db(db_path, exclude=exclude)
    if args:
        if isinstance(args[0], str):
            db_name = [args[0]]
        if isinstance(args[0], list):
            db_name = args[0]
        if isinstance(args[0], tuple):
            db_name = list(args[0])
    for dbn in db_name:
        exec('class {}(object): pass'.format(dbn))
        engine = db.create_engine('sqlite:///{}'.format(
            sep.join((home,) + db_path + (dbn,))))
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
        else:
            ifs = [
                    'subject_id', 'date', 'time', 'sys', 'dia',
                    'pulse', 'remarks']
        ifs = ['.'.join([dbn, _]) for _ in ifs]
        data = eval("session.query({}).all()".format(','.join(ifs)))
        res[dbn] = {
                'engine': engine, 'table': records,
                'session': session, 'data': data}
    return res


class AS(object):
    def __init__(self, name, prefer='records'):
        self.db = name
        engine = db.create_engine(f"sqlite:///{filepath(self.db)}")
        def tables():
            table = db.Table('sqlite_master', db.MetaData(), autoload=True, autoload_with=engine)
            mc = table.columns
            qry = db.select(mc.name).filter(mc.type==db.text("'table'")).filter(mc.name.notlike('sqlite_%')).filter(mc.name.notlike('android_%'))
            return [_[0] for _ in engine.connect().execute(qry).fetchall()]

        if prefer in tables():
            self.table = db.Table(prefer, db.MetaData(), autoload=True, autoload_with=engine)
            self.columns = self.table.columns
        self.connect = engine.connect()

    def __del__(self):
        self.table = self.columns = self.connect = None
        del(self.table, self.columns, self.connect)


class AE(AS):
    def __init__(self, eid):
        pE = pref.db['Equities']
        ae = AS(pE['name'], pE['table'])
        self.columns = ae.columns
        self.connect = ae.connect
        self.table = ae.table
        self.code = eid
        self.data = self.acquire(
                {'date': '>datetime(datetime.today().year - 4, 12, 31).date()'})
        self.close = self.data['Close'][-1]
        self.yahoo_code = f'{eid:04d}.HK'

    def __del__(self):
        self.columns = self.connect = self.table = self.code = None
        self.data = self.close = self.yahoo_code = None
        del(self.columns, self.connect, self.table, self.code, self.data,
            self.close, self.yahoo_code)

    def __call__(self, dataframe=True):
        return self.acquire(
                {'date': '>datetime(datetime.today().year - 4, 12, 31).date()'},
                dataframe)

    def append(self, values, conditions):
        hdr = {self.columns.eid: self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                hdr[_] = values[_]
        query = self.table.insert()
        trans = self.connect.begin()
        self.connect.execute(query, [hdr])
        trans.commit()

    def remove(self, conditions):
        def obtain_id(conditions=None):
            if not conditions:
                conditions = {'date': datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['eid'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    cs = f"self.columns.{_}=={conditions[_]}"
                    if _ == 'date':
                        cs = f"self.columns.{_}=='{conditions[_]}'"
                    hdr.append(cs)
                    # if _ == 'date': hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    # else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(
                eval('db.and_(' + ', '.join(hdr) + ')'))
            try:
                return self.connect.execute(query).scalar()
            except Exception:
                pass
        query = self.table.delete().where(
                self.columns.id == obtain_id(conditions))
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
            except Exception: pass
        hdr = {}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                hdr[_] = values[_]
        query = self.table.update().values(hdr).where(
                self.columns.id == obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def acquire(self, conditions):
        hdr = {'eid': f'=={self.code}'}
        for _ in conditions.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                hdr[_] = conditions[_]
        query = db.select([
            self.columns.date, self.columns.open, self.columns.high,
            self.columns.low, self.columns.close, self.columns.volume]).where(
                    eval('db.and_(' + ', '.join([
                        f"self.columns.{_}{hdr[_]}" for _ in hdr.keys()]) + ')'
                        ))
        res = pd.DataFrame(
                self.connect.execute(query).fetchall(),
                columns=[__.capitalize() for __ in [
                    f'{_}'.split('.')[-1] for _ in self.columns] if __ not in [
                        'eid', 'id']])
        res.set_index('Date', inplace=True)
        return res


class AF(AS):
    def __init__(self, code):
        pF = pref.db['Futures']
        ae = AS(pF['name'], pF['table'])
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
        hdr = {self.columns.code: self.code}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                hdr[_] = values[_]
        query = self.table.insert()
        trans = self.connect.begin()
        self.connect.execute(query, [hdr])
        trans.commit()

    def remove(self, conditions):
        def obtain_id(conditions=None):
            if not conditions:
                conditions = {'date': datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    cs = f"self.columns.{_}=={conditions[_]}"
                    if _ in ['code', 'date']:
                        cs = f"self.columns.{_}=='{conditions[_]}'"
                    hdr.append(cs)
                    # if _ in ['code', 'date']: hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    # else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(
                eval('db.and_(' + ', '.join(hdr) + ')'))
            try:
                return self.connect.execute(query).scalar()
            except Exception:
                pass
        query = self.table.delete().where(
                self.columns.id == obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def amend(self, values, conditions):
        def obtain_id(conditions=None):
            if not conditions:
                conditions = {'date': datetime.today().date()}
            if isinstance(conditions, dict):
                conditions['code'] = self.code
            hdr = []
            for _ in conditions.keys():
                if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                    cs = f"self.columns.{_}=={conditions[_]}"
                    if _ in ['code', 'session' 'date']:
                        cs = f"self.columns.{_}=='{conditions[_]}'"
                    hdr.append(cs)
                    # if _ in ['date', 'code', 'session']: hdr.append(f"self.columns.{_}=='{conditions[_]}'")
                    # else: hdr.append(f"self.columns.{_}=={conditions[_]}")
            query = db.select([self.columns.id]).where(
                eval('db.and_(' + ', '.join(hdr) + ')'))
            try:
                return self.connect.execute(query).scalar()
            except Exception:
                pass
        hdr = {}
        for _ in values.keys():
            if _ in [f'{__}'.split('.')[-1] for __ in self.columns]:
                hdr[_] = values[_]
        query = self.table.update().values(hdr).where(
                self.columns.id == obtain_id(conditions))
        trans = self.connect.begin()
        self.connect.execute(query)
        trans.commit()

    def combine(self, freq='bi-daily'):
        if freq.lower() == 'bi-daily':
            rd = {}
            sc = self.columns
            fl = [db.column('date'), sc.open, sc.high, sc.low, sc.close, sc.volume]
            qstr = db.select(fl).where(sc.code==self.code).order_by(db.desc(sc.session))
            _ = pd.read_sql(qstr, self.connect)
            for i in _.date.unique():
                td = {}
                ts = _[[i in s for s in _.date]]
                td['Open'] = ts.iloc[0].open
                td['High'] = ts.high.max()
                td['Low'] = ts.low.min()
                td['Close'] = ts.iloc[-1].close
                td['Volume'] = ts.volume.sum()
                rd[i] = td
            p_ = pd.DataFrame.from_dict(rd, orient='index')
            p_.index = p_.index.astype('datetime64[ns]')
            p_.index.name = 'Date'
            return p_
