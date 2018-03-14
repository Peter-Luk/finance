from sfa import load, mapper, Danta
from utilities import web_collect
class RD(object): pass

class Equities(object):
    def __init__(self, *args):
        self.db_name = 'Securities'
        self.__RD = RD
        if args:
            if isinstance(args[0], str): self.db_name = args[0]
        self.__db = load(self.db_name)
        mapper(self.__RD, self.__db[self.db_name]['table'])
        self.__session = self.__db[self.db_name]['session']
        self.query = self.__session.query(self.__RD)
        avail_eid = [_[0] for _ in self.__session.query(self.__RD.eid).distinct()]
        avail_eid.sort()
        self.eid = [_ for _ in avail_eid if _ not in [805]]
        self.data_fields = ['open', 'high', 'low', 'close',  'volume']

    def __del__(self):
        self.__RD = self.db_name = self.__db = self.__session = self.query = self.eid = self.data_fields = None
        del(self.__RD, self.db_name, self.__db, self.__session, self.query, self.eid, self.data_fields)

    def __call__(self, *args):
        if args:
            if isinstance(args[0], (int, float)): eid = int(args[0])
        if eid in self.eid:
            return Danta(eid)

    def check(self):
        res, u_count, i_count = '', 0, 0
#    try:
        nrl, wdata = [], web_collect(self.eid)
        for _ in self.eid:
            witem = wdata['{:04d}.HK'.format(_)]
            lwik = list(witem.keys())
            lwik.sort()
            sitemdate = [__[0] for __ in self.query.filter(self.__RD.eid == _).values(self.__RD.date)]
            for __ in lwik[1:]:
                vol = witem[__]['Volume']
                if __.to_pydatetime().date() in sitemdate:
                    dhdr, iitem = {}, self.query.filter(self.__RD.eid == _, self.__RD.date == __)
                    if vol:
                        # dhdr['eid'] = _
                        # dhdr['date'] = __
                        if iitem.value(self.__RD.open) != witem[__]['Open']: dhdr['open'] = witem[__]['Open']
                        if iitem.value(self.__RD.high) != witem[__]['High']: dhdr['high'] = witem[__]['High']
                        if iitem.value(self.__RD.low) != witem[__]['Low']: dhdr['low'] = witem[__]['Low']
                        if iitem.value(self.__RD.close) != witem[__]['Close']: dhdr['close'] = witem[__]['Close']
                        if iitem.value(self.__RD.volume) != vol: dhdr['volume'] = vol
                    if dhdr:
                        iitem.update(dhdr)
                        self.__session.commit()
                        u_count += 1
                        self.__session.flush()
                else:
                    if vol:
                        nr = self.__RD()
                        nr.eid = _
                        nr.date = __
                        nr.volume = int(vol)
                        for f in [_ for _ in self.data_fields if _ not in ['volume']]:
                            exec("nr.{} = witem[__]['{}']".format(f, f.capitalize()))
                        nrl.append(nr)
#                        self.__session.add(nr)
#                        self.__session.commit()
                        i_count += 1
#                        self.__session.flush()
#        except: pass
        if i_count:
            try:
                self.__session.add_all(nrl)
                self.__session.commit()
            except: self.__session.rollback()
#             if u_count: res = '{:d} append, {:d} amend'.format(i_count, u_count)
#             res = '{:d} append'.format(i_count)
#         elif u_count: res = '{:d} amend'.format(u_count)
#         if res: return res
