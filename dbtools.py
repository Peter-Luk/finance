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
        res, erl, udl, wdata = [], [], [], web_collect(self.eid)
        for _ in self.eid:
            witem = wdata['{:04d}.HK'.format(_)]
            lwik = list(witem.keys())
            lwik.sort()
            sitemdate = [__[0] for __ in self.query.filter(self.__RD.eid == _).values(self.__RD.date)]
            for __ in lwik[1:]:
                vol = int(witem[__]['Volume'])
                if __.to_pydatetime().date() in sitemdate:
                    dhdr, iitem = {}, self.query.filter(self.__RD.eid == _, self.__RD.date == __.to_pydatetime().date())
                    if vol:
                        dhdr['id'] = iitem.value(self.__RD.id)
                        if iitem.value(self.__RD.open) != witem[__]['Open']: dhdr['open'] = witem[__]['Open']
                        if iitem.value(self.__RD.high) != witem[__]['High']: dhdr['high'] = witem[__]['High']
                        if iitem.value(self.__RD.low) != witem[__]['Low']: dhdr['low'] = witem[__]['Low']
                        if iitem.value(self.__RD.close) != witem[__]['Close']: dhdr['close'] = witem[__]['Close']
                        if iitem.value(self.__RD.volume) != vol: dhdr['volume'] = vol
                    udl.append(dhdr)
                else:
                    if vol:
                        nr = {'eid': _}
                        nr['date'] = __.to_pydatetime().date()
                        nr['volume'] = vol
                        for f in [_ for _ in self.data_fields if _ not in ['volume']]:
                            exec("nr['{}'] = witem[__]['{}']".format(f, f.capitalize()))
                        nrl.append(nr)
        if len(nrl) > 0:
            try:
                self.__session.bulk_insert_mappings(self.__db[self.db_name]['table'], nrl)
                self.__session.commit()
                res.append('{:d} record(s) added'.format(len(nrl)))
            except:
                self.__session.rollback()
                res.append('no records added')
        if len(udl) > 0:
            try:
                self.__session.bulk_update_mappings(self.__db[self.db_name]['table'], udl)
                self.__session.commit()
                res.append('{:d} record(s) updated'.format(len(udl)))
            except:
                self.__session.rollback()
                res.append('no record updated')
        return ','.join(res)
