from alchemy import load, mapper
from sfa import avail_eid
from utilities import web_collect
class RD(object): pass
dbs = load('Securities')
mapper(RD, dbs['Securities']['table'])
s_session = dbs['Securities']['session']
s_query = s_session.query(RD)
el = [_ for _ in avail_eid if _ not in [805]]
ifields = ['open', 'high', 'low', 'close',  'volume']
try:
    wdata = web_collect(el)
    for _ in el:
        witem = wdata['{:04d}.HK'.format(_)]
        lwik = [_.to_datetime().date() for _ in witem.keys()]
        lwik.sort()
        sitemdate = [_[0] for _ in s_query.filter(RD.eid == _).values(RD.date)]
        for __ in lwik:
            if __ not in sitemdate:
                if s_query.filter(RD.eid == _, RD.date == __).value(RD.volume) != 0:
                    nr = RD()
                    nr.eid = _
                    nr.date = __
                    for f in ifields:
                        exec('nr.{0} = witem[__][{0}.capitalize()]'.format(f))
                        if f == 'volume': exec('nr.{0} = int(witem[__][{0}.capitalize()])'.format(f))
                    s_session.add(nr)
                    s_session.commit()
                    s_session.flush()
except: pass
