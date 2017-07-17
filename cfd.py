e = getattr(__import__('handy'),'encoder')
rim = {'cherrypy':(),'os':('linesep',),'sys':('argv','platform'),'utilities':('ltd',),'derivatives':('today','waf','estimate')}
__ = e(rim)
for _ in list(__.keys()): exec("%s=__['%s']"%(_,_))
rim = {'derivatives':('Analyser','Futures')}
__ = e(rim, case='capitalize')
for _ in list(__.keys()): exec("%s=__['%s']"%(_,_))
rim = {'utilities':('IP',),'tags':('HTML', 'HEAD', 'TITLE', 'BODY', 'FORM', 'TABLE', 'TR', 'TD', 'LABEL', 'SELECT', 'OPTION', 'BUTTON', 'INPUT', 'B')}
__ = e(rim, case='upper')
for _ in list(__.keys()): exec("%s=__['%s']"%(_,_))
# from os import linesep
# from sys import argv, platform
# from derivatives import Analyser, Futures, today, waf, estimate
# from tags import HTML, HEAD, TITLE, BODY, FORM, TABLE, TR, TD, LABEL, SELECT, OPTION, BUTTON, INPUT, B
# from utilities import ltd, IP

# import cherrypy

server_host, server_port = IP('public').address, 2028
if len(argv) > 1:server_host = argv[1]
if platform in ('linux', 'linux2'):server_port = 2080
cherrypy.config.update({'server.socket_host': server_host,'server.socket_port': server_port})

class Inputter(object):
    @cherrypy.expose
    def index(self):
        hd = HEAD(TITLE('Daily statistic recording'))
        ops = [OPTION(v,{'value':v}) for v in waf()]
#        if today.day == ltd['%s'%today.year][today.month-1]:ops = [OPTION(v,{'value':v}) for v in waf(1)]
        if today.day == ltd(today.year, today.month):ops = [OPTION(v,{'value':v}) for v in waf(1)]
        sl = SELECT(linesep.join([str(v) for v in ops]),{'name':'contract'})
        btn = BUTTON('Amend',{'type':'submit'})
        trs = [TR(linesep.join([str(x) for x in [TD(LABEL('Contract: ')),TD(linesep.join([str(v) for v in [sl, btn]]),{'align':'right'})]]))]
        trs.extend([TR(linesep.join([str(v) for v in [TD('%s: '%u.capitalize(),{'align':'right'}),TD(INPUT({'type':'text','name':u}))]])) for u in ['open','high','low','close','volume']])
        bd = BODY(FORM(TABLE(linesep.join([str(v) for v in trs])),{'method':'post','action':'append'}))
        return str(HTML(linesep.join([str(v) for v in [hd,bd]])))

    @cherrypy.expose
    def append(self, contract, open, high, low, close, volume):
        fu = Futures()
        session = fu._Futures__session
        if session == 'A':sstr = 'afternoon'
        elif session == 'M':sstr = 'morning'
        hd = HEAD(TITLE('%s: %s %s session data'%(today.date(),contract.upper(),sstr)))
        id = fu._Futures__cur.execute("SELECT id FROM %s WHERE date='%s' AND code='%s' AND session='%s'"%(fu._Futures__table,today.date(),contract,session)).fetchone()
        if id:fu.amend(criteria={'id':id},value={'open':int(open),'high':int(high),'low':int(low),'close':int(close),'volume':int(volume)})
        else:fu.append((contract.upper(), int(open), int(high), int(low), int(close), int(volume)))
        res = fu.show(criteria={'date':today.date()})
        if len(res):
            bd = BODY(TABLE(TR(TD("%s record(s) add on '%s'"%(len(res), today.date())))))
        return str(HTML(linesep.join([str(v) for v in [hd,bd]])))

class Analysor(object):
    @cherrypy.expose
    def index(self):
        methods, trs = ('wma', 'kama', 'ema', 'hv'), []

        for f in [x.upper() for x in waf()]:
            an = Analyser(f)
            res = an.daily(type=methods)
            keys = list(res.keys())
            keys.sort()
            tds = [TD('date'.capitalize())]
            tds.extend([TD(v.upper(),{'align':'center'}) for v in methods])
            trs.extend([TR(TD(B(f),{'colspan':'5','align':'center'})),TR(linesep.join([str(v) for v in tds]))])
            trs.extend([TR(linesep.join([str(v) for v in [TD('%s:'% k),TD('%0.3f'%res[k]['wma10'],{'align':'center'}),TD('%0.3f'%res[k]['kama10'],{'align':'center'}),TD('%0.3f'%res[k]['ema10'],{'align':'center'}),TD('%0.5f'%res[k]['hv10'],{'align':'center'})]])) for k in keys])
        hd = HEAD(TITLE('Analysis Report'))
        bd = BODY(TABLE(linesep.join([str(v) for v in trs]),{'width':'600'}))
        return str(HTML(linesep.join([str(v) for v in [hd,bd]])))

class Estimator(object):
    @cherrypy.expose
    def index(self):
        hd = HEAD(TITLE('Estimate session range'))
        ops = [OPTION(v,{'value':v}) for v in waf()]
#        if today.day == ltd['%s'%today.year][today.month - 1]:ops = [OPTION(v,{'value':v}) for v in waf(1)]
        if today.day == ltd(today.year, today.month):ops = [OPTION(v,{'value':v}) for v in waf(1)]
        sl = SELECT(linesep.join([str(v) for v in ops]),{'name':'contract'})
        btn = BUTTON('Estimate',{'type':'submit'})
        trs = [TR(linesep.join([str(v) for v in [TD(LABEL('Contract: ')),TD(linesep.join([str(u) for u in [sl,btn]]),{'align':'right'})]]))]
        trs.append(TR(linesep.join([str(v) for v in [TD('Pivot Point',{'align':'right'}),TD(INPUT({'type':'text','name':'pp'}))]])))
        bd = BODY(FORM(TABLE(linesep.join([str(v) for v in trs])),{'method':'post','action':'process_request'}))
        return str(HTML(linesep.join([str(v) for v in [hd,bd]])))

    @cherrypy.expose
    def process_request(self, contract, pp):
        an = Analyser(contract)
        res = an._Analyser__cur.execute("SELECT %s FROM %s WHERE code='%s'"%('id','records',contract)).fetchall()
        if res:
            trs, eres = '', estimate(pp, contract)
            if type(eres) is str:
                for v in eres.split(linesep):
                    trs += str(TR(TD(v)))
            elif type(eres) is dict:
                for v in list(eres.keys()):
                    trs += str(TR(TD('%s (est.): %i to %i / %i to %i' % ((v,) + eres[v]['upper'] + eres[v]['lower']))))
            return str(HTML('%s%s%s' % (HEAD(TITLE('Estimate for %s'%contract)), linesep, BODY(TABLE(trs)))))

if __name__ == '__main__':
    cherrypy.tree.mount(Inputter())
    cherrypy.tree.mount(Estimator(),'/estimate')
    cherrypy.tree.mount(Analysor(),'/analyse')
    cherrypy.engine.start()
    cherrypy.engine.block()
