him = getattr(__import__('handy'), 'him')
__ = him({'utilities':('mtf','waf'), 'bt':('LF',), 'os':('linesep',), 'sys':('version_info',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))
def an(*args, **kwargs):
    if args: pf = mtf(args[0])
    if 'futures_type' in list(kwargs.keys()): pf = mtf(kwargs['futures_type'])
    if 'code' in list(kwargs.keys()):
        if kwargs['code'] in waf(): pf = kwargs['code']
    mpd = getattr(lf(pf), 'fp')
    print('%s: (latest @ %s)' % (pf, mpd.trade_day[-1]))
    try:
        mos = getattr(mpd, 'ltdmos')('a')
        ar = getattr(mpd, 'fdc')('b')
        dm, ds = ar['Delta'].mean(), ar['Delta'].std()
        lv, vm, vs = ar['Volume'].values[-1], ar['Volume'].mean(), ar['Volume'].std()
        lc, cs = ar['Close'].values[-1], ar['Close'].std()
        print('Close: %i' % lc)
        print("%sVolume over mean: %.2f%%" % (linesep, lv / vm* 100.))
        print("Volume over (mean + std): %.2f%%" % (lv / (vm +vs) * 100.))
        il = list(filter(lambda _:(_ > lc - cs) and (_ < lc + cs), mos))
        ol = list(filter(lambda _:(_ < lc - cs) or (_ > lc + cs), mos))
        print('%sWithin statistical range:' % linesep, il)
        ml = list(filter(lambda _:_ > lc, ol))
        csl = list(filter(lambda _:_ not in ml, ol))
        if ml: print('%sMoon shot:' % linesep, ml)
        if csl: print('China syndrome:', csl)
        xd = getattr(mpd, 'xfinder')('d')
        dtxd = xd.transpose().to_dict()
        if len(dtxd.keys()):
            print('%sDelta extreme case:' % linesep)
            for _ in list(dtxd.keys()):
                print("%s: %i (%i - %i)" % (dtxd[_]['Date'].strftime('%d-%m-%Y'), dtxd[_]['Delta'], dm - ds, dm + ds))
    except:pass
    try:
        ai = getattr(mpd, 'fdc')('i')
        xr = getattr(mpd, 'xfinder')('r')
        rm, rs = ai['RSI'].mean(), ai['RSI'].std()
        dtxr = xr.transpose().to_dict()
        if len(dtxr.keys()):
            print('RSI extreme case:')
            for _ in list(dtxr.keys()):
                print("%s: %.3f (%.3f - %.3f)" % (dtxr[_]['Date'].strftime('%d-%m-%Y'), dtxr[_]['RSI'], rm - rs, rm + rs))
    except:pass

if __name__ == "__main__":
    confirm = 'Y'
    while confirm.upper() != 'N':
        if version_info.major == 2: ct = raw_input("Futures (C)ode/(T)ype: ")
        if version_info.major == 3: ct = input("Futures (C)ode/(T)ype: ")
        if ct.upper() == 'C':
            if version_info.major == 2: contract = raw_input('Contract: ')
            if version_info.major == 3: contract = input('Contract: ')
            if contract.upper() in waf(): an(code=contract.upper())
        elif ct.upper() == 'T':
            if version_info.major == 2: tp = raw_input('Type (H)SI/(M)HI: ')
            if version_info.major == 3: tp = input('Type (H)SI/(M)HI: ')
            if tp.upper() == 'H': an('hsi')
            elif tp.upper() == 'M': an('mhi')
        if version_info.major == 2: confirm = raw_input("Proceed (Y)es/(N)o: ")
        if version_info.major == 3: confirm = input("Proceed (Y)es/(N)o: ")
