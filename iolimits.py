him = getattr(__import__('handy'), 'him')
__ = him({'utilities':('mtf',), 'bt':('LF',), 'os':('linesep',)})
for _ in list(__.keys()): exec("%s=__['%s']" % (_,_))
def an(*args, **kwargs):
    if args: fit = args[0]
    if 'futures_type' in list(kwargs.keys()): fit = kwargs['futures_type']
    pf = mtf(fit)
    mpd = getattr(lf(pf), 'fp')
    print('%s: (latest @ %s)' % (pf, mpd.trade_day[-1]))
    try:
        ai = getattr(mpd, 'fdc')('i')
        xr = getattr(mpd, 'xfinder')('r')
        rm, rs = ai['RSI'].mean(), ai['RSI'].std()
        dtxr = xr.transpose().to_dict()
        for _ in list(dtxr.keys()):
            print("%s: RSI @ %.3f (%.3f - %.3f)" % (dtxr[_]['Date'].strftime('%d-%m-%Y'), dtxr[_]['RSI'], rm - rs, rm + rs))
    except:pass
    try:
        mos = getattr(mpd, 'ltdmos')('a')
        ar = getattr(mpd, 'fdc')('b')
        xd = getattr(mpd, 'xfinder')('d')
        dm, ds = ar['Delta'].mean(), ar['Delta'].std()
        dtxd = xd.transpose().to_dict()
        for _ in list(dtxd.keys()):
            print("%s: delta @ %i (%i - %i)" % (dtxd[_]['Date'].strftime('%d-%m-%Y'), dtxd[_]['Delta'], dm - ds, dm + ds))
        lv, vm, vs = ar['Volume'].values[-1], ar['Volume'].mean(), ar['Volume'].std()
        print("%sVolume over mean: %.2f%%" % (linesep, lv / vm* 100.))
        print("Volume over (mean + std): %.2f%%" % (lv / (vm +vs) * 100.))
        lc, cs = ar['Close'].values[-1], ar['Close'].std()
        il = list(filter(lambda _:(_ > lc - cs) and (_ < lc + cs), mos))
        ol = list(filter(lambda _:(_ < lc - cs) or (_ > lc + cs), mos))
        print('%sLast Close: %i' % (linesep, lc))
        print('%sWithin statistical range:' % linesep, il)
        ml = list(filter(lambda _:_ > lc, ol))
        csl = list(filter(lambda _:_ not in ml, ol))
        if ml: print('%sMoon shot:' % linesep, ml)
        if csl: print('China syndrome:', csl)
    except:pass
an('mhi')
