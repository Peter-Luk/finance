def lstl(*args):
    res, fl = [], args[0]
    if len(args) > 1: sl = args[1]
    if len(fl) == len(sl):
        i = 0
        while i < len(fl):
            tfl = fl[i]
            tsl = sl[i]
            res.append((tfl, tsl))
            i += 1
        # res.reverse()
        return res

def ema(*args):
    steps = 12
    values = args[0]
    if len(args) > 1: steps = args[1]
    count = len(values)
    if count >= steps:
        while count > steps:
            lval = values[-1]
            return (ema(values[:-1], steps) * (steps - 1) + lval) / steps
        return sum(values) / steps

def sma(*args):
    steps = 12
    values = args[0]
    if len(args) > 1: steps = args[1]
    if len(values) >= steps: return sum(values[-steps:]) / steps

def wma(*args):
    steps = 12
    values = args[0]
    if len(args) > 1: steps = args[1]
    if len(values) >= steps:
        res, ys = [], []
        for x, y in values:
            res.append(x * y)
            ys.append(y)
        return sum(res[-steps:]) / sum(ys[-steps:])
