from statistics import stdev

def lstl(*args):
    res, tl = [], [args[0]]
    if len(args) > 1: tl.append(args[1])
    if len(tl[0]) == len(tl[1]):
        i = 0
        while i < len(tl[0]):
            # tl0 = tl[0][i]
            # tl1 = tl[1][i]
            res.append((tl[0][i], tl[1][i]))
            i += 1
        # res.reverse()
        return res

def delta(*args):
    i, res, values = 0, [], args[0]
    while i < len(values) - 1:
        res.append(values[i + 1] - values[i])
        i += 1
    return res

def absum(*args):
    i, res, values = 0, 0, args[0]
    while i < len(values):
        if values[i] > 0: res += values[i]
        if values[i] < 0: res += -values[i]
        i += 1
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

def rsi(*args):
    values, steps  = args[0], 12
    if len(args) > 1: steps = args[1]
    dl = delta(values)
    # i = 0
    # while i < len(values) - 1:
        # dl.append(values[i + 1] - values[i])
        # i += 1

    def ag(*args):
        gs = i = t = 0
        values, steps = args[0], 12
        if len(args) > 1: steps = args[1]
        while steps < len(values):
            if values[-1] > 0: t = values[-1]
            return (ag(values[:-1], steps) * (steps - 1) + t) / steps
        while i < steps:
            if values[i] > 0: gs += values[i]
            i += 1
        return gs / steps

    def al(*args):
        ls = i = t = 0
        values, steps = args[0], 12
        if len(args) > 1: steps = args[1]
        while steps < len(values):
            if values[-1] < 0: t = abs(values[-1])
            return (al(values[:-1], steps) * (steps - 1) + t) / steps
        while i < steps:
            if values[i] < 0: ls += abs(values[i])
            i += 1
        return ls / steps

    rs = ag(dl, steps) / al(dl, steps)
    return 100 - 100 / (1 + rs)

def kama(*args):
    steps, values = 12, args[0]
    if len(args) > 1: steps = args[1]
    count = len(values)
    if count >= steps:
        fc = 2 / (steps + 1)
        sc = 2 / (2 + 1)
        while count > steps:
            er = (values[-1] - values[-steps]) / absum(delta(values[-steps:-1]))
            alpha = (er + (fc - sc) + sc) ** 2
            pk = kama(values[:-1], steps)
            return alpha * (values[-1] - pk) + pk
        return sum(values) / steps
