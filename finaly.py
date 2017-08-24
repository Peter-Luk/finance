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
    i, dl = 0, []
    while i < len(values) - 1:
        dl.append(values[i + 1] - values[i])
        i += 1

    def ag(*args):
        gs, i, t, values, steps = 0, 0, 0, args[0], 12
        while steps < len(values):
            if values[-1] > 0: t = values[-1]
            return (ag(values[:-1], steps) * (steps - 1) + t) / steps
        while i < steps:
            if values[i] > 0: gs += values[i]
            i += 1
        return gs / steps

    def al(*args):
        ls, i, t, values, steps = 0, 0, 0, args[0], 12
        while steps < len(values):
            if values[-1] < 0: t = abs(values[-1])
            return (al(values[:-1], steps) * (steps - 1) + t) / steps
        while i < steps:
            if values[i] < 0: ls += abs(values[i])
            i += 1
        return ls / steps
    rs = ag(dl, 12) / al(dl, 12)
    return 100 - 100 / (1 + rs)
