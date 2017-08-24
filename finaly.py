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
