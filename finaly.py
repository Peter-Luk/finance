def lstl(*args):
    res, tl = [], [args[0]]
    if len(args) > 1: tl.append(args[1])
    if len(tl[0]) == len(tl[1]):
        while tl[0]:
            t0 = tl[0].pop()
            t1 = tl[1].pop()
            res.append((t0, t1))
        res.reverse()
        return res

def ema(*args):
    steps = 12
    values = args[0]
    if len(args) > 1: steps = args[1]
    count = len(values)
    if count >= steps:
        while count > steps:
            lval = values.pop()
            return (ema(values, steps) * (steps - 1) + lval) / steps
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
