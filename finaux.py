import numpy as np
from sys import platform

if platform == 'win32':
    use_numba = False
# else:
try:
    import numba as nb
    use_numba = True
except Exception:
    use_numba = False

def _stepper(period, x):
    y = np.empty(len(x))
    y[:] = np.nan
    i = 0
    while i < len(x):
        if i > period:
            y[i] = (y[i - 1] * (period - 1) + x[i]) / period
        elif i == period:
            y[i] = x[:i].mean()
        i += 1
    return y

def _roundup(value):
    if np.isnan(value) or not value > 0:
        return np.nan
    _ = int(np.floor(np.log10(value)))
    __ = np.divmod(value, 10 ** (_ - 1))[0]
    if _ < 0:
        if __ < 25:
            return np.round(value, 3)
        if __ < 50:
            return np.round(value * 2, 2) / 2
        return np.round(value, 2)
    if _ == 0:
        return np.round(value, 2)
    if _ > 3:
        return np.round(value, 0)
    if _ > 1:
        if __ < 20:
            return np.round(value, 1)
        if __ < 50:
            return np.round(value * 5, 0) / 5
        return np.round(value * 2, 0) / 2
    if _ > 0:
        if __ < 10:
            return np.round(value, 2)
        if __ < 20:
            return np.round(value * 5, 1) / 5
        return np.round(value * 2, 1) / 2

if use_numba:
    print('Using numba')
    # stepper = nb.jit(forceobj=True)(_stepper)
    stepper = nb.jit()(_stepper)
    roundup = nb.jit(forceobj=True)(_roundup)
else:
    print('Falling back to python')
    stepper = _stepper
    roundup = _roundup
