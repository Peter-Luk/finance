import numpy as np
from sys import platform

if platform == 'win32':
    use_numba = False
else:
    try:
        import numba as nb
        use_numba = True
    except Exception:
        use_numba = False

def _stepper(period, x):
    y = np.empty(x.size)
    y[:] = np.nan
    i = 0
    while i < x.size:
        if i > period:
            y[i] = (y[i - 1] * (period - 1) + x[i]) / period
        elif i == period:
            y[i] = x[:i].mean()
        i += 1
    return y

if use_numba:
    # print('Using numba')
    stepper = nb.jit()(_stepper)
else:
    # print('Falling back to python')
    stepper = _stepper
