import numpy as np
from sys import platform

if platform == 'linux':
    from numba import jit
    @jit(nopython=True)
    def stepper(period, x):
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
else:
    def stepper(period, x):
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
