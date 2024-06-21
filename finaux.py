import numpy as np
from sys import platform
from utilities import conditional_decorator
# from finance.utilities import conditional_decorator

if platform == 'win32':
    use_numba = False
# else:
try:
    from numba import jit
    # import numba as nb
    use_numba = True
except Exception:
    use_numba = False

# @conditional_decorator(jit(nopython=True), use_numba)
def stepper(period, x):
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
"""def _stepper(period, x):
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
"""

# @conditional_decorator(jit(nopython=True), use_numba)
def roundup(value):
    if np.isnan(value) or not value > 0:
        hdr = np.nan
    _ = np.floor(np.log10(value))
    __ = np.int32(value / 10 ** (_ - 1))
    if _ < 0:
        if __ < 25:
            hdr = np.round(value, 3)
        if __ < 50:
            hdr = np.round(value * 2, 2) / 2
        hdr = np.round(value, 2)
    if _ == 0:
        hdr = np.round(value, 2)
    if _ > 3:
        hdr = np.round(value, 0)
    if _ > 1:
        if __ < 20:
            hdr = np.round(value, 1)
        if __ < 50:
            hdr = np.round(value * 5, 0) / 5
        hdr = np.round(value * 2, 0) / 2
    if _ > 0:
        if __ < 10:
            hdr = np.round(value, 2)
        if __ < 20:
            hdr = np.round(value * 5, 1) / 5
        hdr = np.round(value * 2, 1) / 2
    return hdr
"""
def _roundup(value):
    if np.isnan(value) or not value > 0:
        hdr = np.nan
    _ = np.floor(np.log10(value))
    __ = np.int32(value / 10 ** (_ - 1))
    if _ < 0:
        if __ < 25:
            hdr = np.round(value, 3)
        if __ < 50:
            hdr = np.round(value * 2, 2) / 2
        hdr = np.round(value, 2)
    if _ == 0:
        hdr = np.round(value, 2)
    if _ > 3:
        hdr = np.round(value, 0)
    if _ > 1:
        if __ < 20:
            hdr = np.round(value, 1)
        if __ < 50:
            hdr = np.round(value * 5, 0) / 5
        hdr = np.round(value * 2, 0) / 2
    if _ > 0:
        if __ < 10:
            hdr = np.round(value, 2)
        if __ < 20:
            hdr = np.round(value * 5, 1) / 5
        hdr = np.round(value * 2, 1) / 2
    return hdr


if use_numba:
    print('Using numba')
    # stepper = nb.jit(forceobj=True)(_stepper)
    # roundup = nb.jit(forceobj=True)(_roundup)
    stepper = nb.jit()(_stepper)
    roundup = nb.jit()(_roundup)
else:
    print('Falling back to python')
    stepper = _stepper
    roundup = _roundup
"""
