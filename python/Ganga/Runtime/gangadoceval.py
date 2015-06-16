
# evaluate help expressions in the Ganga.GPI namespace
# because from ... import * may only be done at the module level
# hence we need this additional module to do the trick

from Ganga.GPI import *


def evaluate(v):
    try:
        return eval(v)
    except Exception as x:
        return v
