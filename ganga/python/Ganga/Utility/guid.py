"""module to build GUIDs"""


import time
import random


#---------------------------------------------------------------------------
def newGuid(value = None):
    """newGUID(value = None) --> guid
    value - any python object"""
    tt = time.gmtime()[0:6] + (random.randint(0,9),
                               random.randint(0,9),
                               random.randint(0,9),
                               id(value))
    return '_' + (''.join(map(str, tt))).replace('-','')
