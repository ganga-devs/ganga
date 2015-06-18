"""module to build GUIDs"""


import time
import random

import uuid as uuid_module

def uuid():
    return str(uuid_module.uuid4())


#---------------------------------------------------------------------------
# this function will become obsolete when ARDA job repository is phased out
def newGuid(value=None):
    """newGUID(value = None) --> guid
    value - any python object"""
    tt = time.gmtime()[0:6] + (random.randint(0, 9),
                               random.randint(0, 9),
                               random.randint(0, 9),
                               id(value))
    return '_' + (''.join(map(str, tt))).replace('-', '')
