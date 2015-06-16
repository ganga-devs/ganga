"""module to build GUIDs"""


import time
import random

# provide uuid missing in python < 2.5

try:  # 2.5
    import uuid as uuid_module

    def uuid():
        return str(uuid_module.uuid4())
except ImportError:  # <2.5
    # FIXME: poor's man uuid
    def uuid():
        """Poor's man uuid. This is a stub provided by Ganga to
        complement missing functionality in python<2.5"""
        return (str(random.uniform(0, 100000000)) + '-' + str(time.time())).replace('.', '-')


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
