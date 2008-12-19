def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}
    

def loadPlugins(c):
    pass

from GangaTutorial.Lib.primes.primes import check_prime_job, split_prime_job

import os, GangaTutorial
TUTDIR = os.path.dirname(GangaTutorial.__file__)
del os,GangaTutorial
