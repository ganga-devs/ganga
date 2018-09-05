def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    return {}
    

def loadPlugins(c):
    pass

from GangaTutorial.Lib.primes.primes import check_prime_job, split_prime_job

from GangaCore.Runtime.GPIexport import exportToGPI

from GangaCore.Utility.logging import getLogger
logger = getLogger(modulename=True)

exportToGPI("check_prime_job",check_prime_job,"Functions")
exportToGPI("split_prime_job",split_prime_job,"Functions")

import os, GangaTutorial
TUTDIR = os.path.dirname(GangaTutorial.__file__)
exportToGPI("TUTDIR",TUTDIR,"Objects")
del os,GangaTutorial

logger.debug("*** Ganga Tutorial Loaded OK ***")
