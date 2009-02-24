"""
Internal initialization of the registries.
"""

import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('Configuration')

from Ganga.Utility.logging import getLogger,log_user_exception
logger = getLogger()

import os.path
from Ganga.Utility.files import expandfilename

def requiresAfsToken():
    if hasattr(repository_runtime,'requiresAfsToken'):
        return repository_runtime.requiresAfsToken()
    from Ganga.Utility.files import fullpath
    return fullpath(repository_runtime.getLocalRoot()).find('/afs') == 0

def requiresGridProxy():
    return repository_runtime.requiresGridProxy()

def getLocalRoot():
    if "Local" in config['repositorytype']:
        return os.path.join(expandfilename(config['gangadir']),'repository',config['user'],config['repositorytype'])
    else:
        return ''

def __selectRepository():
    
    import XML_repository_runtime
    import AMGA_repository_runtime
    
    if config['repositorytype'] == 'LocalXML':
        repository_runtime = XML_repository_runtime
    else:
        if config['repositorytype'] in ['LocalAMGA','RemoteAMGA']:
            repository_runtime = AMGA_repository_runtime
        else:
            raise Ganga.Utility.Config.ConfigError('"%s" is unsupported repository type'%config['repositorytype'])

    return repository_runtime

repository_runtime = __selectRepository()
repository_runtime.getLocalRoot = getLocalRoot
bootstrap = repository_runtime.bootstrap

def _shutdown(names,regs):
    logger.debug("repository shutdown")

    # check if the repository has been already disabled (by Ganga.Core.Coordinator)
    from Ganga.Core.InternalServices import Coordinator
    if not Coordinator.servicesEnabled:
        logger.debug("repository has already been shut down")        
        return 
    
    for n,r in zip(names,regs):
        logger.debug("flushing %s cache to the persistent storage...",n)
        r._flush()
        logger.debug("releasing possible locks")
        # use ARDAMD interface directly to release all locks which may have been left over
        # from the other threads (which are daemonic so they may be interrupted inside the critical section)
        r.repository.releaseAllLocks()
        

