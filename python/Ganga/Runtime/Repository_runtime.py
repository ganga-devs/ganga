"""
Internal initialization of the repositories.
"""

import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('Configuration')

from Ganga.Utility.logging import getLogger,log_user_exception
logger = getLogger()

import os.path
from Ganga.Utility.files import expandfilename
from Ganga.Core.exceptions import RepositoryError
from Ganga.Core.GangaRepository import getRegistries

def requiresAfsToken():
    from Ganga.Utility.files import fullpath
    return fullpath(getLocalRoot()).find('/afs') == 0

def requiresGridProxy():
    return False

def getLocalRoot():
    if config['repositorytype'] in ['LocalXML','LocalAMGA','LocalPickle','SQLite']:
        return os.path.join(expandfilename(config['gangadir']),'repository',config['user'],config['repositorytype'])
    else:
        return ''

started_registries = []
def bootstrap():
    retval = []
    for registry in getRegistries():
        if registry.name in started_registries: continue
        registry.type = config["repositorytype"]
        registry.location = getLocalRoot()
        registry.startup()
        started_registries.append(registry.name)
        retval.append((registry.name, registry.getProxy(), registry.doc))
    import atexit
    atexit.register(shutdown)
    return retval

def shutdown():
    logger.debug("registry shutdown")
    for registry in getRegistries():
        if not registry.name in started_registries: continue
        registry.shutdown() # flush and release locks
        started_registries.remove(registry.name)

