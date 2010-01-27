"""
Internal initialization of the repositories.
"""

import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('Configuration')

from Ganga.Utility.logging import getLogger,log_user_exception
logger = getLogger()

import os.path
from Ganga.Utility.files import expandfilename
from Ganga.Core.GangaRepository import getRegistries, RepositoryError

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

def getOldJobs():
    salvaged_jobs = {'jobs':[],'templates':[]}
    from Ganga.Core.JobRepository.ARDA import repositoryFactory
    for name in names:
        path = os.path.join(basepath,"LocalAMGA")
        if os.path.exists(path) and not os.path.exists(os.path.join(path,"converted.to.XML.6.0")):
            try:
                rep = repositoryFactory(subpath = name)
                co_jobs = rep.checkoutJobs({})
                salvaged_jobs[name].extend(co_jobs)
                file(os.path.join(path,"converted.to.XML.6.0"),"w").close()
                rep.releaseAllLocks()
                if len(co_jobs) > 0:
                    logger.warning("Converted %i jobs from old AMGA repository" % len(co_jobs))
            except Exception,x:
                logger.error("Could not load old AMGA repository:" % x)
                raise

    from Ganga.Core.JobRepositoryXML import factory, version
    names = ['jobs','templates']
    basepath = os.path.join(expandfilename(config['gangadir']),'repository',config['user'])
    for name in names:
        path = os.path.join(basepath,"LocalXML",version,name)
        if os.path.exists(path) and not os.path.exists(os.path.join(path,"converted.to.XML.6.0")):
            try:
                rep = factory(dir = path)
                co_jobs = rep.checkoutJobs({})
                salvaged_jobs[name].extend(co_jobs)
                file(os.path.join(path,"converted.to.XML.6.0"),"w").close()
                rep.releaseAllLocks()
                if len(co_jobs) > 0:
                    logger.warning("Converted %i jobs from old XML repository" % len(co_jobs))
            except Exception,x:
                logger.error("Could not load old XML repository:" % x)
                raise

    return salvaged_jobs

started_registries = []
def bootstrap():
    oldJobs = getOldJobs()
    retval = []
    for registry in getRegistries():
        if registry.name in started_registries: continue
        registry.type = config["repositorytype"]
        registry.location = getLocalRoot()
        registry.startup()
        started_registries.append(registry.name)
        retval.append((registry.name, registry.getProxy(), registry.doc))
        if registry.name in oldJobs:
            for j in oldJobs[registry.name]:
                j._index_cache = None
                registry._add(j)
    import atexit
    atexit.register(shutdown)
    return retval

def shutdown():
    logger.debug("registry shutdown")
    for registry in getRegistries():
        if not registry.name in started_registries: continue
        started_registries.remove(registry.name) # in case this is called repeatedly, only call shutdown once
        registry.shutdown() # flush and release locks


