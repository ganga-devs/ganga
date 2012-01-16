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
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Lib.Job import Job


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
    basepath = os.path.join(expandfilename(config['gangadir']),'repository',config['user'])
    names = ['jobs','templates']

    path = os.path.join(basepath,"LocalAMGA")
    if os.path.exists(path) and not os.path.exists(os.path.join(path,"converted.to.XML.6.0")):
        from Ganga.Core.JobRepository.ARDA import repositoryFactory
        for name in names:
            try:
                rep = repositoryFactory(subpath = name)
                co_jobs = rep.checkoutJobs({})
                salvaged_jobs[name].extend(co_jobs)
                file(os.path.join(path,"converted.to.XML.6.0"),"w").close()
                rep.releaseAllLocks()
                if len(co_jobs) > 0:
                    logger.warning("Converted %i jobs from old AMGA repository" % len(co_jobs))
            except Exception,x:
                logger.error("Could not load old AMGA repository: %s" % x)
                raise

    from Ganga.Core.JobRepositoryXML import factory, version
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
                logger.error("Could not load old XML repository: %s" % x)
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
        logger.debug("started " + registry.info(full=False))
        if registry.name == "jobs":
            registry.print_other_sessions()
        started_registries.append(registry.name)
        retval.append((registry.name, registry.getProxy(), registry.doc))
        if registry.name in oldJobs:
            for j in oldJobs[registry.name]:
                j._index_cache = None
                if not j.id in registry:
                    registry._add(j, force_index = j.id)
                else:
                    logger.warning("Import Collision at id %i, appending job to the end...", j.id)
                    registry._add(j)
    import atexit
    atexit.register(shutdown)
    return retval

def shutdown():
    logger.debug('registry shutdown')
    for registry in getRegistries():
        if not registry.name in started_registries: continue

#        if registry.name == 'prep':
#            print "closing down prep reg"
#            for dir in getRegistry('prep').__iter__():
#                # if there's no directory present on the filesyste
#                print "DIR:", dir

#        if registry.name == 'jobs':
#            resp = 'n'
#            for job in getRegistry('jobs').__iter__():
#                if (resp != 'N'):
#                    if job.application.is_prepared is not None:
#                        shareddir = job.application.is_prepared.name
#                        if not os.path.isdir(shareddir):
#                            if (resp == 'A'):
#                                logger.warning('Unpreparing %s') % shareddir
#                                job.unprepare()
#                                #shutil.rmtree(shareddir)
#                            else:
#                                logger.warning("Can't find shared directory %s associated with job %s" % (shareddir, job.id))
#                                msg = 'Do you want to unprepare job %s? (y/n/All/[N]one)' %(job.id)
#                                resp = raw_input(msg)
#                                if (resp == 'y'):
#                                    #logger.warning("Deleting %s") %(shareddir)
#                                    logger.warning("Unpreparing job %s" % (job.id))
#                                    job.unprepare()
#                                    #shutil.rmtree(shareddir)
#                                elif (resp == 'n'):
#                                    #print 'not deleting %s' %(shareddir)
#                                    print '%s not found, but not unpreparing job %s' %(shareddir, job.id)
#                                elif (resp == 'N'):
#                                    pass
#                                elif (resp == 'A'):
#                                    #print 'deleting %s' %(shareddir)
#                                    logger.warning("Unpreparing job %s" % (job.id))
#                                    job.unprepare()
#                                    #shutil.rmtree(shareddir)
#                                else:
#                                    resp = 'N'
#                else:
#                    pass
 


        if registry.name == 'jobs':
            for job in getRegistry('jobs').__iter__():
                if hasattr(job.application,'is_prepared'):
                    if job.application.is_prepared is not None and job.application.is_prepared is not True:
                        shareddir = job.application.is_prepared.name
                        if not os.path.isdir(shareddir):
                            logger.warning("Can't find shared directory %s associated with job %s. Unpreparing job." % (shareddir, job.id))
                            job.unprepare()
                            
        if registry.name == 'box':
            for item in getRegistry('box').__iter__():
                if isType(item,Job):
                     job = item
                     if hasattr(job.application,'is_prepared'):
                        if job.application.is_prepared is not None and job.application.is_prepared is not True:
                            shareddir = job.application.is_prepared.name
                            if not os.path.isdir(shareddir):
                                logger.warning("Can't find shared directory %s associated with job %s in box. Unpreparing job." % (shareddir, job.id))
                                job.unprepare()
                else:
                    if hasattr(item,'is_prepared'):
                        app = item
                        if app.is_prepared is not None and app.is_prepared is not True:
                            shareddir = app.is_prepared.name
                            if not os.path.isdir(shareddir):
                                logger.warning("Can't find shared directory %s associated with %s application stored in box. Unpreparing application." % (shareddir, app._name))
                                app.unprepare()
                          
                       
        started_registries.remove(registry.name) # in case this is called repeatedly, only call shutdown once
        registry.shutdown() # flush and release locks


