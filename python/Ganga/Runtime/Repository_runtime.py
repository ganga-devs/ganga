"""
Internal initialization of the repositories.
"""

import Ganga.Utility.Config
from Ganga.Utility.logging import getLogger
import os.path
from Ganga.Utility.files import expandfilename
from Ganga.Core.GangaRepository import getRegistries
from Ganga.Core.GangaRepository import getRegistry

config = Ganga.Utility.Config.getConfig('Configuration')
logger = getLogger()


def requiresAfsToken():
    from Ganga.Utility.files import fullpath
    return fullpath(getLocalRoot()).find('/afs') == 0


def requiresGridProxy():
    return False


def getLocalRoot():
    if config['repositorytype'] in ['LocalXML', 'LocalAMGA', 'LocalPickle', 'SQLite']:
        return os.path.join(expandfilename(config['gangadir']), 'repository', config['user'], config['repositorytype'])
    else:
        return ''

def getLocalWorkspace():
    if config['repositorytype'] in ['LocalXML', 'LocalAMGA', 'LocalPickle', 'SQLite']:
        return os.path.join(expandfilename(config['gangadir']), 'workspace', config['user'], config['repositorytype'])
    else:
        return ''


def getOldJobs():
    salvaged_jobs = {'jobs': [], 'templates': []}
    basepath = os.path.join(
            expandfilename(config['gangadir']), 'repository', config['user'])
    names = ['jobs', 'templates']

    path = os.path.join(basepath, "LocalAMGA")
    if os.path.exists(path) and not os.path.exists(os.path.join(path, "converted.to.XML.6.0")):
        from Ganga.Core.JobRepository.ARDA import repositoryFactory
        for name in names:
            try:
                rep = repositoryFactory(subpath=name)
                co_jobs = rep.checkoutJobs({})
                salvaged_jobs[name].extend(co_jobs)
                file(os.path.join(path, "converted.to.XML.6.0"), "w").close()
                rep.releaseAllLocks()
                if len(co_jobs) > 0:
                    logger.warning(
                            "Converted %i jobs from old AMGA repository" % len(co_jobs))
            except Exception as x:
                logger.error("Could not load old AMGA repository: %s" % x)
                raise

    from Ganga.Core.JobRepositoryXML import factory, version
    for name in names:
        path = os.path.join(basepath, "LocalXML", version, name)
        if os.path.exists(path) and not os.path.exists(os.path.join(path, "converted.to.XML.6.0")):
            try:
                rep = factory(dir=path)
                co_jobs = rep.checkoutJobs({})
                salvaged_jobs[name].extend(co_jobs)
                file(os.path.join(path, "converted.to.XML.6.0"), "w").close()
                rep.releaseAllLocks()
                if len(co_jobs) > 0:
                    logger.warning(
                            "Converted %i jobs from old XML repository" % len(co_jobs))
            except Exception as x:
                logger.error("Could not load old XML repository: %s" % x)
                raise

    return salvaged_jobs

started_registries = []


def checkDiskQuota():

    import subprocess
    from Ganga.Utility.files import fullpath

    repo_partition = getLocalRoot()
    repo_partition = os.path.realpath(repo_partition)

    work_partition = getLocalWorkspace()
    work_partition = os.path.realpath(work_partition)

    folders_to_check = [repo_partition, work_partition]

    home_dir = os.environ['HOME']
    to_remove = []
    for partition in folders_to_check:
        if not os.path.exists(partition):
            if home_dir not in folders_to_check:
                folders_to_check.append(home_dir)
            to_remove.append(partition)

    for folder in to_remove:
        folders_to_check.remove(folder)

    for data_partition in folders_to_check:

        if fullpath(data_partition).find('/afs') == 0:
            quota = subprocess.Popen(['fs', 'quota', '%s' % data_partition], stdout=subprocess.PIPE)
            output = quota.communicate()[0]
            logger.debug("fs quota %s:\t%s" % (str(data_partition), str(output)))
            quote_percent = output

        else:
            df = subprocess.Popen(["df", '-Pk', data_partition], stdout=subprocess.PIPE)
            output = df.communicate()[0]
            #device, size, used, available, percent, mountpoint = output.split("\n")[1].split()
            output_data = output.split("\n")[1].split()

            quota_percent = output_data[5]

        try:
            quota_percent = output.split('%')[0]
            if int(quota_percent) >= 95:
                logger.warning("WARNING: You're running low on disk space, Ganga may stall on launch or fail to download job output")
                logger.warning("WARNING: Please free some disk space on: %s" % str(data_partition))
        except Exception, err:
            logger.debug("Error checking disk partition: %s" % str(err))

    return

def bootstrap():
    oldJobs = getOldJobs()
    retval = []

    try:
        checkDiskQuota()
    except Exception, err:
        logger.error("Disk quota check failed due to: %s" % str(err))

    # ALEX added this as need to ensure that prep registry is started up BEFORE job or template
    # or even named templated registries as the _auto__init from job will require the prep registry to
    # already be ready. This showed up when adding the named templates.
    def prep_filter(x, y):
        if x.name == 'prep':
            return -1
        return 1

    for registry in sorted(getRegistries(), prep_filter):
        if registry.name in started_registries:
            continue
        if not hasattr(registry, 'type'):
            registry.type = config["repositorytype"]
        if not hasattr(registry, 'location'):
            registry.location = getLocalRoot()
        registry.startup()
        logger.debug("started " + registry.info(full=False))
        if registry.name == "prep":
            registry.print_other_sessions()
        started_registries.append(registry.name)
        retval.append((registry.name, registry.getProxy(), registry.doc))
        if registry.name in oldJobs:
            for j in oldJobs[registry.name]:
                j._index_cache = None
                if not j.id in registry:
                    registry._add(j, force_index=j.id)
                else:
                    logger.warning(
                            "Import Collision at id %i, appending job to the end...", j.id)
                    registry._add(j)
    import atexit
    atexit.register(shutdown)
    logger.debug(started_registries)
    return retval


def updateLocksNow():

    logger.debug("Updating timestamp of Lock files")
    for registry in getRegistries():
        registry.updateLocksNow()
    return


def shutdown():
    logger.debug('registry shutdown')
    # shutting down the prep registry (i.e. shareref table) first is necessary to allow the closedown()
    # method to perform actions on the box and/or job registries.
    logger.debug(started_registries)
    try:
        if 'prep' in started_registries:
            registry = getRegistry('prep')
            registry.shutdown()
            # in case this is called repeatedly, only call shutdown once
            started_registries.remove(registry.name)
    except:
        logger.error(
                "Failed to Shutdown prep Repository!!! please check for stale lock files")
        logger.error("Trying to shutdown cleanly regardless")
        pass

    for registry in getRegistries():
        thisName = registry.name
        try:
            if not thisName in started_registries:
                continue
            # in case this is called repeatedly, only call shutdown once
            started_registries.remove(thisName)
            registry.shutdown()  # flush and release locks
        except Exception as x:
            logger.error(
                    "Failed to Shutdown Repository: %s !!! please check for stale lock files" % thisName)
            logger.error("%s" % str(x))
            logger.error("Trying to Shutdown cleanly regardless")
            pass

    from Ganga.Core.GangaRepository.SessionLock import removeGlobalSessionFiles, removeGlobalSessionFileHandlers
    removeGlobalSessionFileHandlers()
    removeGlobalSessionFiles()
