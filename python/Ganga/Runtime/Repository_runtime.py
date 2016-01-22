"""
Internal initialization of the repositories.
"""

import Ganga.Utility.Config
from Ganga.Utility.logging import getLogger
import os.path
from Ganga.Utility.files import expandfilename, fullpath
from Ganga.Core.GangaRepository import getRegistries
from Ganga.Core.GangaRepository import getRegistry
from Ganga.Core.exceptions import GangaException

config = Ganga.Utility.Config.getConfig('Configuration')
logger = getLogger()


def requiresAfsToken():
    return fullpath(getLocalRoot(), True).find('/afs') == 0


def requiresGridProxy():
    return False


def getLocalRoot():
    if config['repositorytype'] in ['LocalXML', 'LocalAMGA', 'LocalPickle', 'SQLite']:
        return os.path.join(expandfilename(config['gangadir'], True), 'repository', config['user'], config['repositorytype'])
    else:
        return ''

def getLocalWorkspace():
    if config['repositorytype'] in ['LocalXML', 'LocalAMGA', 'LocalPickle', 'SQLite']:
        return os.path.join(expandfilename(config['gangadir'], True), 'workspace', config['user'], config['repositorytype'])
    else:
        return ''


started_registries = []

partition_warning = 95
partition_critical = 99

def checkDiskQuota():

    import subprocess

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

        if fullpath(data_partition, True).find('/afs') == 0:
            quota = subprocess.Popen(['fs', 'quota', '%s' % data_partition], stdout=subprocess.PIPE)
            output = quota.communicate()[0]
            logger.debug("fs quota %s:\t%s" % (str(data_partition), str(output)))
        else:
            df = subprocess.Popen(["df", '-Pk', data_partition], stdout=subprocess.PIPE)
            output = df.communicate()[0]

        try:
            global partition_warning
            global partition_critical
            quota_percent = output.split('%')[0]
            if int(quota_percent) >= partition_warning:
                logger.warning("WARNING: You're running low on disk space, Ganga may stall on launch or fail to download job output")
                logger.warning("WARNING: Please free some disk space on: %s" % str(data_partition))
            if int(quota_percent) >= partition_critical and config['force_start'] is False:
                logger.error("You are crtitically low on disk space!")
                logger.error("To prevent repository corruption and data loss we won't start Ganga.")
                logger.error("Either set your config variable 'force_start' in .gangarc to enable starting and ignore this check.")
                logger.error("Or, make sure you have more than %s percent free disk space on: %s" %(str(100-partition_critical), str(data_partition)))
                raise GangaException("Not Enough Disk Space!!!")
        except GangaException as err:
            raise err
        except Exception as err:
            logger.debug("Error checking disk partition: %s" % str(err))

    return

def bootstrap():
    retval = []

    try:
        checkDiskQuota()
    except GangaException as err:
        raise err
    except Exception as err:
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

    #import atexit
    #atexit.register(shutdown)
    #logger.debug("Registries: %s" % str(started_registries))
    return retval


def updateLocksNow():

    logger.debug("Updating timestamp of Lock files")
    for registry in getRegistries():
        registry.updateLocksNow()
    return


def shutdown():
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    logger.info('Registry Shutdown')
    #import traceback
    #traceback.print_stack()
    # shutting down the prep registry (i.e. shareref table) first is necessary to allow the closedown()
    # method to perform actions on the box and/or job registries.
    logger.debug(started_registries)
    try:
        if 'prep' in started_registries:
            registry = getRegistry('prep')
            registry.shutdown()
            # in case this is called repeatedly, only call shutdown once
            started_registries.remove(registry.name)
    except Exception as err:
        logger.debug("Err: %s" % str(err))
        logger.error("Failed to Shutdown prep Repository!!! please check for stale lock files")
        logger.error("Trying to shutdown cleanly regardless")

    for registry in getRegistries():
        thisName = registry.name
        try:
            if not thisName in started_registries:
                continue
            # in case this is called repeatedly, only call shutdown once
            started_registries.remove(thisName)
            registry.shutdown()  # flush and release locks
        except Exception as x:
            logger.error("Failed to Shutdown Repository: %s !!! please check for stale lock files" % thisName)
            logger.error("%s" % str(x))
            logger.error("Trying to Shutdown cleanly regardless")

    from Ganga.Core.GangaRepository.SessionLock import removeGlobalSessionFiles, removeGlobalSessionFileHandlers
    removeGlobalSessionFileHandlers()
    removeGlobalSessionFiles()

def flush_all():
    from Ganga.Utility.logging import getLogger
    logger = getLogger()
    logger.debug("Flushing All repositories")

    for registry in getRegistries():
        thisName = registry.name
        try:
            if registry.hasStarted() is True:
                logger.debug("Flushing: %s" % thisName)
                registry._flush()
        except Exception as err:
            logger.debug("Failed to flush: %s" % str(thisName))
            logger.debug("Err: %s" % str(err))

