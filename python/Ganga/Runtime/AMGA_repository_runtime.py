
from Ganga.Utility.logging import getLogger, log_user_exception
logger = getLogger()
import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('Configuration')

# keep the (names,regs)
names = []
regs = []


def requiresGridProxy():
    rtype = config['repositorytype']
    if 'Remote' in rtype:
        remote_config = Ganga.Utility.Config.getConfig("%s_Repository" % rtype)
        return bool(remote_config['reqSSL'])
    else:
        return False


def bootstrap():
    import os.path

    global names, regs

    names = ['jobs', 'templates']
    docstrings = ['default job registry', 'default registry of job templates']

    # debug
    for c in config:
        logger.debug('%s = %s', c, config[c])

    from Ganga.Core.JobRepository.ARDA import repositoryFactory
    from Ganga.Core.exceptions import RepositoryError

    def print_error(x):
        for c in config:
            logger.error('%s = %s', c, config[c])
        if config['repositorytype'] == 'RemoteAMGA':
            logger.error('check login name, host and port number')
        s = 'Cannot connect to the repository: ' + str(x)
        logger.error(s)
        return s

    reps = []
    try:
        for n in names:
            reps.append(repositoryFactory(subpath=n))
    except RepositoryError as x:
        s = print_error(x)
        raise
    except Exception as x:
        s = print_error(x)
        log_user_exception(logger)
        raise

    from Ganga.GPIDev.Lib.JobRegistry import JobRegistryInstance, JobRegistryInterface, allJobRegistries

    regs = map(lambda x: JobRegistryInstance(*x), zip(names, reps))

    from Ganga.GPIDev.Streamers.MigrationControl import migrated_jobs
    assert(not migrated_jobs)  # initially the list should be empty

    for n, r in zip(names, regs):
        allJobRegistries['native_' + n] = r
        r._scan_repository()
        # commit the migrated jobs (if any)
        r._flush(migrated_jobs)
        migrated_jobs[:] = []

    proxies = map(lambda x: JobRegistryInterface(x), regs)

    logger.debug('registred repository atexit handler')

    import atexit
    atexit.register(shutdown)

    return zip(names, proxies, docstrings)


def shutdown():
    global names, regs
    from Ganga.Runtime.Repository_runtime import _shutdown
    _shutdown(names, regs)
