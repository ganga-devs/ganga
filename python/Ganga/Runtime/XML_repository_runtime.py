import os
from Ganga.Utility.logging import getLogger,log_user_exception
logger = getLogger()
import Ganga.Utility.Config
config = Ganga.Utility.Config.makeConfig('LocalXML_Repository','Parameters of the local XML-based job repository')
config.addOption('DEBUG_startup_profile',False,'(ADVANCED DEBUGGING) enable/disable profiling of job repository at startup')

c_config = Ganga.Utility.Config.getConfig('Configuration')

# this is set by the Repository_runtime module
getLocalRoot = None

#keep the (names,regs)
names=[]
regs=[]

def requiresGridProxy():
    return False

def bootstrap():
    global names,regs

    names = ['jobs','templates']
    docstrings = ['default job registry', 'default registry of job templates']

    from Ganga.Core.JobRepositoryXML import factory, version

    from Ganga.Core.exceptions import RepositoryError

    def print_error(x):
        for c in config:
            logger.error('%s = %s',c,config[c])
        s = 'Cannot connect to the repository: '+str(x)
        logger.error(s)        
        return s

    reps = []
    try:
        for n in names:
            reps.append(factory(dir = os.path.join(getLocalRoot(),version,n)))
    except RepositoryError as x:
        s = print_error(x)
        raise
    except Exception as x:
        s = print_error(x)
        log_user_exception(logger)
        raise
        
    from Ganga.GPIDev.Lib.JobRegistry import JobRegistryInstance, JobRegistryInterface, allJobRegistries

    regs = map(lambda x: JobRegistryInstance(*x), zip(names,reps))

    for n,r in zip(names,regs):
        allJobRegistries['native_'+n] = r
        if n == 'jobs' and config['DEBUG_startup_profile']:
            PROFN = 'xml.startup.profile.txt'
            logger.debug('profiling ON, saving status to '+PROFN)
            import profile
            profile.runctx('r._scan_repository()',globals(),{'r':r},PROFN)
        else:
            try:
                r._scan_repository()
            except Exception as x:
                s = print_error(x)
                log_user_exception(logger)
                logger.critical('the repository cannot be loaded (dir=%s)',r.repository.dir)
                import tempfile, tarfile
                fn = tempfile.mktemp(suffix='.tgz',prefix='Ganga-Repository-XML')
                tf = tarfile.open(fn,mode='w:gz')
                tf.add(r.repository.dir,"Ganga-Repository-XML",recursive=True)
                tf.close()
                logger.critical('the repository was dumped to %s (%s) for debugging',fn,Ganga.Utility.util.hostname())
                raise x

        # commit the migrated jobs (if any)
        #r._flush(migrated_jobs)
        #migrated_jobs[:] = []

    proxies = map(lambda x: JobRegistryInterface(x), regs)

    logger.debug('registred repository atexit handler')
    
    import atexit
    atexit.register(shutdown)
    
    return zip(names,proxies,docstrings)


def shutdown():
    global names,regs
    from Ganga.Runtime.Repository_runtime import _shutdown
    _shutdown(names,regs)

