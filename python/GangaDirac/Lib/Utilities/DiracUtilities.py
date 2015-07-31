import os, base64, subprocess, threading, pickle, signal
from Ganga.Utility.Config  import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Credentials2 import credential_store
import Ganga.Utility.execute as gexecute
logger = getLogger()

## Cache
# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
DIRAC_ENV={}
DIRAC_INCLUDE=''
Dirac_Env_Lock = threading.Lock()

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def getDiracEnv(force=False):
    global DIRAC_ENV
    global Dirac_Env_Lock
    lock = Dirac_Env_Lock
    lock.acquire()
    if DIRAC_ENV == {} or force:
        if getConfig('DIRAC')['DiracEnvFile'] != "" and os.path.exists(getConfig('DIRAC')['DiracEnvFile']):
            with open(getConfig('DIRAC')['DiracEnvFile'],'r') as env_file:
                DIRAC_ENV = dict((tuple(line.strip().split('=',1)) for line in env_file.readlines() if len(line.strip().split('=',1)) == 2))
                keys_to_remove = []
                for k, v in DIRAC_ENV.iteritems():
                    if str(v).startswith('() {'):
                        keys_to_remove.append( k )
                for key in keys_to_remove:
                    del DIRAC_ENV[ key ]

        else:
            logger.error("'DiracEnvFile' config variable empty or file not present")
    lock.release()
    #print DIRAC_ENV
    return DIRAC_ENV

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def getDiracCommandIncludes(force=False):
    global DIRAC_INCLUDE
    if DIRAC_INCLUDE == '' or force:
        for fname in getConfig('DIRAC')['DiracCommandFiles']:
            if not os.path.exists(fname):
                raise GangaException("Specified Dirac command file '%s' does not exist." % fname )
            with open(fname, 'r') as inc_file:
                DIRAC_INCLUDE += inc_file.read() + '\n'

    return DIRAC_INCLUDE

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def getValidDiracFiles(job, names=None):
    from GangaDirac.Lib.Files.DiracFile import DiracFile
    if job.subjobs:
        for sj in job.subjobs:
            for df in (f for f in sj.outputfiles if isinstance(f, DiracFile)):
                if df.subfiles:
                    for valid_sf in (sf for sf in df.subfiles if sf.lfn!='' and (names is None or sf.namePattern in names)):
                        yield valid_sf
                else:
                    if df.lfn!='' and (names is None or df.namePattern in names):
                        yield df
    else:
        for df in (f for f in job.outputfiles if isinstance(f, DiracFile)):
            if df.subfiles:
                for valid_sf in (sf for sf in df.subfiles if sf.lfn!='' and (names is None or sf.namePattern in names)):
                    yield valid_sf
            else:
                if df.lfn!='' and (names is None or df.namePattern in names):
                    yield df


# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def execute(command,
            timeout       = getConfig('DIRAC')['Timeout'],
            env           = None,
            cwd           = None,
            shell         = False,
            python_setup  = '',
            eval_includes = None,
            update_env    = False,
            cred_req      = None):
    """
    Execute a command on the local DIRAC server.
    
    This function blocks until the server returns.
    """

    if env is None:
        env = getDiracEnv()
    if python_setup == '':
        python_setup = getDiracCommandIncludes()

    if cred_req is not None:
        env = env or {}
        env['X509_USER_PROXY'] = credential_store[cred_req].location

    return gexecute.execute(command,
                            timeout       = timeout,
                            env           = env,
                            cwd           = cwd,
                            shell         = shell,
                            python_setup  = python_setup,
                            eval_includes = eval_includes,
                            update_env    = update_env)
