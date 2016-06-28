import os
import threading
import tempfile
import shutil
import json
import time
from copy import deepcopy

from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.execute import execute
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Credentials import getCredential
import Ganga.Utility.execute as gexecute

logger = getLogger()
proxy = None

# Cache
# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
DIRAC_ENV = {}
DIRAC_INCLUDE = ''
Dirac_Env_Lock = threading.Lock()
Dirac_Proxy_Lock = threading.Lock()

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def getDiracEnv():
    """
    Returns the dirac environment stored in a global dictionary by Ganga.
    Once loaded and stored this is used for executing all DIRAC code in future
    """
    global DIRAC_ENV
    with Dirac_Env_Lock:
        if not DIRAC_ENV:
            cache_file = getConfig('DIRAC')['DiracEnvJSON']
            source_command = getConfig('DIRAC')['DiracEnvSource']
            if cache_file:
                DIRAC_ENV = read_env_cache(cache_file)
            elif source_command:
                DIRAC_ENV = get_env(source_command)
            else:
                logger.error("'DiracEnvSource' config variable empty")
    return DIRAC_ENV


def get_env(env_source):
    """
    Given a source command, return the DIRAC environment that the
    command created.

    Args:
        env_source: a command which can be sourced, providing the desired environment

    Returns:
        dict: the environment

    """
    logger.debug('Running DIRAC source command %s', env_source)
    env = dict(os.environ)
    execute('source {0}'.format(env_source), shell=True, env=env, update_env=True)
    if not any(key.startswith('DIRAC') for key in env):
        raise RuntimeError("'DIRAC*' not found in environment")
    return env


def write_env_cache(env, cache_filename):
    """
    Given a command and a file path, source the command and store it
    in the file

    Args:
        env (dict): the environment
        cache_filename: a full path to a file to store the cache in

    """
    cache_dir = os.path.dirname(cache_filename)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    with open(cache_filename, 'w') as cache_file:
        json.dump(env, cache_file)


def read_env_cache(cache_filename):
    """
    Args:
        cache_filename: a full path to a file to store the cache in

    Returns:
        dict: the cached environment

    """
    logger.debug('Reading DIRAC cache file at %s', cache_filename)
    with open(cache_filename, 'r') as cache_file:
        env = json.load(cache_file)
    # Convert unicode strings to byte strings
    env = dict((k.encode('utf-8'), v.encode('utf-8')) for k, v in env.items())
    return env

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def getDiracCommandIncludes(force=False):
    """
    This helper function returns the Ganga DIRAC helper functions which are called by Ganga code to talk to DIRAC
    These are loaded from disk once and then saved in memory.
    Args:
        force (bool): Triggers a reload from disk when True
    """
    global DIRAC_INCLUDE
    if DIRAC_INCLUDE == '' or force:
        for fname in getConfig('DIRAC')['DiracCommandFiles']:
            if not os.path.exists(fname):
                raise GangaException("Specified Dirac command file '%s' does not exist." % fname)
            with open(fname, 'r') as inc_file:
                DIRAC_INCLUDE += inc_file.read() + '\n'

    return DIRAC_INCLUDE

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def getValidDiracFiles(job, names=None):
    """
    This is a generator for all DiracFiles in a jobs outputfiles
    TODO: Is this still called anywhere?
    Args:
        job (Job): The job which is having it's DiracFiles tested
        names (list): list of strings of names to be matched to namePatterns in outputfiles
    """
    from GangaDirac.Lib.Files.DiracFile import DiracFile
    from Ganga.GPIDev.Base.Proxy import isType
    if job.subjobs:
        for sj in job.subjobs:
            for df in (f for f in sj.outputfiles if isType(f, DiracFile)):
                if df.subfiles:
                    for valid_sf in (sf for sf in df.subfiles if sf.lfn != '' and (names is None or sf.namePattern in names)):
                        yield valid_sf
                else:
                    if df.lfn != '' and (names is None or df.namePattern in names):
                        yield df
    else:
        for df in (f for f in job.outputfiles if isType(f, DiracFile)):
            if df.subfiles:
                for valid_sf in (sf for sf in df.subfiles if sf.lfn != '' and (names is None or sf.namePattern in names)):
                    yield valid_sf
            else:
                if df.lfn != '' and (names is None or df.namePattern in names):
                    yield df

last_modified_time = None
last_modified_valid = False

# This will move/change when new credential system in place
############################


def _dirac_check_proxy( renew = True, shouldRaise = True):
    """
    This function checks the validity of the DIRAC proxy
    Args:
        renew (bool): When True this will require a proxy to be valid before we proceed. False means raise Exception when expired
    """
    global last_modified_valid
    global proxy
    if proxy is None:
        proxy = getCredential('GridProxy')
    _isValid = proxy.isValid()
    if not _isValid:
        if renew is True:
            proxy.renew()
            if not proxy.isValid():
                last_modified_valid = False
                if shouldRaise:
                    raise GangaException('Can not execute DIRAC API code w/o a valid grid proxy.')
            else:
                last_modified_valid = True
        else:
            last_modified_valid = False
    else:
        last_modified_valid = True
############################

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

def _proxyValid(shouldRenew = True, shouldRaise = True):
    """
    This function is a wrapper for the _checkProxy with a default of False for renew. Returns the last modified time global object
    """
    _checkProxy( renew = shouldRenew, shouldRaise = shouldRaise )
    return last_modified_valid

def _checkProxy( delay=60, renew = True, shouldRaise = True, force = False ):
    """
    Check the validity of the DIRAC proxy. If it's marked as valid, check once every 'delay' seconds.
    Args:
        delay (int): number of seconds between calls to the tools to test the proxy
        renew (bool): If True, trigger the regeneration of a valid proxy
    """
    ## Handling mutable globals in a multi-threaded system to remember to LOCK
    with Dirac_Proxy_Lock:
        ## Function to check for a valid proxy once every 60( or n) seconds
        global last_modified_time
        if last_modified_time is None:
            # This will move/change when new credential system in place
            ############################
            _dirac_check_proxy( renew, shouldRaise )
            ############################
            last_modified_time = time.time()

        if (time.time() - last_modified_time) > int(delay) or not last_modified_valid or force:
            _dirac_check_proxy( renew, shouldRaise )
            last_modified_time = time.time()


def execute(command,
            timeout=getConfig('DIRAC')['Timeout'],
            env=None,
            cwd=None,
            shell=False,
            python_setup='',
            eval_includes=None,
            update_env=False,
            ):
    """
    Execute a command on the local DIRAC server.

    This function blocks until the server returns.
    
    Args:
        command (str): This is the command we're running within our DIRAC session
        timeout (int): This is the length of time that a DIRAC call has before it's decided some interaction has timed out
        env (dict): an optional environment to execute the DIRAC code in
        cwd (str): an optional string to a valid path where this code should be executed
        shell (bool): Should this code be executed in a new shell environment
        python_setup (str): Optional extra code to pass to python when executing
        eval_includes (???): TODO document me
        update_env (bool): Should this modify the given env object with the env after the command has executed
    """

    if env is None:
        env = getDiracEnv()
    if python_setup == '':
        python_setup = getDiracCommandIncludes()

    # We're about to perform an expensive operation so being safe before we run it shouldn't cost too much
    _checkProxy(force = True)

    #logger.debug("Executing command:\n'%s'" % str(command))
    #logger.debug("python_setup:\n'%s'" % str(python_setup))
    #logger.debug("eval_includes:\n'%s'" % str(eval_includes))

    if cwd is None:
        # We can in all likelyhood be in a temp folder on a shared (SLOW) filesystem
        # If we are we do NOT want to execute commands which will involve any I/O on the system that isn't needed
        cwd_ = tempfile.mkdtemp()
    else:
        # We know were whe want to run, lets just run there
        cwd_ = cwd

    global last_modified_valid
    if not last_modified_valid:
        return None

    returnable = gexecute.execute(command,
                                  timeout=timeout,
                                  env=env,
                                  cwd=cwd_,
                                  shell=shell,
                                  python_setup=python_setup,
                                  eval_includes=eval_includes,
                                  update_env=update_env)

    # TODO we would like some way of working out if the code has been executed correctly
    # Most commands will be OK now that we've added the check for the valid proxy before executing commands here

    if cwd is None:
        shutil.rmtree(cwd_, ignore_errors=True)

    return returnable

