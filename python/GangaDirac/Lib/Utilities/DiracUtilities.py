import os
import base64
import subprocess
import threading
import pickle
import signal
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Core.exceptions import GangaException
from Ganga.GPIDev.Credentials import getCredential
import Ganga.Utility.execute as gexecute


import time
import math
import copy
import inspect

logger = getLogger()
proxy = getCredential('GridProxy', '')

# Cache
# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
DIRAC_ENV = {}
DIRAC_INCLUDE = ''
Dirac_Env_Lock = threading.Lock()
Dirac_Proxy_Lock = threading.Lock()

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def getDiracEnv(force=False):
    """
    Function to get and load the DIRAC env from disk and store it in a global cached dictionary
    Args:
        force (bool): Forces the (re)loading of the env from disk again
    """
    global DIRAC_ENV
    with Dirac_Env_Lock:
        if DIRAC_ENV == {} or force:
            config_file = getConfig('DIRAC')['DiracEnvFile']
            if not os.path.exists(config_file):
                absolute_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../..', config_file)
            else:
                absolute_path = config_file
            if getConfig('DIRAC')['DiracEnvFile'] != "" and os.path.exists(absolute_path):
                with open(absolute_path, 'r') as env_file:
                    DIRAC_ENV = dict((tuple(line.strip().split('=', 1)) for line in env_file.readlines(
                    ) if len(line.strip().split('=', 1)) == 2))
                    keys_to_remove = []
                    for k, v in DIRAC_ENV.iteritems():
                        if str(v).startswith('() {'):
                            keys_to_remove.append(k)
                    for key in keys_to_remove:
                        del DIRAC_ENV[key]

            else:
                logger.error("'DiracEnvFile' config variable empty or file not present")
                logger.error("Tried looking in : '%s' Please check your config" % absolute_path) 
    logger.debug("Dirac Env: %s" % DIRAC_ENV)
    return DIRAC_ENV

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\


def getDiracCommandIncludes(force=False):
    """
    Function to get and load the DIRAC commands used to translate requests between ganga/DIRAC
    Args:
        force (bool): Forces the (re)loading of the code from disk again
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


def _dirac_check_proxy( renew = True):
    """
    Function to check the balidity of a DIRAC proxy
    Args:
        renew (bool): Will trigger the proxy to be regenerated if needed when True
    """
    global last_modified_valid
    global proxy
    _isValid = proxy.isValid()
    if not _isValid:
        if renew is True:
            proxy.renew()
            if not proxy.isValid():
                last_modified_valid = False
                raise GangaException('Can not execute DIRAC API code w/o a valid grid proxy.')
            else:
                last_modified_valid = True
        else:
            last_modified_valid = False
    else:
        last_modified_valid = True
############################

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

def _proxyValid():
    """
    Function to check if the proxy is valid without triggering it to be regenerated
    """
    _checkProxy( renew = False )
    return last_modified_valid

def _checkProxy( delay=60, renew = True ):
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
            _dirac_check_proxy( True )
            ############################
            last_modified_time = time.time()

        if abs(last_modified_time - time.time()) > int(delay):
            _dirac_check_proxy( renew )
            last_modified_time = time.time()


def execute(command,
            timeout=getConfig('DIRAC')['Timeout'],
            env=None,
            cwd=None,
            shell=False,
            python_setup='',
            eval_includes=None,
            update_env=False):
    """
    Execute a command on the local DIRAC server.

    This function blocks until the server returns.
    
    Args:
        command (str): The command to be executed
        timeout (int): The time a command is given before it's assumed it may have timed out
        env (dict): Optional dict in which the command is to be executed
        cwd (str): Optional, the path where a command is to be run
        shell (bool): Should we launch a new shell for these commands
        python_setup (str): Unclear what this is used for TODO: DOCUMENT BETTER
        eval_includes (None): Unclear what this is used for TODO: DOCUMENT BETTER
        update_env (bool): Should the env passed to the function be updated after command has been run
    """

    if env is None:
        env = getDiracEnv()
    if python_setup == '':
        python_setup = getDiracCommandIncludes()

    _checkProxy()

    #logger.info("Executing command:\n'%s'" % str(command))
    #logger.debug("python_setup:\n'%s'" % str(python_setup))
    #logger.debug("eval_includes:\n'%s'" % str(eval_includes))

    returnable = gexecute.execute(command,
                                  timeout=timeout,
                                  env=env,
                                  cwd=cwd,
                                  shell=shell,
                                  python_setup=python_setup,
                                  eval_includes=eval_includes,
                                  update_env=update_env)

    #TODO: can we just use deepcopy here on the returned object?

    # rcurrie I've seen problems with just returning this raw object,
    # expanding it to be sure that an instance remains in memory
    myObject = {}
    if hasattr(returnable, 'keys'):
        # Expand object(s) in dictionaries
        myObject = _expand_object(returnable)
    elif type(returnable) in[list, tuple]:
        # Expand object(s) in lists
        myObject = _expand_list(returnable)
    else:
        # Copy object(s) so thet they definately are in memory
        myObject = copy.deepcopy(returnable)

    return myObject


def _expand_object(myobj):
    """
    An attempt to go into children of a dict object and deepcopy the contents
    Args:
        myobj (dict): dictionary to be descended into and copied
    """
    new_obj = {}
    if hasattr(myobj, 'keys'):
        for key in myobj.keys():
            value = myobj.get(key)
            if hasattr(value, 'keys'):
                new_obj[key] = _expand_object(value)
            else:
                new_obj[key] = copy.deepcopy(value)
    return new_obj


def _expand_list(mylist):
    """
    An attempt to go into children of a list object and deepcopy the contents
    Args:
        mylist (list): list to be descended into and copied
    """
    new_list = []
    for element in mylist:
        new_list.append(copy.deepcopy(element))
    return new_list

