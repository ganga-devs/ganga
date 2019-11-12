import os
import errno
import threading
import datetime
import tempfile
import shutil
import json
import time
import socket
import re
from copy import deepcopy
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger
from GangaCore.Core.exceptions import GangaException
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.GPIDev.Credentials import credential_store
import GangaCore.Utility.execute as gexecute
logger = getLogger()

# Cache
# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
DIRAC_ENV = {}
DIRAC_INCLUDE = ''
Dirac_Env_Lock = threading.Lock()
Dirac_Proxy_Lock = threading.Lock()
Dirac_Exec_Lock = threading.Lock()
# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

class GangaDiracError(GangaException):
    """ Exception type which is thrown from problems executing a command against DIRAC """
    def __init__(self, message, dirac_id = None, job_id = None):
        GangaException.__init__(self, message)
        self.dirac_id = dirac_id
        self.job_id = job_id
    def __str__(self):
        if self.job_id and self.dirac_id:
            return "GangaDiracError, Job %s with Dirac ID %s : %s" % (self.job_id, self.dirac_id, self.message)
        else:
            return GangaException.__str__(self)


def getDiracEnv(sourceFile = None):
    """
    Returns the dirac environment stored in a global dictionary by GangaCore.
    Once loaded and stored this is used for executing all DIRAC code in future
    Args:
        sourceFile (str): This is an optional file path which points to the env which should be sourced for this DIRAC
    """
    global DIRAC_ENV
    with Dirac_Env_Lock:
        if sourceFile is None:
            sourceFile = 'default'
            cache_file = getConfig('DIRAC')['DiracEnvJSON']
            source_command = getConfig('DIRAC')['DiracEnvSource']
            if not cache_file and not source_command:
                source_command = getConfig('DIRAC')['DiracEnvFile']
        else:
            # Needed for backwards compatibility with old configs...
            cache_file = None
            source_command = sourceFile

        if sourceFile not in DIRAC_ENV:
            if cache_file:
                DIRAC_ENV[sourceFile] = read_env_cache(cache_file)
            elif source_command:
                DIRAC_ENV[sourceFile] = get_env(source_command)
            else:
                logger.error("'DiracEnvSource' config variable empty")
                logger.error("%s  %s" % (getConfig('DIRAC')['DiracEnvJSON'], getConfig('DIRAC')['DiracEnvSource']))

        #In case of custom location
        if os.getenv('X509_USER_PROXY'):
            DIRAC_ENV[sourceFile]['X509_USER_PROXY'] = os.getenv('X509_USER_PROXY')
    return DIRAC_ENV[sourceFile]


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
    gexecute.execute('source {0}'.format(env_source), shell=True, env=env, update_env=True)
    if not any(key.startswith('DIRAC') for key in env):
        fake_dict = {}
        with open(env_source) as _env:
            for _line in _env.readlines():
                split_val = _line.split('=')
                if len(split_val) == 2:
                    key = split_val[0]
                    value = split_val[1]
                    fake_dict[key] = value
        if not any(key.startswith('DIRAC') for key in fake_dict):
            logger.error("Env: %s" % str(env))
            logger.error("Fake: %s" % str(fake_dict))
            raise RuntimeError("'DIRAC*' not found in environment")
        else:
            return fake_dict
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
    with Dirac_Env_Lock:
        if DIRAC_INCLUDE == '' or force:
            for fname in getConfig('DIRAC')['DiracCommandFiles']:
                if not os.path.exists(fname):
                    raise RuntimeError("Specified Dirac command file '%s' does not exist." % fname)
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


def execute(command,
            timeout=getConfig('DIRAC')['Timeout'],
            env=None,
            cwd=None,
            shell=False,
            python_setup='',
            eval_includes=None,
            update_env=False,
            return_raw_dict=False,
            cred_req=None,
            new_subprocess = False
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
        return_raw_dict(bool): Should we return the raw dict from the DIRAC interface or parse it here
        cred_req (ICredentialRequirement): What credentials does this call need
        new_subprocess(bool): Do we want to do this in a fresh subprocess or just connect to the DIRAC server process?
    """

    if cwd is None:
        # We can in all likelyhood be in a temp folder on a shared (SLOW) filesystem
        # If we are we do NOT want to execute commands which will involve any I/O on the system that isn't needed
        cwd_ = tempfile.mkdtemp()
    else:
        # We know were whe want to run, lets just run there
        cwd_ = cwd

    from GangaDirac.BOOT import startDiracProcess
    returnable = ''
    if not new_subprocess:
        with Dirac_Exec_Lock:
            # First check if a Dirac process is running
            from GangaDirac.BOOT import running_dirac_process
            if not running_dirac_process:
                startDiracProcess()
            #Set up a socket to connect to the process
            from GangaDirac.BOOT import dirac_process_ids
            HOST = 'localhost'  # The server's hostname or IP address
            PORT = dirac_process_ids[1]        # The port used by the server

            #Put inside a try/except in case the existing process has timed out
            try:
                s= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((HOST, PORT))
            except socket.error as serr:
                #Start a new process
                startDiracProcess()
                from GangaDirac.BOOT import dirac_process_ids
                PORT = dirac_process_ids[1]
                s= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((HOST, PORT))

            #Send a random string, then change the directory to carry out the command, then send the command
            command_to_send  = str(dirac_process_ids[2])
            command_to_send += 'os.chdir("%s")\n' % cwd_
            command_to_send += command
            s.sendall(('%s###END-TRANS###' % command_to_send).encode('utf-8'))
            out = ''
            while '###END-TRANS###' not in out:
                data = s.recv(1024)
                out += data.decode("utf-8")
            s.close()
            #Some regex nonsense to deal with the long representations in python 3
            out = re.sub(r'((?:^|\s|,|{|\()\d+)L([^A-Za-z0-9\"\'])', r'\1\2', out)
            returnable = eval(out)

    else:
        if env is None:
            if cred_req is None:
                env = getDiracEnv()
            else:
                env = getDiracEnv(cred_req.dirac_env)
        if python_setup == '':
            python_setup = getDiracCommandIncludes()

        if cred_req is not None:
            env['X509_USER_PROXY'] = credential_store[cred_req].location
            if os.getenv('KRB5CCNAME'):
                env['KRB5CCNAME'] = os.getenv('KRB5CCNAME')

        returnable = gexecute.execute(command,
                                      timeout=timeout,
                                      env=env,
                                      cwd=cwd_,
                                      shell=shell,
                                      python_setup=python_setup,
                                      eval_includes=eval_includes,
                                      update_env=update_env)

        # If the time 
        if returnable == 'Command timed out!':
            raise GangaDiracError("DIRAC command timed out")

        # TODO we would like some way of working out if the code has been executed correctly
        # Most commands will be OK now that we've added the check for the valid proxy before executing commands here

    if cwd is None:
        shutil.rmtree(cwd_, ignore_errors=True)

    if isinstance(returnable, dict):
        if return_raw_dict:
            # If the output is a dictionary return and it has been requested, then return it
            return returnable
        # If the output is a dictionary allow for automatic error detection
        if returnable['OK']:
            return returnable['Value']
        else:
            raise GangaDiracError(returnable['Message'])
    else:
        # Else raise an exception as it should be a dictionary
        raise GangaDiracError(returnable)

