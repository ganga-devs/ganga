import subprocess
from datetime import datetime
import time
import uuid
from os import makedirs, path
from GangaCore.Core.exceptions import GangaException
from GangaCore.Runtime.GPIexport import exportToGPI
from GangaCore.Utility.files import expandfilename
from GangaCore.Utility.logging import getLogger
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.Core.exceptions import ApplicationPrepareError
from .PythonOptsCmakeParser import PythonOptsCmakeParser

logger = getLogger()

def gaudiPythonWrapper(sys_args, options, data_file, script_file):
    """
    Returns the script which is used by GaudiPython to wrap the job on the WN
    Args:
        sys_args
    """
    wrapper_script = """
from Gaudi.Configuration import *

import sys
sys.argv += %s

# Import extra Opts
importOptions('%s')

# Import data.py
importOptions('%s')

for script_file in %s:
    importOptions( script_file )

# Run the command
execfile('%s')
""" % (sys_args, options, data_file, repr(script_file[1:]), script_file[0])
    return wrapper_script

def getTimestampContent():
    """
    Returns a string containing the current time in a given format and a unique random uuid
    """
    fmt = '%Y-%m-%d-%H-%M-%S'
    return datetime.now().strftime(fmt) + '\n' + str(uuid.uuid4())

def addTimestampFile(given_path, fileName='__timestamp__'):
    """
    This creates a file in this directory given called __timestamp__ which contains the time so that the final file is unique
    I also add a unique UUID which reduces the risk of collisions between users
    Args:
        given_path (str): Path which we want to create the timestamp within
    """
    time_filename = path.join(given_path, fileName)
    logger.debug("Constructing: %s" % time_filename)
    with open(time_filename, 'a+') as time_file:
        time_file.write(getTimestampContent())

def getGaudiExecInputData(optsfiles, app):
    '''Returns a LHCbDataSet object from a list of options files. The
       optional argument extraopts will decide if the extraopts string inside
       the application is considered or not.

    Usage example:
    # Get the data from an options file and assign it to the jobs inputdata field
    j.inputdata = getGaudiExecInputData([\"~/cmtuser/DaVinci_v22r0p2/Tutorial/Analysis/options/Bs2JpsiPhi2008.py\"], j.application)

    This is also called behind the scenes for j.readInputData([\"~/cmtuser/DaVinci_v22r0p2/Tutorial/Analysis/options/Bs2JpsiPhi2008.py\"])

    '''
    if not isinstance(optsfiles, list):
        optsfiles = [optsfiles]

    # use a dummy file to keep the parser happy
    if len(optsfiles) == 0:
        raise GangaException("Need a file to parse to call this method")

    try:
        parser = PythonOptsCmakeParser(optsfiles, app)
    except Exception as err:
        msg = 'Unable to parse the job options. Please check options files and extraopts.'
        logger.error("PythonOptsCmakeParserError:\n%s" % str(err))
        raise ApplicationConfigurationError(msg)

    return parser.get_input_data()

exportToGPI('getGaudiExecInputData', getGaudiExecInputData, 'Functions')

def prepare_cmake_app(myApp, myVer, myPath='$HOME/cmtuser', myUse=None, myFolder=None , myBranch='master'):
    """
        Short helper function for setting up minimal application environments on disk for job submission
        Args:
            myApp (str): This is the name of the app to pass to lb-dev
            myVer (str): This is the version of 'myApp' to pass to lb-dev
            myPath (str): This is where lb-dev will be run
            myUse (str): This is a git lb-use which will be run once the lb-dev has executed
            myFolder (str): This is a git lb-checkout after the lb-use. 
            myBranch (str): This is the branch used for the lb-checkout. Master branch assumed if not specified
    """

    full_path = expandfilename(myPath, True)
    if not path.exists(full_path):
        makedirs(full_path)

    #First guess a suitable platform for checking out the application
    verStat, verOut, verErr = _exec_cmd('source /cvmfs/lhcb.cern.ch/lib/LbEnv && lb-sdb-query listPlatforms %s %s' % (myApp, myVer), full_path)
    if verStat != 0 or len(verOut.decode().split('\n'))==0:
        logger.error("lb-sdb-query listPlatforms %s %s failed!" % (myApp, myVer))
        raise ApplicationPrepareError(verErr)

    platformToUse = verOut.decode().split('\n')[-2]

    if not path.exists(full_path + '/' + myApp + 'Dev_' +myVer):
        devStat, devOut, devErr = _exec_cmd('source /cvmfs/lhcb.cern.ch/lib/LbEnv && source LbLogin.sh --cmtconfig=%s && lb-dev %s/%s' % (platformToUse, myApp, myVer), full_path)
        logger.info("Running lb-dev %s %s with platform %s" % (myApp, myVer, platformToUse))
        if devStat != 0:
            logger.error("lb-dev %s %s failed!" % (myApp, myVer))
            raise ApplicationPrepareError(devErr)
    else:
        raise GangaException("Path %s already exists. Not checking out application. Try a different location." % str(full_path + '/' + myApp + 'Dev_' +myVer))
    dev_dir = path.join(full_path, myApp + 'Dev_' + myVer)
    logger.info("Set up App Env at: %s" % dev_dir)
    if myUse:
        lbUse, lbUseOut, lbUseErr = _exec_cmd('source /cvmfs/lhcb.cern.ch/lib/LbEnv && source LbLogin.sh --cmtconfig=%s && git lb-use %s' % (platformToUse, myUse), dev_dir)
        logger.info("Running git lb-use %s" % myUse)
        if lbUse != 0:
            logger.error("git lb-use %s failed!" % myUse)
            raise ApplicationPrepareError(lbUseErr)
        if myFolder:
            chk, chkOut, chkErr = _exec_cmd('source /cvmfs/lhcb.cern.ch/lib/LbEnv && source LbLogin.sh --cmtconfig=%s && git lb-checkout %s/%s %s' % (platformToUse, myUse, myBranch, myFolder), dev_dir)
            logger.info("Running git lb-checkout %s/%s %s" % (myUse,myBranch, myFolder))
            if chk != 0:
                logger.error("git lb-checkout %s/%s %s failed!" % (myUse, myBranch, myFolder))
                raise ApplicationPrepareError(chkErr)
    return dev_dir

exportToGPI('prepare_cmake_app', prepare_cmake_app, 'Functions')

def prepareGaudiExec(myApp, myVer, myPath='$HOME/cmtuser', myUse=None, myFolder=None , myBranch = 'master'):
    """
        Setup and Return a GaudiExec based upon a release version of a given App.
        Args:
            myApp (str): This is name of the App you want to run
            myVer (str): This is the version of the app you want
            myPath (str): This is where lb-dev will be run
            myUse (str): This is a git lb-use which will be run once the lb-dev has executed
            myFolder (str): This is a git lb-checkout for after the lb-use. 
            myBranch (str) : This is what is appended to the lb-checkout myFolder/myBranch , master assumed if not specified
    """
    path = prepare_cmake_app(myApp, myVer, myPath, myUse, myFolder, myBranch)
    from GangaCore.GPI import GaudiExec
    return GaudiExec(directory=path)

exportToGPI('prepareGaudiExec', prepareGaudiExec, 'Functions')

def _exec_cmd(cmd, cwdir):
    """
        This is taken from the code which runs SetupProject
        TODO: Replace me with a correct call to execute once the return code is known to work

        Args:
            cmd (str): This is the full command which is to be executed
            cwdir (str): The folder the command is to be run in
    """
    pipe = subprocess.Popen(cmd, shell=True, env={}, cwd=cwdir,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
    stdout, stderr = pipe.communicate()
    while pipe.poll() is None:
        time.sleep(0.5)
    return pipe.returncode, stdout, stderr

