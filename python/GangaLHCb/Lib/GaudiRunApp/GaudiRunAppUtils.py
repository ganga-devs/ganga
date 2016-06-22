import subprocess
import time
from Ganga.Core.exceptions import GangaException
from Ganga.Runtime.GPIexport import exportToGPI
from Ganga.Utility.logging import getLogger
from Ganga.Core.exceptions import ApplicationConfigurationError
from .PythonOptsCmakeParser import PythonOptsCmakeParser

logger = getLogger()

def getGaudiRunInputData(optsfiles, app):
    '''Returns a LHCbDataSet object from a list of options files. The
       optional argument extraopts will decide if the extraopts string inside
       the application is considered or not.

    Usage example:
    # Get the data from an options file and assign it to the jobs inputdata field
    j.inputdata = getGaudiRunInputData([\"~/cmtuser/DaVinci_v22r0p2/Tutorial/Analysis/options/Bs2JpsiPhi2008.py\"], j.application)

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
        raise ApplicationConfigurationError(None, msg)

    return parser.get_input_data()

exportToGPI('getGaudiRunInputData', getGaudiRunInputData, 'Functions')

def prepare_cmake_app(myApp, myVer, myPath='$HOME/cmtuser', myGetpack=None):
    """
        Short helper function for setting up minimal application environments on disk for job submission
        Args:
            myApp (str): This is the name of the app to pass to lb-dev
            myVer (str): This is the version of 'myApp' to pass tp lb-dev
            myPath (str): This is where lb-dev will be run
            myGepPack (str): This is a getpack which will be run once the lb-dev has executed
    """
    full_path = expandfilename(myPath, True)
    if not path.exists(full_path):
        makedirs(full_path)
        chdir(full_path)
    _exec_cmd('lb-dev %s %s' % (myApp, myVer), full_path)
    logger.info("Set up App Env at: %s" % full_path)
    if myGetpack:
        dev_dir = path.join(full_path, myApp + 'Dev_' + myVer)
        _exec_cmd('getpack %s' % myGetpack, dev_dir)
        logger.info("Ran Getpack: %s" % myGetpack)

exportToGPI('prepare_cmake_app', prepare_cmake_app, 'Functions')

def _exec_cmd(cmd, cwdir):
    """
        This is taken from the code which runs SetupProject
        TODO: Replace me with a correct call to execute once the return code is known to work

        Args:
            cmd (str): This is the full command which is to be executed
            cwdir (str): The folder the command is to be run in
    """
    pipe = subprocess.Popen(cmd, shell=True, env=None, cwd=cwdir,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    while pipe.poll() is None:
        time.sleep(0.5)
    return pipe.returncode, stdout, stderr

