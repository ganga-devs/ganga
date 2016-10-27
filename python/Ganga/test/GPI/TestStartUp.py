from __future__ import absolute_import
import os
import inspect
import sys
import shutil
import glob


# First clear away any configurations and temp files which may not be present on first launch
homeDir = os.path.expanduser("~")
if os.path.exists(os.path.join(homeDir, '.gangarc')):
    os.unlink(os.path.join(homeDir, '.gangarc'))
for logFile in glob.glob(os.path.join(homeDir, '.ganga.log*')):
    os.unlink(logFile)
shutil.rmtree(os.path.join(homeDir, '.ipython-ganga'), ignore_errors=True)
shutil.rmtree(os.path.join(homeDir, '.gangarc_backups'), ignore_errors=True)

def standardSetup():
    """Function to perform standard setup for Ganga.
    """

    gangaDir = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), '../../')

    sys.path.insert(0, gangaDir)

    binDir = os.path.join(gangaDir, '../bin')

    sys.path.insert(1, binDir)

    from Ganga.PACKAGE import standardSetup
    standardSetup()

standardSetup()
del standardSetup

def testStartUp():
    """ Lets test the startup of Ganga mimicking first launch """
    # Process options given at command line and in configuration file(s)
    # Perform environment setup and bootstrap

    from Ganga.Runtime import setupGanga
    setupGanga(argv=['ganga', '--no-mon'], interactive=False)

def testShutdown():
    """ Lets just call the shutdown here for safety """
    from Ganga.Core.InternalServices import ShutdownManager
    ShutdownManager._ganga_run_exitfuncs()

