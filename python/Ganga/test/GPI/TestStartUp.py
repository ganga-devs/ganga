from __future__ import absolute_import
import os
import inspect
import sys
import shutil
import glob
from tempfile import mkdtemp

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

this_dir = mkdtemp()

def testStartUp():
    """ Lets test the startup of Ganga mimicking first launch """
    # Process options given at command line and in configuration file(s)
    # Perform environment setup and bootstrap

    from Ganga.Runtime import setupGanga
    argv = ['ganga', '--no-mon', '-o[Configuration]gangadir=%s' % this_dir, '-o[Configuration]RUNTIME_PATH=GangaTest']
    setupGanga(argv=argv, interactive=False)

    for this_file in ['.gangarc', '.ganga.log']:
        assert os.path.isfile(os.path.join(homeDir, this_file))

    # No way known to mimic IPython starting up in a simple way
    #assert os.path.isdir(os.path.join(homeDir, '.ipython-ganga'))

    for this_folder in ['repository',]:
        assert os.path.isdir(os.path.join(this_dir, this_folder))

    from Ganga.GPI import Job

    j=Job()
    j.submit()

    for this_folder in ['shared', 'workspace']:
        assert os.path.isdir(os.path.join(this_dir, this_folder))

def testShutdown():
    """ Lets just call the shutdown here for safety """

    from Ganga.testlib.GangaUnitTest import stop_ganga

    stop_ganga()

    shutil.rmtree(this_dir, ignore_errors=True)

