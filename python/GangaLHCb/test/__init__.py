from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
#from GangaTest.Lib.TestSubmitter import TestSubmitter
try:
    ##    from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler
    ##    from GangaLHCb.Lib.DIRAC.RootDiracRTHandler import RootDiracRTHandler
    from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler
    from GangaLHCb.Lib.RTHandlers.LHCbRootDiracRunTimeHandler import LHCbRootDiracRunTimeHandler
    loadRootHandler = True
except ImportError:
    loadRootHandler = False

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)

from Ganga.GPI import *
import os.path
DAVINCI_VERSION = None


def getDiracAppPlatform():
    return 'x86_64-slc6-gcc48-opt'

# Add Local runtimehandler for Testsubmitter


def addLocalTestSubmitter():
    allHandlers.add('DaVinci', 'TestSubmitter', LHCbGaudiRunTimeHandler)


class DiracTestSubmitter(TestSubmitter):

    def __init__(self):
        super(DiracTestSubmitter, self).__init__()
        self._impl.diracOpts = ''
        self._impl.settings = {}

# Add Dirac runtimehandler for Testsubmitter


def addDiracTestSubmitter():
    allHandlers.add('DaVinci', 'TestSubmitter', LHCbGaudiDiracRunTimeHandler)
    allHandlers.add(
        'GaudiPython', 'TestSubmitter', LHCbGaudiDiracRunTimeHandler)
    if loadRootHandler:
        allHandlers.add('Root', 'TestSubmitter', LHCbRootDiracRunTimeHandler)

# Get Current test version


def getTestDaVinciVersion():
    d = {}
    # FIXME once config option is available
    siteInstallArea = '/afs/cern.ch/sw/ganga/install/TEST'
    execfile(
        os.path.join(siteInstallArea, 'LHCb', 'software', 'CURRENT'), d, d)
    return d['DAVINCI_VERSION']


def getTestDaVinciApplication():
    # FIXME once config option is available
    siteInstallArea = '/afs/cern.ch/sw/ganga/install/TEST'
    app = DaVinci()
    app.version = getTestDaVinciVersion()
    app.user_release_area = os.path.join(
        siteInstallArea, 'LHCb', 'software', 'install_area')
    hat = 'Tutorial'
    package = 'Analysis'
    tutorialversion = 'v6r5'
    app.masterpackage = package + ' ' + tutorialversion + ' ' + hat
    app.optsfile = [os.path.join(app.user_release_area,
                                 'DaVinci_' + str(app.version),
                                 hat, package, tutorialversion,
                                 'solutions', 'DaVinci2', 'DVTutorial_2.opts')]
    return app


def checkFileExists(file):
    import os.path
    return os.path.isfile(file)


def checkFolderExists(folder):
    import os.path
    return os.path.isdir(folder)

# check for a file in a tar file


def checkFileInTar(tf, file):
    import tarfile
    tar1 = tarfile.open(tf, "r")
    logger.info("%s files in %s" % (str(len(tar1.getnames())), tf))
    for fileName in tar1.getnames():
        if str(fileName[:2]) == str('./'):
            logger.info("found file: %s" % str(fileName[2:]))
            testFileName = str(fileName[2:])
        else:
            logger.info("found file: %s" % fileName)
            testFileName = fileName
        if file == testFileName:
            return True
        else:
            continue

    # return (file in tar1.getnames())
    return False

# check for a file in the inputsandbox


def checkFileInSandbox(job, file):
    import os.path
    masterTest = False
    subJobTest = False
    tarFile2 = os.path.join(
        job.inputdir, '_input_sandbox_%d_master.tgz' % job.id)
    tarFile1 = os.path.join(job.inputdir, '_input_sandbox_%d.tgz' % job.id)

    logger.info("Checking: %s and %s" % (tarFile1, tarFile2))

    if checkFileExists(tarFile2):
        masterTest = checkFileInTar(tarFile2, file)
    elif checkFileExists(tarFile1):
        subJobTest = checkFileInTar(tarFile1, file)
    else:
        pass

    if masterTest is True:
        return True
    elif subJobTest is True:
        return True
    else:
        return False
