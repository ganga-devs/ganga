from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
try:
##    from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler
##    from GangaLHCb.Lib.DIRAC.RootDiracRTHandler import RootDiracRTHandler
    from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler
    from GangaLHCb.Lib.RTHandlers.LHCbRootDiracRunTimeHandler import LHCbRootDiracRunTimeHandler
    loadRootHandler=True
except ImportError:
    loadRootHandler=False

from Ganga.GPI import *
import os.path
DAVINCI_VERSION=None

def getDiracAppPlatform():
    return 'x86_64-slc5-gcc43-opt'

# Add Local runtimehandler for Testsubmitter
def addLocalTestSubmitter():
    allHandlers.add('DaVinci', 'TestSubmitter', LHCbGaudiRunTimeHandler)

class DiracTestSubmitter(TestSubmitter):
    def __init__(self):
        super(DiracTestSubmitter,self).__init__()
        self._impl.diracOpts=''
        self._impl.settings={}

# Add Dirac runtimehandler for Testsubmitter
def addDiracTestSubmitter():
    allHandlers.add('DaVinci', 'TestSubmitter', LHCbGaudiDiracRunTimeHandler)
    allHandlers.add('GaudiPython', 'TestSubmitter', LHCbGaudiDiracRunTimeHandler)
    if loadRootHandler:
        allHandlers.add('Root', 'TestSubmitter', LHCbRootDiracRunTimeHandler)

# Get Current test version
def getTestDaVinciVersion():
    d={}
    siteInstallArea='/afs/cern.ch/sw/ganga/install/TEST' #FIXME once config option is available
    execfile(os.path.join(siteInstallArea,'LHCb','software','CURRENT'),d,d)
    return d['DAVINCI_VERSION']

def getTestDaVinciApplication():
    siteInstallArea='/afs/cern.ch/sw/ganga/install/TEST' #FIXME once config option is available
    app=DaVinci()
    app.version=getTestDaVinciVersion()
    app.user_release_area=os.path.join(siteInstallArea,'LHCb','software','install_area')
    hat = 'Tutorial'
    package='Analysis'
    tutorialversion = 'v6r5'
    app.masterpackage=package+' '+tutorialversion+' '+hat
    app.optsfile=[os.path.join(app.user_release_area,
                              'DaVinci_'+str(app.version),
                              hat,package,tutorialversion,
                              'solutions','DaVinci2','DVTutorial_2.opts')]
    return app

# check for a file in a tar file
def checkFileInTar(tf,file):
    import tarfile
    tar1=tarfile.open(tf,"r")
    return (file in tar1.getnames())

# check for a file in the inputsandbox
def checkFileInSandbox(job,file):
    import os.path
    tarFile2=os.path.join(job.inputdir,'_input_sandbox_%d_master.tgz'%job.id)
    tarFile1=os.path.join(job.inputdir,'_input_sandbox_%d.tgz'%job.id)
    return (checkFileInTar(tarFile1,file) or checkFileInTar(tarFile2,file))
