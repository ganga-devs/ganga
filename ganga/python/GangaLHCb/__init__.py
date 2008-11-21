import os
import Ganga.Utility.logging
import Ganga.Utility.Config
configLHCb=Ganga.Utility.Config.makeConfig('LHCb','Parameters for LHCb')
configDirac=Ganga.Utility.Config.makeConfig('DIRAC','Parameters for DIRAC')
logger=Ganga.Utility.logging.getLogger()

# Set default values for the LHCb config section.
dscrpt = 'The location of the DIRAC client installation that Ganga should use.'
configLHCb.addOption('DiracTopDir',
                     '/afs/cern.ch/lhcb/software/releases/DIRAC/DIRAC_v2r19', 
                     dscrpt)
dscrpt = 'The age in minutes that a cache of site locations for LFNs in an \
LHCbDataset stays valid.'
configLHCb.addOption('maximum_cache_age',10080,dscrpt)
dscrpt = 'The name of the local site to be used for resolving LFNs into PFNs.'
configLHCb.addOption('LocalSite','',dscrpt)
dscrpt = 'The protocol to be used for reading PFNs (rfio, castor etc.)'
configLHCb.addOption('SEProtocol','',dscrpt)
dscrpt = 'The place where OutputData should go.'
configLHCb.addOption('DataOutput',os.environ['HOME'],dscrpt)
dscrpt = 'The command to used to create a directory in the locations of \
`DataOutput`'
configLHCb.addOption('mkdir_cmd','/bin/mkdir',dscrpt)
dscrpt = 'The command used to copy out data to the `DataOutput` locations'
configLHCb.addOption('cp_cmd','/bin/cp',dscrpt)
dscrpt = 'Root versions used by Dirac for given versions of DaVinci'
versions = {'5.14.00f': 'v19r5', '5.14.00h': 'v19r8', '5.14.00i': 'v19r9',
            '5.18.00': 'v19r10', '5.18.00a': 'v19r11', '4.04.02': 'v14r5',
            '3.10.02': 'v12r18'}
dscrpt = 'Files from these services will go to the output sandbox (unless \
overridden by the user in a specific job via the Job.outputdata field). Files \
from all other known handlers will go to output data (unless overridden by \
the user in\n# a specific job via the Job.outputsandbox field).'
configLHCb.addOption('outputsandbox_types',
                     ['NTupleSvc','HistogramPersistencySvc','MicroDSTStream'],
                     dscrpt)

# Set default values for the Dirac section.
configDirac.addOption('RootVersions',versions,dscrpt)
dscrpt = 'extra lifetime required for a proxy to submit a job'
configDirac.addOption('extraProxytime','600',dscrpt)
configDirac.addOption('DIRACsite',None,'Used for testing only')
dcsrpt = 'The logging level of DIRAC, not the logging level of the Dirac \
plugin in Ganga'
configDirac.addOption('DiracLoggerLevel','ERROR',dscrpt)
configDirac.addOption('AllowedPlatforms',['slc4_ia32_gcc34'],
                      'Allowed platforms for submission to DIRAC')

    
def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return

def loadPlugins( config = {} ):
    import Lib.Dirac
    import Lib.Gaudi
    import Lib.LHCbDataset

