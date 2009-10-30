import os
import Ganga.Utility.logging
import Ganga.Utility.Config
configLHCb=Ganga.Utility.Config.makeConfig('LHCb','Parameters for LHCb')
configDirac=Ganga.Utility.Config.makeConfig('DIRAC','Parameters for DIRAC')
logger=Ganga.Utility.logging.getLogger()

# Set default values for the LHCb config section.
dscrpt = 'The name of the local site to be used for resolving LFNs into PFNs.'
configLHCb.addOption('LocalSite','',dscrpt)
dscrpt = 'The place where OutputData should go.'
configLHCb.addOption('DataOutput',os.environ['HOME'],dscrpt)
dscrpt = 'The command to used to create a directory in the locations of \
`DataOutput`'
configLHCb.addOption('mkdir_cmd','/bin/mkdir',dscrpt)
dscrpt = 'The command used to copy out data to the `DataOutput` locations'
configLHCb.addOption('cp_cmd','/bin/cp',dscrpt)
dscrpt = 'Files from these services will go to the output sandbox (unless \
overridden by the user in a specific job via the Job.outputdata field). Files \
from all other known handlers will go to output data (unless overridden by \
the user in a specific job via the Job.outputsandbox field).'
configLHCb.addOption('outputsandbox_types',
                     ['NTupleSvc','HistogramPersistencySvc',
                      'MicroDSTStream','EvtTupleSvc'],dscrpt)
dscrpt = 'The string that is added after the filename in the options to tell' \
         ' Gaudi how to read the data. This is the default value used if the '\
         'file name does not match any of the patterns in '\
         'datatype_string_patterns.'
configLHCb.addOption('datatype_string_default',
                     """TYP='POOL_ROOTTREE' OPT='READ'""",dscrpt)
dscrpt = 'If a file matches one of these patterns, then the string here '\
         'overrides the datatype_string_default value.'
defval = {"SVC='LHCb::MDFSelector'" : ['*.raw','*.RAW','*.mdf','*.MDF']}
configLHCb.addOption('datatype_string_patterns',defval,dscrpt)
 
# Set default values for the Dirac section.
dscrpt = 'Valid Root versions used by Dirac.'
versions = ['5.22.00a','5.14.00f','5.14.00h', '5.14.00i','5.18.00', '5.18.00a',
            '4.04.02','3.10.02']
configDirac.addOption('RootVersions',versions,dscrpt)
configDirac.addOption('AllowedPlatforms',['slc4_ia32_gcc34'],
                      'Allowed platforms for submission to DIRAC')
dscrpt = 'Display DIRAC API stdout to the screen in Ganga?'
configDirac.addOption('ShowDIRACstdout',False,dscrpt)

    
def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return

def loadPlugins( config = {} ):
    #import Lib.Dirac
    import Lib.Gaudi
    import Lib.LHCbDataset
    import Lib.DIRAC

#from Ganga.GPIDev.Credentials import getCredential
#proxy = getCredential('GridProxy', '')

