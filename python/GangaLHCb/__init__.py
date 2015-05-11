import os
import Ganga.Utility.logging
import Ganga.Utility.Config
configLHCb=Ganga.Utility.Config.makeConfig('LHCb','Parameters for LHCb')
#configDirac=Ganga.Utility.Config.makeConfig('DIRAC','Parameters for DIRAC')
logger=Ganga.Utility.logging.getLogger()
#config=Ganga.Utility.Config.getConfig('Configuration')

# Set default values for the LHCb config section.
dscrpt = 'The name of the local site to be used for resolving LFNs into PFNs.'
configLHCb.addOption('LocalSite','',dscrpt)
#dscrpt = 'The place where OutputData should go.'
#configLHCb.addOption('DataOutput',os.environ['HOME'],dscrpt)
#dscrpt = 'The command to used to create a directory in the locations of \
#`DataOutput`'
#configLHCb.addOption('mkdir_cmd','/bin/mkdir',dscrpt)
#dscrpt = 'The command used to copy out data to the `DataOutput` locations'
#configLHCb.addOption('cp_cmd','/bin/cp',dscrpt)
dscrpt = 'Files from these services will go to the output sandbox (unless \
overridden by the user in a specific job via the Job.outputdata field). Files \
from all other known handlers will go to output data (unless overridden by \
the user in a specific job via the Job.outputsandbox field).'
configLHCb.addOption('outputsandbox_types',
                     ['CounterSummarySvc','NTupleSvc',
                      'HistogramPersistencySvc','MicroDSTStream',
                      'EvtTupleSvc'],dscrpt)
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
configLHCb.addOption('UserAddedApplications',"","List of user added LHCb applications split by ':'")

## dscrpt = 'Automatically download sandbox for failed jobs?'
## configLHCb.addOption('failed_sandbox_download',True,dscrpt)
## dscrpt = 'List of SEs where Dirac ouput data should be placed (empty means '\
##          'let DIRAC decide where to put the data).'
## configLHCb.addOption('DiracOutputDataSE',[],dscrpt)
## dscrpt = 'List of sites to ban when a user job has no input data (this is '\
##          'meant to reduce the load on these sites)'
## sites = ['LCG.CERN.ch','LCG.CNAF.it','LCG.GRIDKA.de','LCG.IN2P3.fr',
##          'LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.SARA.nl']
## configLHCb.addOption('noInputDataBannedSites',sites,dscrpt)
## tokens = ['CERN-USER','CNAF-USER','GRIDKA-USER','IN2P3-USER','SARA-USER',
##           'PIC-USER','RAL-USER']
## dscrpt = 'Space tokens allowed for replication, etc.'
## configLHCb.addOption('DiracSpaceTokens',tokens,dscrpt)
#dscrpt = 'Switch whether or not a check that the required app version/platform is valid for the backend'
#configLHCb.addOption('ignore_version_check',True,dscrpt)
#dscrpt = 'The Maximum allowed number of bulk submitted jobs before Ganga intervenes'
#configLHCb.addOption('MaxDiracBulkJobs',500,dscrpt)
 
# Set default values for the Dirac section.
#dscrpt = 'Display DIRAC API stdout to the screen in Ganga?'
#configDirac.addOption('ShowDIRACstdout',False,dscrpt)
#dscrpt = 'Global timeout (seconds) for Dirac commands'
#configDirac.addOption('Timeout',1000,dscrpt)
#dscrpt = 'Wait time (seconds) prior to first poll of Dirac child proc'
#configDirac.addOption('StartUpWaitTime',3,dscrpt)
## dscrpt = 'Base dir appended to create LFN name from DiracFile(\'name\')'
## configDirac.addOption('DiracLFNBase','/lhcb/user/%s/%s'%(config['user'][0],config['user']),dscrpt)

#This is now done for all plugin packages automatically by the bootstrap
#but leave here to remind how to set up another registry if needed.
#from Ganga.GPIDev.Lib.Registry.RegistryUtils import establishNamedTemplates
#establishNamedTemplates('templatesLHCb', os.path.join(os.path.dirname(__file__),'templates'), file_ext='tpl', pickle_files=True)

def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return

def loadPlugins( config = {} ):
    import Lib.Backends
    import Lib.Applications
    import Lib.LHCbDataset
    import Lib.Mergers
    import Lib.RTHandlers
    import Lib.Splitters
    import Lib.Tasks
    import Lib.Files

#from Ganga.GPIDev.Credentials import getCredential
#proxy = getCredential('GridProxy', '')

## This is being dropped from 6.1.0 due to causing some bus in loading large numbers of jobs
#
#from Ganga.GPIDev.Lib.Registry.JobRegistry import config as display_config
#display_config.overrideDefaultValue( 'jobs_columns', ('fqid', 'status', 'name', 'subjobs', 'application', 'backend', 'backend.actualCE', 'backend.extraInfo', 'comment') )
#display_config.overrideDefaultValue( 'jobs_columns_functions', {'comment': 'lambda j: j.comment', 'backend.extraInfo': 'lambda j : j.backend.extraInfo ', 'subjobs': 'lambda j: len(j.subjobs)', 'backend.actualCE': 'lambda j:j.backend.actualCE', 'application': 'lambda j: j.application._name', 'backend': 'lambda j:j.backend._name'} )
#display_config.overrideDefaultValue('jobs_columns_width', {'fqid': 8, 'status': 10, 'name': 10, 'application': 15, 'backend.extraInfo': 30, 'subjobs': 8, 'backend.actualCE': 17, 'comment': 20, 'backend': 15} )
