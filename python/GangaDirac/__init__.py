import os
#from multiprocessing     import cpu_count
from Ganga.Utility.Config import makeConfig, getConfig
configDirac = makeConfig('DIRAC','Parameters for DIRAC')
config      = getConfig('Configuration')
 
# Set default values for the Dirac section.
#configDirac.addOption('ShowDIRACstdout', False,
#                      'Display DIRAC API stdout to the screen in Ganga?')

configDirac.addOption('Timeout', 1000,
                      'Default timeout (seconds) for Dirac commands')

configDirac.addOption('splitFilesChunks', 5000,
                      'when splitting datasets, pre split into chunks of this int')
#configDirac.addOption('NumWorkerThreads', cpu_count(),
configDirac.addOption('NumWorkerThreads', 5,
                      'Number of worker threads that the local DIRAC server and client should establish')

configDirac.addOption('DiracEnvFile', os.environ["GANGADIRACENVIRONMENT"],
                      'Ganga environment file for DIRAC environment (do not change unless you are sure you know what you are doing).')

configDirac.addOption('DiracCommandFiles', [os.path.join(os.path.dirname(__file__),'Lib/Server/DiracCommands.py')],
                      'The file containing the python commands that the local DIRAC server can execute. The default DiracCommands.py is added automatically')

configDirac.addOption('DiracOutputDataSE', [],
                      'List of SEs where Dirac ouput data should be placed (empty means let DIRAC decide where to put the data).') 

configDirac.addOption('noInputDataBannedSites',
                      ['LCG.CERN.ch','LCG.CNAF.it','LCG.GRIDKA.de','LCG.IN2P3.fr','LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.SARA.nl'],
                      'List of sites to ban when a user job has no input data (this is meant to reduce the load on these sites)')

configDirac.addOption('MaxDiracBulkJobs', 500,
                      'The Maximum allowed number of bulk submitted jobs before Ganga intervenes')

configDirac.addOption('failed_sandbox_download', True,
                      'Automatically download sandbox for failed jobs?')

configDirac.addOption('load_default_Dirac_backend', True,
                      'Whether or not to load the default dirac backend. This allows packages to load a modified version if necessary')

configDirac.addOption('DiracLFNBase','/lhcb/user/%s/%s'%(config['user'][0],
                                                         config['user']),
                      "Base dir appended to create LFN name from DiracFile('name')")

configDirac.addOption('ReplicateOutputData',True, 'Determines whether outputdata stored on Dirac is replicated')

configDirac.addOption('DiracSpaceTokens',
                      ['CERN-USER','CNAF-USER','GRIDKA-USER','IN2P3-USER','SARA-USER',
                       'PIC-USER','RAL-USER'],
                      'Space tokens allowed for replication, etc.')

def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return

def loadPlugins( config = {} ):
    import Lib.Backends
    import Lib.RTHandlers
    import Lib.Files

