import os
import Ganga.Utility.Config
from multiprocessing import cpu_count
configDirac=Ganga.Utility.Config.makeConfig('DIRAC','Parameters for DIRAC')
 
# Set default values for the Dirac section.
configDirac.addOption('ShowDIRACstdout', False,
                      'Display DIRAC API stdout to the screen in Ganga?')

configDirac.addOption('Timeout', 1000,
                      'Default timeout (seconds) for Dirac commands')

configDirac.addOption('StartUpWaitTime', 1.0,
                      'Wait time (seconds) prior to first poll of Dirac child proc')

configDirac.addOption('NumWorkerThreads', cpu_count(),
                      'Number of worker threads that the local DIRAC server and client should establish')

configDirac.addOption('EndDataString', '###END-DATA-TRANS###',
                      'String that indicates end of data transmit')

configDirac.addOption('ServerShutdownString', '###SERVER-SHUTDOWN###',
                      'String that tells local DIRAC server to shutdown')

configDirac.addOption('ServerPortMin', 49000,
                      'The minimum port number that the local DIRAC server should try listening on')

configDirac.addOption('ServerPortMax', 65000,
                      'The maximum port number that the local DIRAC server should try listening on')

configDirac.addOption('DiracEnvFile', os.environ["GANGADIRACENVIRONMENT"],
                      'Ganga environment file for DIRAC environment (do not change unless you are sure you know what you are doing).')

configDirac.addOption('DiracCommandFiles', set([]),
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

def getEnvironment( config = {} ):
   import sys
   import os.path
   import PACKAGE

   PACKAGE.standardSetup()
   return

def loadPlugins( config = {} ):
    import Lib.Server
    import Lib.Backends
    import Lib.RTHandlers

#from Ganga.GPIDev.Credentials import getCredential
#proxy = getCredential('GridProxy', '')

