#Bail out when loading this module as it is not python3 compliant
from GangaCore.Core.exceptions import PluginError
raise PluginError("The GangaPanda module has not been upgraded for python 3. The last python 2 Ganga version is 7.1.15 . Please contact the ganga devs to discuss updating this module.")

## Config options
from GangaCore.Utility.Config import makeConfig
from GangaCore.Utility.logging import getLogger
logger = getLogger()

# -------------------------------------------------
# Panda Options
config = makeConfig('Panda','Panda backend configuration parameters')
config.addOption( 'prodSourceLabelBuild', 'panda', 'prodSourceLabelBuild')
config.addOption( 'prodSourceLabelRun', 'user', 'prodSourceLabelRun')
config.addOption( 'assignedPriorityBuild', 2000, 'assignedPriorityBuild' )
config.addOption( 'assignedPriorityRun', 1000, 'assignedPriorityRun' )
config.addOption( 'processingType', 'ganga', 'processingType' )
config.addOption( 'specialHandling', '', 'specialHandling - Expert only.' )
config.addOption( 'enableDownloadLogs', False , 'enableDownloadLogs' )
config.addOption( 'trustIS', True , 'Trust the Information System' )
config.addOption( 'serverMaxJobs', 5000 , 'Maximum number of subjobs to send to the Panda server' )
config.addOption( 'chirpconfig', '' , 'Configuration string for chirp data output, e.g. "chirp^etpgrid01.garching.physik.uni-muenchen.de^/tanyasandoval^-d chirp" ' )
config.addOption( 'chirpserver', '' , 'Configuration string for the chirp server, e.g. "voatlas92.cern.ch". If this variable is set config.Panda.chirpconfig is filled and chirp output will be enabled.' )
config.addOption( 'siteType', 'analysis' , 'Expert only.' )
config.addOption( 'baseURL', '' , 'Expert only.' )
config.addOption( 'baseURLSSL', '' , 'Expert only.' )
config.addOption( 'AllowDirectSubmission', False, 'Deprecated. Please use the Jedi backend' )
config.addOption('AGISJSONFile', '/tmp/agis_pandaresources.json', 'Local AGIS JSON file for queue -> site mapping')

# -------------------------------------------------
# Jedi Options
config = makeConfig('Jedi','Jedi backend configuration parameters')
config.addOption( 'prodSourceLabelBuild', 'panda', 'prodSourceLabelBuild')
config.addOption( 'prodSourceLabelRun', 'user', 'prodSourceLabelRun')
config.addOption( 'assignedPriorityBuild', 2000, 'assignedPriorityBuild' )
config.addOption( 'assignedPriorityRun', 1000, 'assignedPriorityRun' )
config.addOption( 'processingType', 'ganga', 'processingType' )
config.addOption( 'enableDownloadLogs', False , 'enableDownloadLogs' )
config.addOption( 'trustIS', True , 'Trust the Information System' )
config.addOption( 'serverMaxJobs', 5000 , 'Maximum number of subjobs to send to the Panda server' )
config.addOption( 'chirpconfig', '' , 'Configuration string for chirp data output, e.g. "chirp^etpgrid01.garching.physik.uni-muenchen.de^/tanyasandoval^-d chirp" ' )
config.addOption( 'chirpserver', '' , 'Configuration string for the chirp server, e.g. "voatlas92.cern.ch". If this variable is set config.Panda.chirpconfig is filled and chirp output will be enabled.' )
config.addOption( 'siteType', 'analysis' , 'Expert only.' )



def standardSetup():
    import PACKAGE
    PACKAGE.standardSetup()

    
def loadPlugins(c):

    import sys
    from GangaCore.Utility.logging import getLogger

    try:
        import Lib.Panda
        import Lib.Jedi
    except SystemExit:
        from GangaCore.Core.exceptions import ApplicationConfigurationError
        import commands
        (s,o) = commands.getstatusoutput('curl --version')
        if (s):
            raise ApplicationConfigurationError("Couldn't load Panda Client: ensure 'curl' is available")
        else:
            raise ApplicationConfigurationError("Couldn't load Panda Client")
    import Lib.Athena
    import Lib.Executable
    import Lib.ProdTrans

    return None

