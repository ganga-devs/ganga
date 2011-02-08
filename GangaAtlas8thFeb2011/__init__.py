# File: GangaAtlas/__init__.py

def loadPlugins( config = {} ):

   import warnings
   warnings.filterwarnings('ignore','Python C API version mismatch for module pycurl')
   warnings.filterwarnings('ignore','Python C API version mismatch for module _lfc')

   import Lib.Athena
   import Lib.ATLASDataset
   import Lib.AthenaMC
   import Lib.Tnt
   import Lib.AtlasLCGRequirements
   import Lib.Tasks
   import Lib.TagPrepare
   import Lib.AMIGA
   
   return None

def getEnvironment(c):
    import PACKAGE
    PACKAGE.standardSetup()
    
    #   set up X509_CERT_DIR for DQ2
    from Ganga.Utility.GridShell import getShell
    gshell = getShell()
    if gshell:
       try:
          return { 'X509_CERT_DIR' : gshell.env['X509_CERT_DIR'] }
       except KeyError:
          return { 'X509_CERT_DIR' : '/etc/grid-security/certificates' }


# some checks to make sure that new Dashboard MSG service is enabled in the user's configuration

def postBootstrapHook():
    from Ganga.Utility.logging import getLogger

    logger = getLogger()

    from Ganga.Utility.Config import getConfig
    cfg = getConfig('MonitoringServices')

    MONITORING_DEFAULT = "Ganga.Lib.MonitoringServices.Dashboard.LCGAthenaMS.LCGAthenaMS"

    for name in cfg.options:
        value = cfg[name]
        if 'Athena' in name.split('/') and ('LCG' in name.split('/') or 'CREAM' in name.split('/')):
            if not MONITORING_DEFAULT in value.split(','):
             logger.error('''*** Outdated monitoring configuration - check your configuration files ***

*** Outdated monitoring configuration - check your configuration files ***

Maybe your ~/.gangarc contains old entries which override new defaults?
You may also check the configuration files defined by $GANGA_CONFIG_PATH or $GANGA_CONFIG environment variables.

To fix this problem simply remove (or comment out) the following lines in [MonitoringServices] section:
Athena/LCG=...
Athena/CREAM=...

For now I will add the correct default settings (%s) to the configuration of this Ganga session.
Note that in the future you won't be able to start Ganga until these issues are corrected manually.
'''%MONITORING_DEFAULT)

             cfg.setUserValue(name,value+','+MONITORING_DEFAULT)


