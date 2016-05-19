# File: GangaAtlas/__init__.py

## Config options
from Ganga.Utility.Config import makeConfig, ConfigError, getConfig
import os

from Ganga.Utility.Config.Config import _after_bootstrap
from Ganga.Utility.logging import getLogger
logger = getLogger()

if not _after_bootstrap:

    # -------------------------------------------------
    # Athena Options
    config = makeConfig('Athena','Athena configuration parameters')
    config.addOption('LCGOutputLocation', 'srm://srm-atlas.cern.ch/castor/cern.ch/grid/atlas/scratch/%s/ganga' % os.environ['USER'], 'FIXME')
    config.addOption('LocalOutputLocation', '/castor/cern.ch/atlas/scratch/%s/ganga' % os.environ['USER'], 'FIXME')
    config.addOption('IndividualSubjobDirsForLocalOutput', False, 'When copying local output, should dir structure be jid.sid (False) or jid/sid (True)' )
    config.addOption('SingleDirForLocalOutput', False, 'When copying local output, only a single dirs used for output and output filenames are changed with jid.sid' )
    config.addOption('NoSubDirsAtAllForLocalOutput', False, 'When copying local output, all output is copied to the given output location with no subdirs created' )
    config.addOption('ATLAS_SOFTWARE', '/afs/cern.ch/project/gd/apps/atlas/slc3/software', 'FIXME')
    config.addOption('PRODUCTION_ARCHIVE_BASEURL', 'http://atlas-computing.web.cern.ch/atlas-computing/links/kitsDirectory/Production/kits/', 'FIXME')
    config.addOption('ExcludedSites', '' , 'FIXME')
    config.addOption('CMTHOME', os.path.join(os.environ['HOME'],'cmthome') , 'The path in which the cmtsetup magic function will look up the setup.sh for CMT environment setup')
    config.addOption('CMTCONFIG', 'i686-slc5-gcc43-opt', 'Default value to be used as CMTCONFIG environment setup value (LCG/Batch backend)')
    config.addOption('CMTCONFIG_LIST', [ 'i686-slc4-gcc34-opt', 'i686-slc5-gcc43-opt' ], 'Allowed values for CMTCONFIG environment setup value (LCG/Batch backend)')
    config.addOption('MaxJobsAthenaSplitterJobLCG', 1000 , 'Number of maximum jobs allowed for job splitting with the AthenaSplitterJob and the LCG backend')
    config.addOption('DCACHE_RA_BUFFER', 32768 , 'Size of the dCache read ahead buffer used for dcap input file reading')
    config.addOption('ENABLE_DQ2COPY', False , 'Enable DQ2_COPY input workflow on LCG backend')
    config.addOption('ENABLE_SGE_DQ2JOBSPLITTER', False , 'Enable DQ2JobSplitter for SGE backend')
    config.addOption('ENABLE_SGE_FILESTAGER', False , 'Enable FILE_STAGER input access mode for SGE backend')
    config.addOption('EXE_MAXFILESIZE', 1024*1024 , 'Athena.exetype=EXE jobs: Maximum size of files to be sent to WNs (default 1024*1024B)')
    config.addOption('dereferenceSymLinks', False , 'Set to True to dereference symlinks in the sources area. E.g. if src is a symlink, the target will be copied into the sources archive.')
    config.addOption('MaxJobsDQ2JobSplitter', 5000, 'Maximum number of allowed subjobs of DQ2JobSplitter')
    config.addOption('MaxFilesPandaDQ2JobSplitter', 5000, 'Maximum number of allowed subjobs of DQ2JobSplitter')
    config.addOption('MaxJobsDQ2JobSplitterLCGCompile', 500, 'Maximum number of allowed subjobs of DQ2JobSplitter on LCG/Cream with compile option switched on')
    config.addOption('MaxFileSizeNGDQ2JobSplitter', 14336, 'Maximum total sum of filesizes per subjob of DQ2JobSplitter at the NG backend (in MB)')
    config.addOption('MaxFileSizePandaDQ2JobSplitter', 13336, 'Maximum total sum of filesizes per subjob of DQ2JobSplitter at the Panda backend (in MB)')
    config.addOption('DefaultNumFilesPandaDirectDQ2JobSplitter', 50, 'Default number of input files per subjob used at a direct access site in Panda in DQ2JobSplitter')
    config.addOption('AllowedSitesNGDQ2JobSplitter', [ 'NDGF-T1_DATADISK', 'NDGF-T1_MCDISK', 'NDGF-T1_PRODDISK', 'NDGF-T1_SCRATCHDISK' ], 'Allowed space tokens/sites for DQ2JobSplitter on NG backend' )
    config.addOption('AnyCloudPreferenceList', [ ], 'List of clouds that should be preferentially submitted to when using the anyCloud option' )
    config.addOption('ATLASOutputDatasetLFC', 'prod-lfc-atlas-local.cern.ch', 'FIXME')
    config.addOption('PathToEOSBinary', '/afs/cern.ch/project/eos/installation/pro/bin/eos.select', 'Path to the EOS binary for output copying/checking')
    config.addOption('RemoveTempUserAreaAfterPrepare', True, 'If True, user areas created in /tmp are removed after prepare has been called (the version in the gangadir shared area will be used instead)')

    # -------------------------------------------------
    # AthenaMC Options
    config = makeConfig('AthenaMC', 'AthenaMC configuration options')

    # -------------------------------------------------
    # DQ2 Options
    config = makeConfig('DQ2', 'DQ2 configuration options')

    try:
        config.addOption('DQ2_URL_SERVER', os.environ['DQ2_URL_SERVER'], 'FIXME')
    except KeyError:
        config.addOption('DQ2_URL_SERVER', 'http://atlddmcat.cern.ch/dq2/', 'FIXME')
    try:
        config.addOption('DQ2_URL_SERVER_SSL', os.environ['DQ2_URL_SERVER_SSL'], 'FIXME')
    except KeyError:
        config.addOption('DQ2_URL_SERVER_SSL', 'https://atlddmcat.cern.ch:443/dq2/', 'FIXME')

    try:
        config.addOption('DQ2_LOCAL_SITE_ID', os.environ['DQ2_LOCAL_SITE_ID'], 'Sets the DQ2 local site id')
    except KeyError:
        config.addOption('DQ2_LOCAL_SITE_ID', 'CERN-PROD_DATADISK', 'Sets the DQ2 local site id')

    config.addOption('DQ2_OUTPUT_SPACE_TOKENS', [ 'ATLASSCRATCHDISK', 'ATLASLOCALGROUPDISK', 'T2ATLASSCRATCHDISK', 'T2ATLASLOCALGROUPDISK' ] , 'Allowed space tokens names of DQ2OutputDataset output' )

    config.addOption('DQ2_BACKUP_OUTPUT_LOCATIONS', [ 'CERN-PROD_SCRATCHDISK', 'CERN-PROD_USERTAPE', 'FZK-LCG2_SCRATCHDISK', 'IN2P3-CC_SCRATCHDISK', 'TRIUMF-LCG2_SCRATCHDISK', 'IFAE_SCRATCHDISK', 'NIKHEF-ELPROD_SCRATCHDISK' ], 'Default backup locations of DQ2OutputDataset output' )

    config.addOption('USE_STAGEOUT_SUBSCRIPTION', False, 'Allow DQ2 subscription to aggregate DQ2OutputDataset output on a storage element instead of using remote lcg-cr' )

    config.addOption('usertag','user','user tag for a given data taking period')

    config.addOption('USE_ACCESS_INFO', False, 'Use automatic best choice of input dataset access mode provided by AtlasLCGRequirements.')

    config.addOption('CHECK_OUTPUT_DUPLICATES', False, 'Check for duplicate files in DQ2OutputDataset in LCG backend - this could possibly happen by ShallowRetry of glite WMS. A duplicates dataset is created')
    config.addOption('DELETE_DUPLICATES_DATASET', False, 'If CHECK_OUTPUT_DUPLICATES=True is used, duplicates dataset can be automatically deleted by setting this flag to True.')

    config.addOption('USE_NICKNAME_DQ2OUTPUTDATASET', True, 'Use voms nicknames for DQ2OutputDataset.')
    config.addOption('ALLOW_MISSING_NICKNAME_DQ2OUTPUTDATASET', False, 'Allow that voms nickname is empty for DQ2OutputDataset name creating.')

    config.addOption('OUTPUTDATASET_LIFETIME', '', 'Maximum lifetime of a DQ2OutputDataset.')
    config.addOption('OUTPUTDATASET_NAMELENGTH', 131, 'Maximum characters of a DQ2OutputDataset.')
    config.addOption('OUTPUTFILE_NAMELENGTH', 150, 'Maximum characters of a filename in DQ2OutputDataset.')
    config.addOption('NumberOfDQ2DownloadThreads', 5, 'Number of simultaneous DQ2 downloads when calling "retrieve"')

    config.addOption('setupScript', '/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/user/gangaDDMSetup.sh', 'Script to setup DQ2Clients software')

    # -------------------------------------------------
    # AMI Dataset Options
    config = makeConfig('AMIDataset','AMI dataset')
    config.addOption('MaxNumOfFiles', 3000, 'Maximum number of files in a given dataset patterns')
    config.addOption('MaxNumOfDatasets', 100, 'Maximum number of datasets in a given dataset patterns')

    # -------------------------------------------------
    # Tasks Options
    config = getConfig("Tasks")
    config.addOption('cloudPreference',[],'list of preferred clouds to choose for AnaTask analysis')
    config.addOption('backendPreference',["LCG","Panda","NG"],'order of preferred backends (LCG, Panda, NG) for AnaTask analysis')
    config.addOption('merged_files_per_job',1,'OBSOLETE', type=int)
    config.addOption('recon_files_per_job',10,'OBSOLETE', type=int)



def loadPlugins( config = {} ):

   import warnings
   warnings.filterwarnings('ignore','Python C API version mismatch for module pycurl')
   warnings.filterwarnings('ignore','Python C API version mismatch for module _lfc')

   import Lib.Athena
   import Lib.ATLASDataset
   import Lib.AthenaMC
   import Lib.AtlasLCGRequirements
   import Lib.Tasks
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
          return { 'X509_CERT_DIR' : gshell.env['X509_CERT_DIR'], 'X509_USER_PROXY' : gshell.env['X509_USER_PROXY']  }
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


