import os
import Ganga.Utility.logging
import Ganga.Utility.Config
configLHCb=Ganga.Utility.Config.makeConfig('LHCb','Parameters for LHCb')
configDirac=Ganga.Utility.Config.makeConfig('DIRAC','Parameters for DIRAC')
logger=Ganga.Utility.logging.getLogger()



# Set default values for the LHCb config section
# The location where the DIRAC client is found
configLHCb.addOption('DiracTopDir', '/afs/cern.ch/lhcb/software/releases/DIRAC/DIRAC_v2r19', 
                 'The location of the DIRAC client installation that Ganga should use.')
# The age in minutes that a cache of site locations for LFNs in an
# LHCbDataset stays valid.
configLHCb.addOption('maximum_cache_age', 10080,
                 'The age in minutes that a cache of site locations for LFNs in an LHCbDataset stays valid.')
# The name of the local site to be used for resolving LFNs into PFNs.
configLHCb.addOption('LocalSite','',
                 'The name of the local site to be used for resolving LFNs into PFNs.')
# The protocol to be used for reading PFNs (rfio, castor etc.)
configLHCb.addOption('SEProtocol','',
                 'The protocol to be used for reading PFNs (rfio, castor etc.)')
configLHCb.addOption('DataOutput',os.environ['HOME'],'The place where OutputData should go.')
configLHCb.addOption('mkdir_cmd','/bin/mkdir','The command to used to create a directory in the locations of `DataOutput`')
configLHCb.addOption('cp_cmd','/bin/cp','The command used to copy out data to the `DataOutput` locations')


configDirac.addOption('RootVersions',
        {'5.14.00f': 'v19r5', '5.14.00h': 'v19r8', '5.14.00i': 'v19r9', '5.18.00': 'v19r10', '5.18.00a': 'v19r11', '4.04.02': 'v14r5', '3.10.02': 'v12r18'},
        'Root versions used by Dirac for given versions of DaVinci')
configDirac.addOption('extraProxytime','600','extra lifetime required for a proxy to submit a job')
configDirac.addOption('DIRACsite','None','Used for testing only')
configDirac.addOption('DiracLoggerLevel','ERROR','The logging level of DIRAC, not the logging level of the Dirac plugin in Ganga')
    
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

# Revision 1.37  2008/05/01 15:47:45  uegede
# - Removed forced 32 bit running
# - Took away modifications to LD_LIBRARY_PATH
# - Introduced "diracwrapper"to execute DIRAC commands in separate
#   process with a different environment. Updates to LHCbDataset and
#   Dirac backend to make use of this feature.
#
# Revision 1.28.6.5  2008/03/17 12:08:03  andrew
# Tried to fix the worst problems after merging from head
#
# Revision 1.28.6.4  2008/03/17 11:08:26  andrew
# Merge from head
#
# Revision 1.28.6.3  2007/12/13 09:31:00  wreece
# Initial pass at porting GangaLHCb to the new config system. Also sorts out some of the warnings in the files.
#
# Revision 1.28.6.2  2007/12/12 19:54:04  wreece
# Merge in changes from HEAD.
#
# Revision 1.33  2007/12/07 12:58:29  andrew
# fixed looger and Config once and for all :)))))
#
# Revision 1.32  2007/12/07 09:52:24  andrew
# fixed problem with  missing logger variable in append_LD_LIBRARY_PATH
#
# Revision 1.31  2007/11/13 16:48:33  andrew
# Added a default for the config option DiracLoggerLevel
#
# Revision 1.30  2007/10/17 12:10:20  andrew
# Fix for bug #30479. In append_LD_LIBRARY_PATH(), check is the cached shell
# exists.
#
# Revision 1.29  2007/10/15 15:30:18  uegede
# Merge from Ganga_4-4-0-dev-branch-ulrik-dirac with new Dirac backend
#
# Revision 1.28.2.1  2007/08/15 15:44:59  uegede
# Test branch
#
# Revision 1.36  2008/03/07 16:19:46  andrew
# Fix for bug #21546: Force use of DiracTopDir, even when LHCBPRODROOT is
#       defined
#
# Revision 1.35  2008/03/07 15:13:39  andrew
#
# Fixes for:
#
# - [-] Bug fixes
#     - [+] bug #28955: cmt.showuses() broken
#     - [+] bug #33367: Option file format changed for specifying XML
#           slice
#     - [+] bug #29368: Dataoutput variable wrongly flagged as undefined
#     - [+] bug #33720: Multiple inclusion of options file
#
#
# Removes CERN centricity of the Gaudi wrapper script for batch and interactive
#
# Revision 1.34  2008/01/23 23:15:42  uegede
# - Changed default DIRAC version to v2r18
# - Changed magic line in python script for DIRAC to have
#   "/bin/env python". this ensures that python version which is in PATH
#   is started.
# - Removed Panoramix application type as it never worked
# - Removed GaudiLCG runtime handler as it is not functional.
#
# Revision 1.33  2007/12/07 12:58:29  andrew
# fixed looger and Config once and for all :)))))
#
# Revision 1.32  2007/12/07 09:52:24  andrew
# fixed problem with  missing logger variable in append_LD_LIBRARY_PATH
#
# Revision 1.31  2007/11/13 16:48:33  andrew
# Added a default for the config option DiracLoggerLevel
#
# Revision 1.30  2007/10/17 12:10:20  andrew
# Fix for bug #30479. In append_LD_LIBRARY_PATH(), check is the cached shell
# exists.
#
# Revision 1.29  2007/10/15 15:30:18  uegede
# Merge from Ganga_4-4-0-dev-branch-ulrik-dirac with new Dirac backend
#
# Revision 1.28.2.1  2007/08/15 15:44:59  uegede
# Test branch
