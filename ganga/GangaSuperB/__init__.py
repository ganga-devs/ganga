#Bail out when loading this module as it is not python3 compliant
from GangaCore.Core.exceptions import PluginError
raise PluginError("The GangaSuperB module has not been upgraded for python 3. The last python 2 Ganga version is 7.1.15 . Please contact the ganga devs to discuss updating this module.")

from GangaCore.Runtime.GPIexport import exportToGPI
from GangaCore.Utility.Config import *

from GangaCore.Utility.Config.Config import _after_bootstrap
from GangaCore.Utility.logging import getLogger
logger = getLogger()

if not _after_bootstrap:
    config = makeConfig('SuperB', 'Configuration parameters for SuperB ganga plugin')
    # /storage/gpfs_superb/users/ganga/ will be replaced by $VO_SUPERBVO_ORG_SW_DIR
    config.addOption('severus_dir', '/storage/gpfs_superb/users/ganga_util/GangaSuperB/severus', 'Local UI directory where severus.tgz is located.')
    config.addOption('submission_site', 'INFN-T1', 'Target site where job analysis output will be transfered. Must be set to local UI site.')
    config.addOption('gridmon_user', '', 'Gridmon DB username')
    config.addOption('gridmon_pass', '', 'Gridmon DB password')
    config.addOption('sbk_user', '', 'sbk DB username')
    config.addOption('sbk_pass', '', 'sbk DB password')


def standardSetup():
    import PACKAGE
    PACKAGE.standardSetup()


def loadPlugins(c):
    import Lib.SBApp
    import Lib.SBSubmission
    import Lib.SBDatasetManager
    import Lib.SBInputDataset
    import Lib.SBOutputDataset
    #import Lib.SBRequirements
    
    #exportToGPI('SBRequirements', Lib.SBRequirements.SBRequirements, "Functions")
