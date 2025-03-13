import os

# Bootstrap all of ganga, setup GPI, registries, etc.
import atexit

from GangaCore.Utility.Runtime import allRuntimes
from GangaCore.Utility.Config import getConfig, setSessionValuesFromFiles
from GangaCore.Utility.logging import getLogger
from GangaCore import _gangaPythonPath
import GangaCore.Core
from GangaCore.Core.GangaRepository import getRegistry
from GangaCore.Core.InternalServices.ShutdownManager import _protected_ganga_exitfuncs

logger = getLogger(modulename=True)


def ganga_license():
    'Print the full license (GPL)'
    with open(os.path.join(_gangaPythonPath, '..', 'LICENSE_GPL')) as printable:
        logger.info(printable.read())


# ------------------------------------------------------------------------------------
# Setup the shutdown manager
atexit.register(_protected_ganga_exitfuncs)

import ganga

# Lets load the config files from disk
from GangaCore.Utility.Config import load_config_files
load_config_files()

# Setup the proxy interface early
from GangaCore.GPIDev.Base.Proxy import setProxyInterface
setProxyInterface(ganga)

# Init Setup and Load the RuntimePlugins

logger.debug("Import plugins")
try:
    # load Ganga system plugins...
    from GangaCore.Runtime import plugins
except Exception as x:
    logger.critical('Ganga system plugins could not be loaded due to the following reason: %s', x)
    logger.exception(x)
    raise GangaException.with_traceback(sys.exc_info()[2])

from GangaCore.Utility.Runtime import initSetupRuntimePackages, loadPlugins, autoPopulateGPI
initSetupRuntimePackages()
loadPlugins(ganga)
autoPopulateGPI(ganga)

from GangaCore.Core.GangaThread.WorkerThreads import startUpQueues
startUpQueues(ganga)

# ------------------------------------------------------------------------------------
# set the default value for the plugins

from GangaCore.Utility.Runtime import setPluginDefaults
setPluginDefaults(ganga)


from GangaCore.Runtime.bootstrap import manualExportToGPI
manualExportToGPI(ganga)


## Registries now add themselves to the Interface in this step
from GangaCore.Runtime.Repository_runtime import startUpRegistries
startUpRegistries(ganga)

# ------------------------------------------------------------------------------------
#  bootstrap core modules
interactive = False
from GangaCore.Core.GangaRepository import getRegistrySlice
GangaCore.Core.bootstrap(getRegistrySlice('jobs'), interactive, my_interface=ganga)
GangaCore.GPIDev.Lib.Config.bootstrap()

# ------------------------------------------------------------------------------------
# run post bootstrap hooks
for r in allRuntimes.values():
    try:
        r.postBootstrapHook()
    except Exception as err:
        logger.error("problems with post bootstrap hook for %s" % r.name)
        logger.error("Reason: %s" % str(err))
