import os

# Bootstrap all of ganga, setup GPI, registries, etc.
import atexit

from Ganga.Utility.Runtime import allRuntimes
from Ganga.Utility.Config import getConfig, setSessionValuesFromFiles
from Ganga.Utility.logging import getLogger
from Ganga import _gangaPythonPath
import Ganga.Core
from Ganga.Core.GangaRepository import getRegistry
from Ganga.Core.InternalServices.ShutdownManager import _ganga_run_exitfuncs

logger = getLogger(modulename=True)


def ganga_license():
    'Print the full license (GPL)'
    with open(os.path.join(_gangaPythonPath, '..', 'LICENSE_GPL')) as printable:
        logger.info(printable.read())


# ------------------------------------------------------------------------------------
# Setup the shutdown manager
atexit.register(_ganga_run_exitfuncs)

import ganga

# Lets load the config files from disk
from Ganga.Utility.Config import load_config_files
load_config_files()

# Setup the proxy interface early
from Ganga.GPIDev.Base.Proxy import setProxyInterface
setProxyInterface(ganga)

# Init Setup and Load the RuntimePlugins

logger.debug("Import plugins")
try:
    # load Ganga system plugins...
    from Ganga.Runtime import plugins
except Exception as x:
    logger.critical('Ganga system plugins could not be loaded due to the following reason: %s', x)
    logger.exception(x)
    raise GangaException(x), None, sys.exc_info()[2]

from Ganga.Utility.Runtime import initSetupRuntimePackages, loadPlugins, autoPopulateGPI
initSetupRuntimePackages()
loadPlugins(ganga)
autoPopulateGPI(ganga)

from Ganga.Core.GangaThread.WorkerThreads import startUpQueues
startUpQueues(ganga)

# ------------------------------------------------------------------------------------
# set the default value for the plugins

from Ganga.Utility.Runtime import setPluginDefaults
setPluginDefaults(ganga)


from Ganga.Runtime.bootstrap import manualExportToGPI
manualExportToGPI(ganga)


## Registries now add themselves to the Interface in this step
from Ganga.Runtime.Repository_runtime import startUpRegistries
startUpRegistries(ganga)

# ------------------------------------------------------------------------------------
#  bootstrap core modules
interactive = False
from Ganga.Core.GangaRepository import getRegistrySlice
Ganga.Core.bootstrap(getRegistrySlice('jobs'), interactive, my_interface=ganga)
Ganga.GPIDev.Lib.Config.bootstrap()

# ------------------------------------------------------------------------------------
# run post bootstrap hooks
for r in allRuntimes.values():
    try:
        r.postBootstrapHook()
    except Exception as err:
        logger.error("problems with post bootstrap hook for %s" % r.name)
        logger.error("Reason: %s" % str(err))

