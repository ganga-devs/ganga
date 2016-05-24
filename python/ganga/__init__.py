# Bootstrap all of ganga, setup GPI, registries, etc.
from Ganga.Utility.Runtime import allRuntimes
import Ganga.Utility.Config
from Ganga.Utility.logging import getLogger
from Ganga import _gangaPythonPath
import Ganga.Core
from Ganga.Core.GangaRepository import getRegistry

logger = getLogger(modulename=True)


def ganga_license():
    'Print the full license (GPL)'
    with open(os.path.join(_gangaPythonPath, '..', 'LICENSE_GPL')) as printable:
        logger.info(printable.read())


# ------------------------------------------------------------------------------------
# Setup the shutdown manager
from Ganga.Core.InternalServices import ShutdownManager
ShutdownManager.install()

system_vars = {}
for opt in Ganga.Utility.Config.getConfig('System'):
    system_vars[opt] = Ganga.Utility.Config.getConfig('System')[opt]
Ganga.Utility.Config.configure([os.path.expanduser('~/.gangarc')], system_vars)

import ganga
from Ganga.Runtime import plugins

from Ganga.GPIDev.Base.Proxy import setProxyInterface
setProxyInterface(ganga)

from Ganga.Utility.Runtime import loadPlugins, autoPopulateGPI
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

