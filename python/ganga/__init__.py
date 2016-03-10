# Bootstrap all of ganga, setup GPI, registries, etc.
from Ganga.Utility.Runtime import allRuntimes
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Plugin import allPlugins
from Ganga.GPIDev.Base import ProtectedAttributeError, ReadOnlyObjectError, GangaAttributeError
from Ganga.GPIDev.Lib.Job.Job import JobError
from Ganga import _gangaPythonPath
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory, getProxyClass
from Ganga.GPIDev.Credentials import getCredential
from Ganga.GPIDev.Persistency import export, load
from Ganga.GPIDev.Adapters.IPostProcessor import MultiPostProcessor
from Ganga.Runtime import Repository_runtime
import Ganga.Core
from Ganga.GPIDev.Lib.JobTree import TreeError
from Ganga.Core.GangaRepository import getRegistry
from Ganga.GPIDev.Base.VPrinter import full_print
from Ganga.GPIDev.Base.Proxy import implRef
import Ganga.GPIDev.Lib.Config
from Ganga.Utility.feedback_report import report
from Ganga.Runtime.gangadoc import adddoc
from Ganga.Core.GangaThread.WorkerThreads.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
from Ganga.Runtime import plugins

logger = getLogger(modulename=True)


def ganga_license():
    'Print the full license (GPL)'
    with open(os.path.join(_gangaPythonPath, '..', 'LICENSE_GPL')) as printable:
        logger.info(printable.read())


# ------------------------------------------------------------------------------------
# Setup the shutdown manager
from Ganga.Core.InternalServices import ShutdownManager
ShutdownManager.install()

from Ganga.Utility.Runtime import loadPlugins, autoPopulateGPI
import ganga
loadPlugins( [Ganga.GPI, ganga] )
autoPopulateGPI(ganga)

from Ganga.Core.GangaThread.WorkerThreads import startUpQueues
startUpQueues()

# ------------------------------------------------------------------------------------
# set the default value for the plugins

from Ganga.Utility.Runtime import setPluginDefaults
setPluginDefaults(ganga)


from Ganga.Runtime.bootstrap import manualExportToGPI
manualExportToGPI(ganga)


from Ganga.Runtime.Repository_runtime import startUpRegistries
startUpRegistries()

# ------------------------------------------------------------------------------------
#  bootstrap core modules
interactive = False
Ganga.Core.bootstrap(getattr(Ganga.GPI.jobs, implRef), interactive)
Ganga.GPIDev.Lib.Config.bootstrap()

# ------------------------------------------------------------------------------------
# run post bootstrap hooks
for r in allRuntimes.values():
    try:
        r.postBootstrapHook()
    except Exception as err:
        logger.error("problems with post bootstrap hook for %s" % r.name)
        logger.error("Reason: %s" % str(err))

