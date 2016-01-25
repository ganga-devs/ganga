import ganga

from Ganga.Runtime import plugins
from Ganga.Runtime.Repository_runtime import bootstrap
import Ganga.GPIDev.Lib.Registry
from Ganga.Utility.Plugin import allPlugins
import Ganga.GPI

# make all plugins visible in both the GPI and ganga.*
for k in allPlugins.allCategories():
    for n in allPlugins.allClasses(k):
        cls = allPlugins.find(k, n)
        if not cls._declared_property('hidden'):
            setattr(ganga, n, cls._proxyClass)
            setattr(Ganga.GPI, n, cls._proxyClass)

# start up the registries
for n, k, d in bootstrap():
    setattr(ganga, n, k)
    setattr(Ganga.GPI, n, k)

# bootstrap the workspace
from Ganga.Runtime import Workspace_runtime
Workspace_runtime.bootstrap()

# bootstrap core modules
from Ganga.GPIDev.Base.Proxy import proxyRef
Ganga.Core.bootstrap(getattr(Ganga.GPI.jobs, proxyRef), True)

# start queues
from Ganga.Runtime.GPIexport import exportToGPI
from Ganga.Core.GangaThread.WorkerThreads.ThreadPoolQueueMonitor import ThreadPoolQueueMonitor
setattr(ganga, "queues", ThreadPoolQueueMonitor() )
setattr(Ganga.GPI, "queues", ThreadPoolQueueMonitor() )

# Setup shutdown manager
from Ganga.Core.InternalServices import ShutdownManager
ShutdownManager.install()