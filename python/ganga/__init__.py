import ganga

from Ganga.Runtime import plugins
from Ganga.Runtime.Repository_runtime import bootstrap
import Ganga.GPIDev.Lib.Registry
from Ganga.Utility.Plugin import allPlugins
import Ganga.GPI

# start up the registries
bootstrap()

# make all plugins visible in both the GPI and ganga.*
for k in allPlugins.allCategories():
    for n in allPlugins.allClasses(k):
        cls = allPlugins.find(k, n)
        if not cls._declared_property('hidden'):
            setattr(ganga, n, cls._proxyClass)
            setattr(Ganga.GPI, n, cls._proxyClass)
