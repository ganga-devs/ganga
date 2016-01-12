import ganga

from Ganga.Runtime import plugins

from Ganga.Runtime.GPIexport import exportToGPI
from Ganga.Utility.Plugin import allPlugins

# make all plugins visible in GPI
for k in allPlugins.allCategories():
    for n in allPlugins.allClasses(k):
        cls = allPlugins.find(k, n)
        if not cls._declared_property('hidden'):
            setattr(ganga, n, cls._proxyClass)
