# File: GangaSNOplus/__init__.py

#Bail out when loading this module as it is not python3 compliant
from GangaCore.Core.exceptions import PluginError
raise PluginError("The GangaSNOplus module has not been upgraded for python 3. The last python 2 Ganga version is 7.1.15 . Please contact the ganga devs to discuss updating this module.")

def loadPlugins( config = {} ):

    # import any modules that need to be visible to the user

    import Lib.Applications
    import Lib.Backends

    return None

