# File: GangaSNOplus/__init__.py

def loadPlugins( config = {} ):

    # import any modules that need to be visible to the user

    from . import Lib.Applications
    from . import Lib.Backends

    return None

