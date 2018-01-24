
from GangaCore.Utility.Config import getConfig

outputconfig = getConfig("Output")

def getSharedPath():
    # Required to be initialized for ShareDir object
    from GangaCore.Utility.files import expandfilename
    import os.path
    Conf_config = getConfig('Configuration')
    root_default = os.path.join(expandfilename(Conf_config['gangadir']), 'shared', Conf_config['user'])
    return root_default


