from GangaCore.Utility.Config import getConfig

if getConfig('DIRAC')['load_default_Dirac_backend']:
    from .Dirac import Dirac
