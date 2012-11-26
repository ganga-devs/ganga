from Ganga.Utility.Config import getConfig
from DiracBase import DiracBase


if getConfig('DIRAC')['load_default_Dirac_backend']:
    from Dirac import Dirac
