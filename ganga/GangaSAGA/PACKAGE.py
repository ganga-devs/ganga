##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

from GangaCore.Utility.Setup import PackageSetup
external_packages = {}

setup = PackageSetup(external_packages)


def standardSetup(setup=setup):
    import GangaCore.Utility.Setup
#    GangaCore.Utility.Setup.setPlatform('slc3_gcc323')

    for p in setup.packages:
        setup.prependPath(p, 'PYTHONPATH')
        setup.prependPath(p, 'PATH')
        setup.prependPath(p, 'LD_LIBRARY_PATH')
