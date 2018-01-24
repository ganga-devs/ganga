################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.9 2009-06-18 08:35:46 dvanders Exp $
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

_external_packages = {
}

from GangaCore.Utility.Setup import PackageSetup

setup = PackageSetup(_external_packages)

def standardSetup(setup=setup):
    for p in setup.packages:
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')
        setup.setPath(p,'PANDA_SYS')

    # update the syspath and PYTHONPATH for the packages
    for name in setup.packages:
        setup.setSysPath(name)
