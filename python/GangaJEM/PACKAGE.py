################################################################################
# Ganga Project. http://cern.ch/ganga
#
# GangaJEM plugin: external packages definition
################################################################################

"""
Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = {
                     'JEM' : {
                              'version' : '0.3.2.1',
                              'noarch' : True
                              },
}

from Ganga.Utility.Setup import PackageSetup
setup = PackageSetup(external_packages)

def standardSetup(setup=setup):
    import Ganga.Utility.Setup

    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'PATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')

