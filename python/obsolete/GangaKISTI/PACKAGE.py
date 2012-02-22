################################################################################
# Ganga Project. http://cern.ch/ganga
#
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = { }
from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup(external_packages)

def standardSetup(setup=setup):
    import Ganga.Utility.Setup
#    Ganga.Utility.Setup.setPlatform('slc3_gcc323')
    
    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'PATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
 
