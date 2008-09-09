################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.3 2008-09-03 16:59:14 dvanders Exp $
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

_external_packages = {
    'PandaTools' : { 'version' : '20080827', 
                    'PYTHONPATH':'python',
                    'CONFIGEXTRACTOR_PATH':'share',
                    'noarch':True
    } 
}

from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup(_external_packages)

def standardSetup(setup=setup):
    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')
        setup.prependPath(p,'CONFIGEXTRACTOR_PATH')

