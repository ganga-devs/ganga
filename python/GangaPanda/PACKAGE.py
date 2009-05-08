################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.6 2009-05-08 17:01:15 dvanders Exp $
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

_external_packages = {
    'panda-client' : { 'version' : '0.1.53', 
                    'PYTHONPATH':['lib/python2.3/site-packages'],
                    'CONFIGEXTRACTOR_PATH':'etc/panda/share',
                    'PANDA_SYS':'.',
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
        setup.prependPath(p,'PANDA_SYS')

