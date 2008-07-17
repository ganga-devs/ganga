###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.1 2008-07-17 16:41:17 moscicki Exp $
###############################################################################
""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""
#The platform needed by external packages. 
#Change this is you want to use external packages for different platform

import sys, os
from Ganga.Utility.Setup import PackageSetup


_external_packages = { 
    'DQ2Client' : {'version' : '20080212', 'PYTHONPATH':'usr/lib/python2.3/site-packages/', 'LD_LIBRARY_PATH':'.', 'PATH' : 'opt/dq2/bin', 'DQ2_HOME': 'opt/dq2', 'noarch':True }
    }

setup = PackageSetup(_external_packages)

def standardSetup(setup=setup):
    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')
        setup.setPath(p,'DQ2_HOME')
