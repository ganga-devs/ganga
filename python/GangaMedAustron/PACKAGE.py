###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.16 2009/06/18 08:32:28 dvanders Exp $
###############################################################################
""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""
#The platform needed by external packages. 
#Change this is you want to use external packages for different platform

import sys, os
from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup({})

def standardSetup(setup=setup):
    for p in setup.packages:
        print p
        setup.prependPath(p,'PYTHONPATH')
