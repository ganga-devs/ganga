###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.1 2008-10-04 17:42:37 karl Exp $
###############################################################################
""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = {}

from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup( external_packages )

def standardSetup( setup=setup ):
    pass
