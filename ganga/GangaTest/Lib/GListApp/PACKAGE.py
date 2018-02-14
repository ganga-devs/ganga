##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.1 2008-07-17 16:41:11 moscicki Exp $
##########################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = {}

from GangaCore.Utility.Setup import PackageSetup

setup = PackageSetup(external_packages)


def standardSetup(setup=setup):
    pass
