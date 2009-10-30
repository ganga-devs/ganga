################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.1 2008/11/19 15:18:35 mbarison Exp $
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

from Ganga.Utility.Setup import PackageSetup

_external_packages = {
    'DQ2Client' : {'version' : '0.3a', 'PYTHONPATH':'.', 'LD_LIBRARY_PATH':'.', 'PATH' : '.', 'DQ2_HOME': '.', 'noarch':True }         
                     }

setup = PackageSetup(_external_packages)

def standardSetup(setup=setup):
    pass
