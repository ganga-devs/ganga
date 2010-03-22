###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.16 2009-06-18 08:32:28 dvanders Exp $
###############################################################################
""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""
#The platform needed by external packages. 
#Change this is you want to use external packages for different platform

import sys, os
from Ganga.Utility.Setup import PackageSetup


_external_packages = { 
    'DQ2Clients' : { 'version' : '0.1.33.1',
                     'DQ2_HOME' : 'opt/dq2',
                     'PATH' : ['opt/dq2/bin','nordugrid/bin'],
                     'PYTHONPATH' : ['usr/lib/python2.3/site-packages/','external/mysqldb32/'],
                     'LD_LIBRARY_PATH' : ['external/mysql32/','external/mysqldb32/','external/nordugrid/lib/'],
                     'DQ2_ENDUSER_SETUP' : 'True',
                     'noarch':True 
                     },
    'panda-client' : { 'version' : '0.2.21', 
                       'PYTHONPATH':['lib/python2.3/site-packages'],
                       'CONFIGEXTRACTOR_PATH':'etc/panda/share',
                       'PANDA_SYS':'.',
                       'noarch':True
                       }

    }


setup = PackageSetup(_external_packages)

def standardSetup(setup=setup):
    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')
        setup.setPath(p,'DQ2_HOME')
        if setup.packages[p].has_key('DQ2_ENDUSER_SETUP'):
            os.environ['DQ2_ENDUSER_SETUP'] = setup.packages[p]['DQ2_ENDUSER_SETUP']
        setup.setPath(p,'PANDA_SYS')
