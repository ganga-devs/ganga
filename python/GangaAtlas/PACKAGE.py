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
from Ganga.Utility.Setup import PackageSetup, checkPythonVersion


_external_packages = { 
    'DQ2Clients' : { 'version' : '2.4.0',
                     'DQ2_HOME' : 'opt/dq2',
                     'PATH' : ['opt/dq2/bin','nordugrid/bin'],
                     'PYTHONPATH' : ['opt/dq2/lib/','external/mysqldb32/'],
                     'LD_LIBRARY_PATH' : ['external/mysql32/','external/mysqldb32/','external/nordugrid/lib/'],
                     'DQ2_ENDUSER_SETUP' : 'True',
                     'noarch':True ,
                     'RUCIO_APPID' : 'ganga',
                     },
    'panda-client' : { 'version' : '0.4.21', 
                       'PYTHONPATH':['lib/python2.4/site-packages'],
                       'CONFIGEXTRACTOR_PATH':'etc/panda/share',
                       'PANDA_SYS':'.',
                       'noarch':True
                       },
    'zsi' : { 'version' : '2.1-a1', 
                       'PYTHONPATH':['lib/python'],
                       'noarch':True
                       },
    '4Suite' : { 'version' : '1.0.2.1', 
                 'PYTHONPATH':['lib/python2.4/site-packages'],
                 'syspath':['lib/python2.4/site-packages']
                       },
    'pyAMI' : { 'version' : '3.1.2.1', 
                       'PYTHONPATH':['.'],
                       'noarch':True
                       }

    }


setup = PackageSetup(_external_packages)

# Default minimum Python version number asked for by Ganga
_defaultMinVersion = "2.3"
_defaultMinHexVersion = 0x20300f0

def standardSetup(setup=setup):

    # here we assume that the Ganga has been already prepended to sys.path by the caller
    if checkPythonVersion(_defaultMinVersion,_defaultMinHexVersion):
        for name in setup.packages:
            if name == '4Suite' and (sys.hexversion > 0x2050000 and sys.hexversion < 0x2060000):
                # hack the 4Suite path for 2.5
                setup.packages['4Suite']['PYTHONPATH']  =  [ package.replace('2.4','2.5') for package in setup.packages['4Suite']['PYTHONPATH'] ]
                setup.packages['4Suite']['syspath']  =  [ package.replace('2.4','2.5') for package in setup.packages['4Suite']['syspath'] ]
                setup.setSysPath(name)
            elif name == '4Suite' and sys.hexversion > 0x2060000:
                # hack the 4Suite path for 2.6
                setup.packages['4Suite']['PYTHONPATH']  =  [ package.replace('2.4','2.6') for package in setup.packages['4Suite']['PYTHONPATH'] ]
                setup.packages['4Suite']['syspath']  =  [ package.replace('2.4','2.6') for package in setup.packages['4Suite']['syspath'] ]
                setup.setSysPath(name)
            else:
               pass 
    else:
        sys.exit()

    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')
        setup.setPath(p,'DQ2_HOME')
        if setup.packages[p].has_key('DQ2_ENDUSER_SETUP'):
            os.environ['DQ2_ENDUSER_SETUP'] = setup.packages[p]['DQ2_ENDUSER_SETUP']
        setup.setPath(p,'PANDA_SYS')

    
