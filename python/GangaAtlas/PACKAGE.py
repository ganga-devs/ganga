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
from GangaCore.Utility.Setup import PackageSetup, checkPythonVersion, getExternalHome


_external_packages = { 
    'DQ2Clients' : { 'version' : '2.6.1_rc15',
                     'DQ2_HOME' : 'opt/dq2',
                     'PATH' : ['opt/dq2/bin','nordugrid/bin'],
                     'syspath' : ['opt/dq2/lib/','external/mysqldb32/'],
                     'LD_LIBRARY_PATH' : ['external/mysql32/','external/mysqldb32/','external/nordugrid/lib/'],
                     'DQ2_ENDUSER_SETUP' : 'True',
                     'noarch':True ,
                     'RUCIO_APPID' : 'ganga',
                     },
    'rucio-clients' : { 'version' : '1.11.3',
                     'PATH' : ['bin/'],
                     'syspath' : [ 'externals/kerberos/lib.slc6-x86_64-2.6', 'externals/kerberos/lib.slc6-i686-2.6', 'lib/python2.7/site-packages' ],
                     'RUCIO_HOME' : '/afs/cern.ch/sw/ganga/external/rucio-clients/0.2.13/noarch/', # done properly below
                     'RUCIO_AUTH_TYPE' : 'x509_proxy',
                     'RUCIO_ACCOUNT' : 'ganga',
                     'noarch':True ,
                     'RUCIO_APPID' : 'ganga',
                     },

    'panda-client' : { 'version' : '0.5.94',
                       'syspath':['lib/python2.6/site-packages'],
                       'CONFIGEXTRACTOR_PATH':'etc/panda/share',
                       'PANDA_SYS':'.',
                       'noarch':True
                       },
    'zsi' : { 'version' : '2.1-a1',  # Needed for pyAMI
                       'syspath':['lib/python'],
                       'noarch':True
                       },
    '4Suite' : { 'version' : '1.0.2.1', 
                 'PYTHONPATH':['lib/python2.4/site-packages'],
                 'syspath':['lib/python2.4/site-packages']
                       },
    'pyAMI' : { 'version' : '3.1.2.1', 
                       'syspath':['.'],
                       'noarch':True
                       }

    }

# use DQ2Clients 2.3.0 if running <2.6
import sys
if sys.hexversion < 0x2050000:
    _external_packages['DQ2Clients']['version'] = '2.3.0'

# use appropriate RUCIO Client version
_external_packages['rucio-clients']['RUCIO_HOME'] = os.path.join(getExternalHome(), 'rucio-clients', _external_packages['rucio-clients']['version'], 'noarch')
if 'CMTCONFIG' in os.environ and 'slc5' in os.environ['CMTCONFIG'] and 'i686' in os.environ['CMTCONFIG']:
    _external_packages['rucio-clients']['syspath'] = [ 'externals/kerberos/lib.slc6-i686-2.6', 'externals/kerberos/lib.slc6-x86_64-2.6', 'lib/python2.6/site-packages' ]
elif 'CMTCONFIG' in os.environ and 'slc5' in os.environ['CMTCONFIG']:
    _external_packages['rucio-clients']['syspath'] = [ 'externals/kerberos/lib.slc6-x86_64-2.6', 'externals/kerberos/lib.slc6-i686-2.6', 'lib/python2.6/site-packages' ]

# use python 2.7 versions if required
if sys.version_info[0] == 2 and sys.version_info[1] == 7:
    new_paths = []
    for p in _external_packages['rucio-clients']['syspath']:
        new_paths.append( p.replace('2.6', '2.7') )
    _external_packages['rucio-clients']['syspath'] = new_paths

# if Panda is already setup, use that
for p in sys.path:
    if p.find("PandaClient") > -1:
        del _external_packages['panda-client']
        break

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
            elif name == '4Suite' and sys.hexversion > 0x2060000:
                # hack the 4Suite path for 2.6
                setup.packages['4Suite']['PYTHONPATH']  =  [ package.replace('2.4','2.6') for package in setup.packages['4Suite']['PYTHONPATH'] ]
                setup.packages['4Suite']['syspath']  =  [ package.replace('2.4','2.6') for package in setup.packages['4Suite']['syspath'] ]
    else:
        sys.exit()

    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')
        setup.setPath(p,'DQ2_HOME')
        if 'DQ2_ENDUSER_SETUP' in setup.packages[p]:
            os.environ['DQ2_ENDUSER_SETUP'] = setup.packages[p]['DQ2_ENDUSER_SETUP']
        setup.setPath(p,'PANDA_SYS')
        setup.setPath(p,'RUCIO_HOME')
        if 'RUCIO_AUTH_TYPE' in setup.packages[p]:
            os.environ['RUCIO_AUTH_TYPE'] = setup.packages[p]['RUCIO_AUTH_TYPE']

    # update the syspath and PYTHONPATH for the packages
    for name in setup.packages:
        setup.setSysPath(name)

