##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.7 2009-07-27 15:15:56 moscicki Exp $
##########################################################################

""" PACKAGE modules describe the installation and setup of the Ganga runtime packages.
Purpose: automatic initialization of Ganga environment setup and software distribution tools.

Each PACKAGE module should provide:
 - setup object : an instance of GangaCore.Utility.Setup.PackageSetup class
 - standardSetup() function

The setup object is used to describe the external dependencies of the package.
The standardSetup() function is used to perform automatic initialization of the environment of the package.
"""

# Default minimum Python version number asked for by Ganga
_defaultMinVersion = "2.6"
_defaultMinHexVersion = 0x20600f0

# The default values will be guessed but you may override them here
_defaultPlatform = None  # 'slc3_gcc323'
_defaultExternalHome = None
#_defaultExternalHome = "/afs/cern.ch/sw/ganga/external/"

# The dictionary of external  packages is used by the release/download
# system to handle the installation  tarballs. Make sure that all your
# dependencies go in here
#
# The layout of the external tree is fixed. The package top is:
#  externalHome/name/version/platf
#
# This layout is similar to  LCG external software repository and thus
# may be profitable in the long term.
#
# syspath specifies the relative path for PYTHONPATH setup (sys.path)
# if noarch=1 then platf is ommited
#
# if a path-like variable (e.g. LD_LIBRARY_PATH) is set using setup.prependPath() method, the
# value specified in the dictionary may be either string or a list of strings (they will be separated by colons ':').
#
_externalPackages = {
    'ipython': {'version': '1.2.1',
                'noarch': True,
                'syspath': 'lib/python'},
#    'paramiko': {'version': '1.7.3',
#                 'noarch': True,
#                 'syspath': 'lib/python2.3/site-packages'},
#    'pycrypto': {'version': '2.0.1',
#                 'syspath': 'lib/python2.3/site-packages'},
    'httplib2': {'version': '0.8',
                 'noarch': True,
                 'syspath': 'python'},
    'python-gflags': {'version': '2.0',
                      'noarch': True,
                      'syspath': 'python'},
    'google-api-python-client': {'version': '1.1',
                                 'noarch': True,
                                 'syspath': 'python'}
}


def detectPlatform():
    """ Try to guess the platform string according to the operating system, current environment and python interpreter.
    Ganga provides precompiled external packages on a limited set of _default platforms_ as explained in:
    https://twiki.cern.ch/twiki/bin/view/ArdaGrid/GangaSupportedPlatforms
    This function is set only to detect the well-known platform strings as defined by the LCG SPI project and is not meant to be a
    generic platform detection utility. If the platform cannot be guessed a default one is returned. This may or may not work on 
    other systems. In this case you should resolve the external binary dependencies yourself.

    Comments about current implementations:

    SLC5 platform is detected using platform module.

    If it's not SLC5 then:

    We assume that 64 bit python implies the slc4, amd64 system.
    We assume that 32 bit python implies the slc4, ia32 system.

    We ignore IA64 architecture (Opteron) as not frequently used.

    """

    # assume INTEL processors (i386, i686,x64), ignore IA64 architecture
    platf4 = {32: 'slc4_ia32_gcc34', 64: 'slc4_amd64_gcc34'}
    platf5 = {32: 'i686-slc5-gcc43-opt', 64: 'x86_64-slc5-gcc43-opt'}

    # for older python versions use some tricks
    import sys
    bits = sys.maxsize >> 32

    if bits:
        arch = 64
    else:
        arch = 32

    platfstring = platf4

    import platform
    import re
    c = re.compile(r'\S+-redhat-(?P<ver>\S+)-\S+')
    r = c.match(platform.platform())
    if r and r.group('ver').split('.')[0] == '5':
        platfstring = platf5

    return platfstring[arch]


# guess defaults if not defined
if not _defaultExternalHome:
    import os.path
    import GangaCore
    from GangaCore.Utility.files import fullpath
    p = fullpath(GangaCore.__file__)
    for i in range(5):
        p = os.path.dirname(p)
    _defaultExternalHome = os.path.join(p, 'external')

if not _defaultPlatform:
    _defaultPlatform = detectPlatform()


from GangaCore.Utility.Setup import PackageSetup
# The setup object
setup = PackageSetup(_externalPackages)


def standardSetup(setup=setup):
    """ Perform automatic initialization of the environment of the package.
    The gangaDir argument is only used by the core package, other packages should have no arguments.
    """

    from GangaCore.Utility.Setup import checkPythonVersion
    import sys

    # here we assume that the Ganga has been already prepended to sys.path by
    # the caller
    if checkPythonVersion(_defaultMinVersion, _defaultMinHexVersion):
        for name in setup.packages:
#            if name == 'pycrypto' and sys.hexversion > 0x2050000:
#                # hack the pycrypto path for 2.5
#                setup.packages['pycrypto']['syspath'] = setup.packages['pycrypto']['syspath'].replace('2.3', '2.5')
#
#            if name == 'paramiko' and sys.hexversion > 0x2050000:
#                # hack the paramiko path for 2.5
#                setup.packages['paramiko']['syspath'] = setup.packages['paramiko']['syspath'].replace('2.3', '2.5')

            setup.setSysPath(name)
            setup.prependPath(name, 'PYTHONPATH')

            # if other PATH variable must be defined, e.g. LD_LIBRARY_PATH, then
            # you should do it this way:
            # setup.prependPath(name,'LD_LIBRARY_PATH')
    else:
        sys.exit()

