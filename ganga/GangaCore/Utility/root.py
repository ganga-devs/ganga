###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: root.py,v 1.1 2008-07-17 16:41:01 moscicki Exp $
###############################################################################

from GangaCore.Utility.Config import getConfig, ConfigError
from subprocess import getstatusoutput
import GangaCore.Utility.logging
import os
from GangaCore.Utility.logging import getLogger
logger = GangaCore.Utility.logging.getLogger()


##  Walk top-down to find 'directory' within the base_path folder(s) and return the path of this
##  directory if it does exist. Returns None if it doesn't
##  If it does the rest of the folder structure is descended and appended to the returned path
##  This allows for custom root install paths sorted by version and arch
def _manipulatePath(base_path, directory):

    split_path = base_path.split(os.sep)

    split_path_len = len(split_path)

    found_path = None

    for i in range(split_path_len):

        local_base = str('%s'%os.sep).join(split_path[:i])

        if os.path.isdir(local_base):
            dir_path = os.path.join(local_base, str(directory))

            if os.path.isdir(dir_path):

                descending_path = str("%s"%os.sep).join(split_path[i+1:])

                if not os.path.isdir(dir_path):
                    found_path = None
                    break

                found_path = os.path.join(dir_path, descending_path)

                if not os.path.isdir(found_path):
                    found_path = dir_path
                    logger.warning("Unable to descend into ROOT path %s" % str(found_path))
                    logger.warning("Assuming this is ROOTSYS")

                return found_path

    return found_path

##  Get the ROOTSYS with a given version in the path
def _getPathVersion( base_path, version):

    found_path = _manipulatePath( base_path, version )

    if found_path is None:
        logger.error("Cannot find ROOT version: %s from manipulating PATH: %s" % (str(version), str(base_path)))
        logger.error("Please check your configuration in your .gangarc")
        logger.error("Attempting to make use of path: %s" % str(base_path) )
        return base_path
    else:
        return found_path

##  Get the ROOTSYS with a given arch in the path
def _getPathArch( base_path, arch):

    found_path = _manipulatePath( base_path, arch )

    if found_path is None:
        logger.error("Cannot find ROOT for arch: %s from manipulating PATH: %s" % (str(arch), str(base_path)))
        logger.error("Pleasde check your configuration in your .gangarc")
        logger.error("Attempting to make use of path: %s" % str(base_path))
        return base_path
    else:
        return found_path

## Get the rootsys by searching first for the requested version and then the requested arch
def getrootsys(version=None, arch=None):

    configroot = getConfig('ROOT')

    if configroot['path'] != '':
        return configroot['path']

    location = configroot.getEffectiveOption('location')

    if arch is None and version is None:
        if location:
            if os.path.isdir(location):
                rootsys = location
                return rootsys

    elif arch is None and version is not None:

        version_path = _getPathVersion( location, version )

        rootsys = version_path

        return rootsys

    elif arch is not None and version is None:

        arch_path = _getPathArch( location, arch )

        rootsys = arch_path

    else:

        version_path = _getPathVersion( location, version )

        arch_path = _getPathArch( version_path, arch )

        rootsys = arch_path

        return rootsys


    rootsys = ""
    try:
        configroot = getConfig('ROOT')
        if version is None:
            rootver = configroot['version']
        else:
            rootver = str(version)
        if arch is None:
            rootarch = configroot['arch']
        else:
            rootarch = str(arch)
        if configroot['path'] != "":
            rootsys = configroot['path'] + "/"
        else:
            rootsys = configroot['location'] + "/" + rootver + "/" + rootarch
            if os.path.exists(rootsys + "/root/"):
                rootsys = rootsys + "/root/"
        if not os.path.isdir( rootsys ):
            rootsys = configroot['location']
    except ConfigError:
        pass
    logger.debug("ROOTSYS: %s", rootsys)

    return rootsys


def getenvrootsys():
    """Determine and return $ROOTSYS environment variable"""
    import os
    try:
        rootsys = os.environ['ROOTSYS']
    except KeyError:
        rootsys = ""
    return rootsys


def getpythonhome(arch=None, pythonversion=None):
    """Looks for the PYTHONHOME for the particular version and arch"""
    pythonhome = ''
    try:
        # returns a copy
        configroot = getConfig('ROOT').getEffectiveOptions()
        if arch is not None:
            configroot['arch'] = arch
        if pythonversion is not None:
            configroot['pythonversion'] = pythonversion
        # allow other Root variables to be used in the definition
        pythonhome = configroot['pythonhome']
        # supports ${foo} type variable expansion
        for k in configroot.keys():
            pythonhome = pythonhome.replace('${%s}' % k, configroot[k])
    except ConfigError as err:
        logger.debug("Config Error!\n%s"%str(err))
        pass
    import os
    if not os.path.exists(pythonhome):
        pythonhome2 = pythonhome.replace('../../external', '../external')
        if os.path.exists(pythonhome2):
            pythonhome = pythonhome2
    logger.debug('PYTHONHOME: %s', pythonhome)
    return pythonhome


def getenvpythonhome():
    """Deterimin the PYTHONHOME environment variable"""
    import os
    pythonhome = ''
    try:
        pythonhome = os.environ['PYTHONHOME']
    except KeyError:
        pass
    return pythonhome


def getconfrootsys():
    """Determine and return ROOTSYS from ganga configuration"""
    return GangaCore.Utility.root.getrootsys()


def getrootprefix(rootsys=None):
    """Determine ROOT path and return prefix,
    emtpy if ROOT is not found in path or ERROR,
    else ROOTSYS+LD_LIBRARY_PATH+prefix
    """
    rc = 0
    if rootsys is None:
        rootsys = GangaCore.Utility.root.getconfrootsys()
        if rootsys == "":
            rootsys = GangaCore.Utility.root.getenvrootsys()
            if rootsys == "":
                logger.error("No proper ROOT setup")
                rc = 1

    rootprefix = "ROOTSYS=" + rootsys + " LD_LIBRARY_PATH=" + \
        rootsys + "/lib:$LD_LIBRARY_PATH " + rootsys + "/bin/"
    logger.debug("ROOTPREFIX: %s", rootprefix)

    return rc, rootprefix


def checkrootprefix(rootsys=None):
    """Check if rootprefix variable holds valid values"""

    rc, rootprefix = GangaCore.Utility.root.getrootprefix(rootsys)

    cmdtest = rootprefix + "root-config --version"
    rc, out = getstatusoutput(cmdtest)
    if (rc != 0):
        logger.error("No proper ROOT setup")
        logger.error("%s", out)
        return 1
    else:
        logger.info("ROOT Version: %s", out)
        return 0


logger = GangaCore.Utility.logging.getLogger()

# $Log: not supported by cvs2svn $
# Revision 1.8.24.1  2007/10/12 13:56:28  moscicki
# merged with the new configuration subsystem
#
# Revision 1.8.26.1  2007/10/09 13:46:22  roma
# Migration to new Config
#
# Revision 1.8  2007/04/13 11:26:28  moscicki
# root version upgrade to 5.14.00d from Will
#
# Revision 1.7  2007/04/12 10:22:55  moscicki
# root version upgrade to 5.14.00b from Will
#
# Revision 1.6  2007/03/14 12:15:14  moscicki
# patches from Will
#
# Revision 1.5  2006/08/08 14:07:45  moscicki
# config fixes from U.Egede
#
# Revision 1.4  2006/08/01 10:05:59  moscicki
# changes from Ulrik
#
# Revision 1.3  2006/06/21 11:50:23  moscicki
# johannes elmsheuser:
#
# * more modular design and a few extentions
# * get $ROOTSYS from configuration or environment
#
# Revision 1.1  2006/06/13 08:46:56  moscicki
# support for ROOT
#
