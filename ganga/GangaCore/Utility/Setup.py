
class PackageSetup(object):

    """ PackageSetup objects represent the external packages required by a given runtime unit of GangaCore.
    The package information is stored in a dictionary. See Ganga/PACKAGE.py
    """

    def __init__(self, packages):
        """
        Initialize the Package object with the list of required external packages
        """
        self.packages = packages

    def getPackagePath(self, name, force=False):
        """ Return a tuple (path,tarball) which describes the external package:
             - path points to the package top directory
             - tarball is the package tarball name

            If *force=False* (default) the python version is checked and if the package
            is not required then return ('','').

            If *force=True* the path is returned even if the package is not required.            
        """

        import os.path
        import sys

        package = self.packages[name]

        try:
            package_required = sys.hexversion < int(
                package['maxHexVersion'], 16) or force
        except (KeyError, ValueError):
            package_required = 1

        if not package_required:
            return ('', '')

        platfdir = getPlatform()

        if 'noarch' in package and package['noarch']:
            platfdir = 'noarch'

        return (os.path.join(getExternalHome(), name, package['version'], platfdir),
                '-'.join([name, package['version'], platfdir, 'ganga.tar.gz']))

    def getPackagePath2(self, name, var, force=False):
        """ Return a path to the package for specified environment variable.
        For example: _getPackagePath('x','LD_LIBRARY_PATH') returns a LD_LIBRARY_PATH for the package (if defined).
        The value may be a string or a list of strings - in the later case the individual path components will
        be separated by colons (':').
        If the path is not defined (see getPackagePath()) then return an empty string.
        """
        import os
        path, tarball = self.getPackagePath(name, force)
        if path and var in self.packages[name]:
            from GangaCore.Utility.util import isStringLike

            ppath = self.packages[name][var]

            if isStringLike(ppath):
                ppath = [ppath]

            return ':'.join(map(lambda p: os.path.join(path, p), ppath))

# for p in ppath:
##                 result += os.path.join(path,p)
# return result
        else:
            return ''

    def prependPath(self, name, var):
        """ Update environment (os.environ) and prepend a package path to var.
        """
        import sys
        import os.path

        if getExternalHome():
            path = self.getPackagePath2(name, var)
            if path:
                try:
                    pp = ':' + os.environ[var]
                except KeyError:
                    pp = ''
                os.environ[var] = path + pp

    def setSysPath(self, name, set_python_path=True):
        """ Update the sys.path variable for a given package by adding a given package to the PYTHONPATH.
        It assumes that the first item in PYTHONPATH is Ganga, so the package path is added as a second item.
        If set_python_path is True, it will update the PYTHONPATH as well
        """
        import sys
        import os

        if getExternalHome():

            # get the path(s)
            paths = self.getPackagePath2(name, 'syspath')
            if not paths:
                return True

            # loop over the sys paths to be added
            for path in reversed(paths.split(':')):
                if path not in sys.path:

                    # add to sys path
                    sys.path.insert(1, path)

                # add to PYTHONPATH?
                if set_python_path:
                    if 'PYTHONPATH' in os.environ:
                        pp_toks = os.environ['PYTHONPATH'].split(':')
                    else:
                        # PYTHONPATH is empty so add Ganga in first
                        pp_toks = [sys.path[0]]

                    # create a new PYTHONPATH
                    if path not in pp_toks:
                        pp_toks.insert(1, path)

                    os.environ['PYTHONPATH'] = ':'.join(pp_toks)

        return True

    def setPath(self, name, var):
        """ 
        Update environment (os.environ) and *set* (overwrite) a package path to var.
        """
        import sys
        import os

        if getExternalHome():
            path = self.getPackagePath2(name, var)
            if path:
                os.environ[var] = path

        return True


def checkPythonVersion(minVersion, minHexVersion):
    """Function to check that the Python version number is greater
       than the minimum required.

       Arguments:
          minVersion    - String representation of minimum version required
          minHexVersion - Hexadecimal representation of minimum version required

       Return value: True if Python version number is greater than minimum,
                     False otherwise"""

    import sys

    status = True

    if sys.hexversion < minHexVersion:
        from GangaCore.Utility.logging import getLogger
        logger = getLogger(modulename=True)
        logger.error("Ganga requires Python version %s or greater" %
                     str(minVersion))
        logger.error("Currently using Python %s" % sys.version.split()[0])
        status = False

    return status


# Override the default guessed platform and external dir
# save the first update request to detect conflicts generated by
# two Ganga packages setting a different platform
_new_platform = None


def setPlatform(platform):
    """
    Override globally the auto-detected platform string
    """
    global _new_platform

    if _new_platform and _new_platform != platform:
        raise RuntimeError(
            "Ganga platform has been already set to: %s" % str(_new_platform))
    # change the global platform used in bootstrap
    import Ganga
    if GangaCore.PACKAGE._defaultPlatform != platform:
        from GangaCore.Utility.logging import getLogger
        logger = getLogger(modulename=True)
        logger.info('The platform identification string (%s) used to resolve Ganga dependencies has been explicitly set to: %s.'
                    % (GangaCore.PACKAGE._defaultPlatform, platform))
    GangaCore.PACKAGE._defaultPlatform = _new_platform = platform


def getPlatform():
    """
    Returns the current platform string set for Ganga
    """
    from GangaCore.PACKAGE import _defaultPlatform
    return _defaultPlatform


def setExternalHome(externalHome):
    """
    Set the dir hosting the external packages
    """
    import Ganga
    GangaCore.PACKAGE._defaultExternalHome = externalHome


def getExternalHome():
    """
    Returns the current external home directory set for Ganga
    """
    from GangaCore.PACKAGE import _defaultExternalHome
    return _defaultExternalHome
