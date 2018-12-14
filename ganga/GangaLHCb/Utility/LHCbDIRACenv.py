import os
from os.path import realpath, basename, join, exists, expanduser, getsize
from optparse import OptionValueError
from distutils.version import LooseVersion

from fnmatch import fnmatch

from GangaDirac.Lib.Utilities.DiracUtilities import write_env_cache
import GangaCore.Utility.Config
from GangaCore.Utility.execute import execute
from GangaCore.Utility.logging import getLogger
from GangaCore.Core.exceptions import PluginError

logger = getLogger()


def select_dirac_version(wildcard):
    """
    Find the LHCbDIRAC version that should be used based on the confuguration
    system. Wildcards can be used and soflinks are dereferenced.
    """
    cmd = 'lb-run -c x86_64-slc6-gcc49-opt -l LHCbDIRAC'
    out = execute(cmd)
    if out == '':
        raise PluginError("Can't find any LHCbDirac versions from '%s'" % cmd)

    versions = [s.split() for s in out.splitlines() if fnmatch(s.split()[0], wildcard)]
    if len(versions) == 0:
        raise PluginError("Can't find LHCbDIRAC version matching %s.", wildcard)

    versions.sort(key=lambda v: v[0])
    version = versions[-1]
    dereferenced_version = basename(realpath(version[2]))[10:]
    return dereferenced_version


def store_dirac_environment():
    """Store the LHCbDIRAC environment in a cache file."""


# Re-enable test case in TestLHCbDiracVersion.py when re-enabled.

#    platform_env_var = 'CMTCONFIG'
#    try:
#        platform = os.environ[platform_env_var]
#    except KeyError:
#        logger.error("Environment variable %s is missing. Can't cache LHCbDIRAC environment.", platform_env_var)
#        raise PluginError
    # While LHCbDirac is only available for gcc49 we shall unfortunately hard-code the platform.
    platform = 'x86_64-slc6-gcc49-opt'

    requestedVersion = GangaCore.Utility.Config.getConfig('LHCb')['LHCbDiracVersion']
    # by default the version is 'prod', and in this case we need to resolve the actual version
    # if a specific version is requested, then we can simply try to determine its environment

    # this returns a list, like ['prod'] or ['v', 9, 'r', 3, 'p', 19]
    lbVersion = LooseVersion(requestedVersion).version
    try:
        # we check if a real version (e.g. v9r2, or v9r3p13 or v10r12-pre9) is requested
        if isinstance(lbVersion[1], int) and isinstance(lbVersion[3], int):
            logger.warn("Specific version is requested (%s), no further check will be done", requestedVersion)
            diracversion = requestedVersion
        else:
            diracversion = select_dirac_version(requestedVersion)
    except IndexError:  # here we assume 'prod', or '*' is requested -- i.e. not a specific version
        diracversion = select_dirac_version(requestedVersion)

    fdir = join(expanduser("~/.cache/Ganga/GangaLHCb"), platform)
    fname = join(fdir, diracversion)
    cmd = 'lb-run -c best LHCBDIRAC/{version} python -c "import os; print(dict(os.environ))"'.format(version=diracversion)
    env = execute(cmd)
    if isinstance(env, str):
        try:
            env_temp = eval(env)
            env = env_temp

        except SyntaxError:
            logger.error("LHCbDirac version %s does not exist", diracversion)
            raise OptionValueError("LHCbDirac version {version} does not exist".format(version=diracversion))
    try:
        write_env_cache(env, fname)
        logger.debug("Storing new LHCbDirac environment (%s:%s)", str(diracversion), str(platform))
    except (OSError, IOError, TypeError):
        logger.error("Unable to store LHCbDirac environment")
        raise PluginError
    logger.info("Using LHCbDirac version %s", diracversion)
    os.environ['GANGADIRACENVIRONMENT'] = fname
