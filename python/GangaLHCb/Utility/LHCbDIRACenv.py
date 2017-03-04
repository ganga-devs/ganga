import os
from os.path import realpath, basename, join, exists, expanduser, getsize

import Ganga.Utility.Config
from GangaDirac.Lib.Utilities.DiracUtilities import write_env_cache
from Ganga.Utility.execute import execute
from Ganga.Utility.logging import getLogger
from Ganga.Core.exceptions import PluginError

logger = getLogger()

def select_dirac_version(wildcard):
    """
    Find the LHCbDIRAC version that should be used based on the confuguration 
    system. Wildcards can be used and soflinks are dereferenced.
    """
    cmd =  'lb-run -l LHCbDIRAC'
    out = execute(cmd)
    if out=='':
        logger.error("Can't find any LHCbDirac versions")
        raise PluginError

    versions = [s.split() for s in out.splitlines() if fnmatch(s.split()[2],wildcard)]
    if len(versions)==0:
        logger.error("Can't find LHCbDIRAC version matching %s. % wildcard")
        raise PluginError

    version = versions.sort(key = lambda v : v[0])[-1]
    dereferenced_version=basename(realpath(version[2]))
    return dereferenced_version

    
def store_dirac_environment():
    """Store the LHCbDIRAC environment in a cache file."""

    platformEnvVar='CMTCONFIG'
    try:
        platform = os.environ[platformEnvVar]
    except KeyError:
        logger.error("Environment variable %s is missing. Can't cache LHCbDIRAC environment." %platformEnvVar)
        raise PluginError

    wildcard = Ganga.Utility.Config.getConfig('LHCb')['LHCbDiracVersion']
    diracversion = select_dirac_version(wildcard)
    fdir = join(expanduser("~/.cache/Ganga/GangaLHCb"), platform)
    fname = join(fdir, diracversion)
    if not exists(fname) or not getsize(fname):
        cmd =  'lb-run LHCBDIRAC {version} python -c "import os; print(dict(os.environ))"'.format(version=diracversion)
        env = execute(cmd)
        if isinstance(env, str):
            try:
                env_temp = eval(env)
                env = env_temp

            except SyntaxError:
                logger.error("LHCbDirac version {version} does not exist".format(version=diracversion))
                raise OptionValueError("LHCbDirac version {version} does not exist".format(version=diracversion))
        try:
            write_env_cache(env, fname)
            logger.info("Storing new LHCbDirac environment (%s:%s)" % (str(diracversion), str(platform)))
        except (OSError, IOError, TypeError):
            logger.error("Unable to store LHCbDirac environment")
            raise PluginError
    logger.info("Using LHCbDirac version %s", diracversion)
    os.environ['GANGADIRACENVIRONMENT'] = fname
