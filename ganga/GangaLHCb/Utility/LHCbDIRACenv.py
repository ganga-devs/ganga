import json
import os
from os.path import join, expanduser
from optparse import OptionValueError

from GangaDirac.Lib.Utilities.DiracUtilities import write_env_cache
import GangaCore.Utility.Config
from GangaCore.Utility.execute import execute
from GangaCore.Utility.logging import getLogger
from GangaCore.Core.exceptions import PluginError

logger = getLogger()


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

    if not requestedVersion == 'prod':
        logger.warn(f"Specific DIRAC version ({requestedVersion}) is set in the [LHCb]LHCbDiracVersion configuration parameter. Unless you really know what you are doing, this should not be done.")

    fdir = join(expanduser("~/.cache/Ganga/GangaLHCb"), platform)
    fname = join(fdir, requestedVersion)

    cmd = (
        '. /cvmfs/lhcb.cern.ch/lib/LbEnv &>/dev/null && '
        f'lb-dirac {requestedVersion} python -c "import json, os; print(json.dumps(dict(os.environ)))"'
    )
    env = execute(cmd, env={"PATH": '/usr/bin:/bin', "HOME": os.environ.get("HOME")})

    if isinstance(env, str):
        try:
            env = json.loads(env)
        except Exception:
            logger.error(f"LHCbDirac version {requestedVersion} does not exist")
            raise OptionValueError(f"LHCbDirac version {requestedVersion} does not exist")
    try:
        write_env_cache(env, fname)
        logger.debug(f"Storing new LHCbDirac environment ({requestedVersion}:{platform})")
    except (OSError, IOError, TypeError):
        logger.error("Unable to store LHCbDirac environment")
        raise PluginError
    logger.info(f"Using LHCbDirac version {requestedVersion}")
    os.environ['GANGADIRACENVIRONMENT'] = fname
