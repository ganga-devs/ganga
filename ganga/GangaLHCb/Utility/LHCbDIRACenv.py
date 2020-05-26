import json
import os
from os.path import join, expanduser
from optparse import OptionValueError
from distutils.version import LooseVersion

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
    # by default the version is 'prod', and in this case we need to resolve the actual version
    # if a specific version is requested, then we can simply try to determine its environment

    # this returns a list, like ['prod'] or ['v', 9, 'r', 3, 'p', 19]
    lbVersion = LooseVersion(requestedVersion).version
    try:
        # we check if a real version (e.g. v9r2, or v9r3p13 or v10r12-pre9) is requested
        if isinstance(lbVersion[1], int) and isinstance(lbVersion[3], int):
            logger.warn("Specific version is requested (%s), please consider removing it!", requestedVersion)
            diracversion = requestedVersion
        else:
            diracversion = 'prod'
    except IndexError:  # here we assume 'prod', and honestly nothing else should be here.
        diracversion = 'prod'

    fdir = join(expanduser("~/.cache/Ganga/GangaLHCb"), platform)
    fname = join(fdir, diracversion)

    cmd = (
        '. /cvmfs/lhcb.cern.ch/lib/LbEnv &>/dev/null && '
        f'lb-dirac {diracversion} python -c "import json, os; print(json.dumps(dict(os.environ)))"'
    )
    env = execute(cmd, env={"PATH": '/usr/bin:/bin', "HOME": os.environ.get("HOME")})

    if isinstance(env, str):
        try:
            env = json.loads(env)
        except Exception:
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
