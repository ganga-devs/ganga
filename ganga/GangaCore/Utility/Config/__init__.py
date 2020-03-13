
from .Config import getConfig, makeConfig, ConfigError, setSessionValuesFromFiles, allConfigs, setConfigOption, expandConfigPath, config_scope, setSessionValue, setUserValue, setUserValueForTest
import os.path

## from Config import getConfigDict

# here are some useful option filters


def expandvars(c, v):
    """The ~ and $VARS are automatically expanded. """
    return os.path.expanduser(os.path.expandvars(v))


def expandgangasystemvars(c, v):
    """Expands vars with the syntax '@{VAR}' from the System config item."""
    system = getConfig('System')
    for key in system.options.keys():
        option = '@{%s}' % key
        if option in v:
            v = v.replace(option, system[key])
    return v

def load_config_files():
    """
    Load the config files as a normal Ganga session would, taking
    into account environment variables etc.
    """
    from GangaCore.Utility.Config import getConfig, setSessionValuesFromFiles
    from GangaCore.Runtime import GangaProgram
    system_vars = {}
    for opt in getConfig('System'):
        system_vars[opt] = getConfig('System')[opt]
        user_config = os.environ.get('GANGA_CONFIG_FILE') or os.path.expanduser('~/.gangarc')
        config_files = GangaProgram.get_config_files(user_config)
        setSessionValuesFromFiles(config_files, system_vars)


def clear_config():
    """
    Reset all the configs back to their default values
    """
    from GangaCore.Utility.Config import allConfigs
    for package in allConfigs.values():
        package._user_handlers = []
        package._session_handlers = []
        package.revertToDefaultOptions()

