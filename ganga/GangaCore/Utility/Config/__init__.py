
from .Config import getConfig, makeConfig, ConfigError, setSessionValuesFromFiles, allConfigs, setConfigOption, expandConfigPath, config_scope, setSessionValue, setUserValue, setUserValueForTest
import os.path

## from Config import getConfigDict


def get_unique_port():
    """
    Obtain a free TCP port from the OS
    """
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def get_unique_name():
    """
    Obtain a unique identifier for the user
    """
    import uuid
    return str(uuid.uuid1()).replace("-", "_")


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

    user_config = os.environ.get('GANGA_CONFIG_FILE') or os.path.expanduser('~/.gangarc')
    config_files = GangaProgram.get_config_files(user_config)

    system_vars = {}
    syscfg = getConfig('System')
    for opt in syscfg:
        system_vars[opt] = syscfg[opt]

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
