from __future__ import absolute_import
from .Config import getConfig, makeConfig, ConfigError, setSessionValuesFromFiles, allConfigs, setConfigOption, expandConfigPath, config_scope, setSessionValue
import os.path

## from Config import getConfigDict

# here are some useful option filters


def expandvars(c, v):
    """The ~ and $VARS are automatically expanded. """
    return os.path.expanduser(os.path.expandvars(v))


def expandgangasystemvars(c, v):
    """Expands vars with the syntax '@{VAR}' from the System config item."""
    system = getConfig('System')
    for key in system.options.iterkeys():
        option = '@{%s}' % key
        if option in v:
            v = v.replace(option, system[key])
    return v
