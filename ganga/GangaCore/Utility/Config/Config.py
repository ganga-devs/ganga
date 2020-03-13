"""
A simple configuration interface for Ganga packages.

1) Overview

PackageConfig object corresponds to a configuration section in the INI file.
Typically each plugin handler has its own section. Additionally Ganga provides
a somewhat arbitrary number of other configuration packages.

Configuration of the package is done in several phases:
 - phase one: developer defines the hardcoded values in his package (default level);
              addOption
 - phase two: at startup ganga reads config files and set options (session level);
              setSessionValue() is used
 - phase three: at runtime user may modify options via GPI (user level);
              setUserValue() is used for setting, getEffectiveOption() for getting


2) Defining default options for your package:

#MyPackage.py#

import GangaCore.Utility.Config

config = GangaCore.Utility.Config.getConfig('MyPackage')

config.addOption('opt1','x','this is option 1')
config.addOption('opt2',3.0, 'this is option 2')

The  default  values of  the  options  may  be strings,  numbers,other
built-in  types  or  any  GPI  objects names  defined  in  the  global
config_scope dictionary (in this module).

IMPORTANT: choose the type of the default value carefully: session and
user options  will be automatically  converted to the type  implied by
the default value (you may  also override the type checking default in
setDefaultOption)  .   The  conversion   is  done  as  following  (for
setSessionValue and setUserValue):

 - nothing is done (value is assigned 'as-is'):
    - if the default type is a string and the assigned value is a string
    - if there is no default value

 - eval the string value

 - check if the type matches the default type and raise ConfigError in case of mismatch
    - unless the default type is None
 
Typically strings are assigned via setSessionValue() as they are read
from the config file or command line.

Note for bootstrap procedure: it is OK to first set the session option
and  then  to  set  it's  default  value. That  is  to  say  that  the
configuration may  be read from the  config file prior  to loading the
corresponding module which  uses it. It is NOT OK  to set user options
before the end of the bootstrap procedure.


3) Getting effective values and using callback handlers

To get the effective value of an option you may use:

print(config['opt1'])
print(config.getEffectiveOption('opt1'))
print(config.getEffectiveOptions()) # all options

The  effective value  takes  into account  default,  session and  user
settings  and  may change  at  runtime.  In  GPI  users  only get  the
effective values and may only set values at the user level.

You  may also attach  the callback  handlers at  the session  and user
level.     Pre-process   handlers   may    modify   what    has   been
set. Post-process handlers may be used to trigger extra actions.

def pre(opt,val):
    print('always setting a square of the value')
    return val*2

def post(opt,val):
    print('effectively set',val)
    
config.attachUserHandler(pre,post)

4) How does user see all this in GPI ?

There is a GPI wrapper in GangaCore.GPIDev.Lib.Config which:
     - internally uses setUserValue and getEffectiveOption
     - has a more appealing interface


"""

import os
import re
import traceback
from collections import defaultdict
from functools import reduce

from GangaCore.Core.exceptions import GangaException


class ConfigError(GangaException):

    """ ConfigError indicates that an option does not exist or it cannot be set.
    """

    __slots__=('what',)

    def __init__(self, what=''):
        super(ConfigError, self).__init__()
        self.what = what

    def __str__(self):
        return "ConfigError: %s " % self.what

# WARNING: avoid importing logging at the module level in this file
# for example, do not do here: from GangaCore.Utility.logging import logger
# use getLogger() function defined below:

_logger = None


def getLogger():
    import GangaCore.Utility.logging
    global _logger
    if _logger is not None:
        return _logger

    # for the configuration of the logging package itself (in the initial
    # phases) the logging may be disabled
    try:
        _logger = GangaCore.Utility.logging.getLogger()
        return _logger
    except AttributeError as err:
        print("AttributeError: %s" % err)
        # in such a case we return a mock proxy object which ignore all calls
        # such as logger.info()...

        class X(object):

            def __getattr__(self, name):
                def f(*args, **kwds):
                    pass
                return f
        return X()


# All configuration units
allConfigs = {}

# configuration session buffer
# this dictionary contains the value for the options which are defined in the configuration file and which have
# not yet been defined
unknownConfigFileValues = defaultdict(dict)
unknownGangarcFileValues = defaultdict(dict)
unknownUserConfigValues = defaultdict(dict)

def getConfig(name):
    """
    Get an existing PackageConfig.
    Principle is the same as for getLogger() -- the config instances may
    be easily shared between different parts of the program.

    Args:
        name: the name of the config section

    Returns:
        PackageConfig:

    Raises:
        KeyError: if the config is not found
    """

    try:
        return allConfigs[name]
    except KeyError:
        raise KeyError('Config section "[{0}]" not found'.format(name))


def makeConfig(name, docstring, **kwds):
    """
    Create a config package and attach metadata to it. makeConfig() should be called once for each package.
    """

    if _after_bootstrap:
        raise ConfigError('attempt to create a configuration section [%s] after bootstrap' % name)

    try:
        c = allConfigs[name]
        c.docstring = docstring
        for k in kwds:
            setattr(c, k, kwds[k])
    except KeyError:
        c = allConfigs[name] = PackageConfig(name, docstring, **kwds)

    c._config_made = True
    return c

def makeConfigForTest(name, docstring, **kwds):
    """
    Copy of makeConfig(), but without the bootstrap restrictions as Config Sections created for the Tests
    can be added after bootstrap and should not contain any critical config.
    """

    try:
        c = allConfigs[name]
        c.docstring = docstring
        for k in kwds:
            setattr(c, k, kwds[k])
    except KeyError:
        c = allConfigs[name] = PackageConfig(name, docstring, **kwds)

    c._config_made = True
    return c


class ConfigOption(object):

    """ Configuration Option has a name, default value and a docstring.

    Metainformation:
       * type - if not specified, then the type is inferred from the default value, type may be a type object such as type(1), StringType, type(None) or a list
       of such objects - in this case any type in the list is accepted
       * examples - example how to use the option
       * hidden - True => do not show the option at the level of GPI proxy (default False)
       * cfile - False => do not put the option in the generated config file (default True)
       * filter - None => filter the option value when set (session and user levels)
       * typelist - None => a typelist as in GPI schema 
    The configuration option may also define the session_value and default_value. The value property gives the effective value.
    """

    __slots__ = ('name', 'hidden', 'cfile', 'examples', 'filter', 'typelist', 'hasModified', 'default_value', 'docstring', 'type', 'user_value', 'session_value', 'gangarc_value')

    def __init__(self, name):
        self.name = name
        self.hidden = False
        self.cfile = True
        self.examples = None
        self.filter = None
        self.typelist = None
        self.hasModified = False

    def defineOption(self, default_value, docstring, typelist=None, **meta):

        self.default_value = default_value
        self.docstring = docstring
        self.hidden = False
        self.cfile = True
        self.examples = None
        self.filter = None
        self.typelist = typelist

        for m in meta:
            setattr(self, m, meta[m])

        self.convert_type('session_value')
        self.convert_type('gangarc_value')
        self.convert_type('user_value')

    def setSessionValue(self, session_value):
        if not hasattr(self, 'docstring'):
            raise ConfigError('Can\'t set a session value without a docstring!')

        self.hasModified = True

        # try:
        if self.filter:
            session_value = self.filter(self, session_value)

        if hasattr(self, 'session_value'):
            session_value = self.transform_PATH_option(session_value, self.session_value)

        self.session_value = session_value
        self.convert_type('session_value')

    def setUserValue(self, user_value):
        if not hasattr(self, 'docstring'):
            raise ConfigError('Can\'t set a user value without a docstring!')

        self.hasModified = True
        try:
            if self.filter:
                user_value = self.filter(self, user_value)
        except Exception as x:
            logger = getLogger()
            logger.warning('problem with option filter: %s: %s', self.name, x)

        if hasattr(self, 'user_value'):
            user_value = self.transform_PATH_option(user_value, self.user_value)

        if hasattr(self, 'user_value'):
            old_value = self.user_value

        self.user_value = user_value
        try:
            self.convert_type('user_value')
        except Exception as x:
            # rollback if conversion failed
            try:
                self.user_value = old_value
            except NameError:
                del self.user_value
            raise x

    def setGangarcValue(self, gangarc_value):
        if not hasattr(self, 'docstring'):
            raise ConfigError('Can\'t set a gangarc value without a docstring!')

        self.hasModified = True
        try:
            if self.filter:
                gangarc_value = self.filter(self, gangarc_value)
        except Exception as x:
            logger = getLogger()
            logger.warning('problem with option filter: %s: %s', self.name, x)

        if hasattr(self, 'gangarc_value'):
            gangarc_value = self.transform_PATH_option(gangarc_value, self.gangarc_value)

        if hasattr(self, 'gangarc_value'):
            old_value = self.gangarc_value

        self.gangarc_value = gangarc_value
        try:
            self.convert_type('gangarc_value')
        except Exception as x:
            # rollback if conversion failed
            try:
                self.gangarc_value = old_value
            except NameError:
                del self.gangarc_value
            raise x

    def overrideDefaultValue(self, default_value):
        self.hasModified = True
        if hasattr(self, 'default_value'):
            default_value = self.transform_PATH_option(default_value, self.default_value)
        self.default_value = default_value
        self.convert_type('user_value')
        self.convert_type('gangarc_value')
        self.convert_type('session_value')

    def __getattr__(self, name):

        if name == 'value':
            if self.name.endswith('_PATH'):
                values = []

                for n in ['user', 'gangarc', 'session', 'default']:
                    str_val = n+'_value'
                    if hasattr(self, str_val):
                        values.append(getattr(self, str_val))

                if values:
                    returnable = reduce(self.transform_PATH_option, values)
                    return returnable
            else:
                for n in ['user', 'gangarc', 'session', 'default']:
                    str_val = n+'_value'
                    if hasattr(self, str_val):
                        return getattr(self, str_val)

        elif name == 'level':

            for level, name in [(0, 'user'), (1, 'gangarc'), (2, 'session'), (3, 'default')]:
                if hasattr(self, name + '_value'):
                    return level

        raise AttributeError(name)

    def __setattr__(self, name, value):
        if name in ['value', 'level']:
            raise AttributeError('Cannot set "%s" attribute of the option object' % name)

        super(ConfigOption, self).__setattr__(name, value)
        super(ConfigOption, self).__setattr__('hasModified', True)

    def check_defined(self):
        return hasattr(self, 'default_value')

    def transform_PATH_option(self, new_value, current_value):
        return transform_PATH_option(self.name, new_value, current_value)

    def convert_type(self, x_name):
        """ Convert the type of session_value or user_value (referred to by x_name) according to the types defined by the self.
        If the option has not been defined or the x_name in question is not defined, then this method is no-op.
        If conversion cannot be performed (type mismatch) then raise ConfigError.
        """

        try:
            value = getattr(self, x_name)
        except AttributeError:
            return

        logger = getLogger()

        # calculate the cast type, if it cannot be done then the option has not been yet defined (setDefaultValue)
        # in this case do not modify the value
        if self.typelist is not None:
            # in this case cast_type is a list of string dotnames (like for
            # typelist property in schemas)
            cast_type = self.typelist
        else:
            try:
                # in this case cast_type is a single type object
                cast_type = type(self.default_value)
            except AttributeError:
                return

        new_value = value

        optdesc = 'while setting option [.]%s = %s ' % (self.name, value)

        # eval string values only if the cast_type is not exactly a string
        if isinstance(value, str) and cast_type is not str:
            try:
                new_value = eval(value, config_scope)
                logger.debug('applied eval(%s) -> %s (%s)', value, new_value, optdesc)
            except Exception as x:
                logger.debug('ignored failed eval(%s): %s (%s)', value, x, optdesc)

        # check the type of the value unless the cast_type is not NoneType
        logger.debug('checking value type: %s (%s)', cast_type, optdesc)

        def check_type(x, t):
            return isinstance(x, t) or x is t

        # first we check using the same rules for typelist as for the GPI proxy
        # objects
        try:
            import GangaCore.GPIDev.TypeCheck
            type_matched = GangaCore.GPIDev.TypeCheck._valueTypeAllowed(new_value, cast_type, logger)
        except TypeError:  # cast_type is not a list
            type_matched = check_type(new_value, cast_type)

        from GangaCore.Utility.logic import implies
        if not implies(not cast_type is type(None), type_matched):
            raise ConfigError('type mismatch: expected %s got %s (%s)' % (cast_type, type(new_value), optdesc))

        setattr(self, x_name, new_value)

# indicate if the GPI proxies for the configuration have been created
_after_bootstrap = False

# Scope used by eval when reading-in the configuration.
# Symbols defined in this scope will be correctly evaluated. For example, File class adds itself here.
# This dictionary may also be used by other parts of the system, e.g. XML
# repository.
config_scope = {}


class PackageConfig(object):

    """ Package  Config object  represents a  Configuration  Unit (typically
    related to Ganga Packages). It should not be created directly
    but only via the getConfig method.

    PackageConfig has a name which  corresponds to the [name] section in
    the .gangarc  file.  Once initialized the configuration  may only be
    modified by special setUserValues  methods. This will give a chance
    to Ganga  to take  further actions such  as automatic update  of the
    .gangarc file.

    The PackageConfig interface is designed for Ganga Package Developers.
    User oriented interface is available via the GPI.

    meta keywords:
      - is_open : True => new options may be added by the users (default False)
      - cfile : True => section will be generated in the config file
      - hidden: True => section is not visible in the GPI

    """

    __slots__ = ('name', 'options', 'docstring', 'hidden', 'cfile', '_user_handlers', '_session_handlers', 'is_open', '_config_made', 'hasModified', '__dict__')

    def __init__(self, name, docstring, **meta):
        """ Arguments:
         - name may not contain blanks and should be a valid python identifier otherwise ValueError is raised
         meta args have the same meaning as for the ConfigOption:
          - hidden
          - cfile
        """
        self.name = name
        self.options = {}  # {'name':Option('name',default_value,docstring)}
        self.docstring = docstring
        self.hidden = False
        self.cfile = True

        # processing handlers
        self._user_handlers = []
        self._session_handlers = []
        self._gangarc_handlers = []

        self.is_open = False

        for m in meta:
            setattr(self, m, meta[m])

        # sanity check to force using makeConfig()
        self._config_made = False

        self.hasModified = False

    def _addOpenOption(self, name, value):
        self.addOption(name, value, "", override=True)

    def __iter__(self):
        """  Iterate over the effective options. """
        # return self.getEffectiveOptions().__iter__()
        return self.options.__iter__()

    def __getitem__(self, o):
        """ Get the effective value of option o. """
        return self.getEffectiveOption(o)

    def addOption(self, name, default_value, docstring, override=False, typelist=None, **meta):
        """
        Add a new option to the configuration.
        """
        if _after_bootstrap and not self.is_open:
            raise ConfigError('attempt to add a new option [%s]%s after bootstrap' % (self.name, name))

        # has the option already been made
        try:
            option = self.options[name]
        except KeyError:
            option = ConfigOption(name)

        if option.check_defined() and not override:
            logger = getLogger()
            logger.warning('attempt to add again the option [%s]%s (ignored)', self.name, name)
            return

        option.defineOption(default_value, docstring, typelist, **meta)
        self.options[option.name] = option

        # is it in the list of unknown options from the standard config files
        try:
            conf_value = unknownConfigFileValues[self.name]
        except KeyError:
            msg = "Error getting ConfigFileValue Option: %s" % self.name
            if locals().get('logger') is not None:
                locals().get('logger').debug("dbg: %s" % msg)
            conf_value = dict()

        if option.name in conf_value:
            session_value = conf_value[option.name]
            try:
                option.setSessionValue(session_value)
                del conf_value[option.name]
            except Exception as err:
                msg = "Error Setting Session Value: %s" % err
                if locals().get('logger') is not None:
                    locals().get('logger').debug("dbg: %s" % msg)
        # is it in the list of unknown options specified by the user at the command line or gangarc
        try:
            conf_value = unknownUserConfigValues[self.name]
        except KeyError:
            msg = "Error getting User Option: %s" % self.name
            if locals().get('logger') is not None:
                locals().get('logger').debug("dbg: %s" % msg)
            conf_value = dict()

        if option.name in conf_value:
            user_value = conf_value[option.name]
            try:
                option.setUserValue(user_value) 
                del conf_value[option.name]
            except Exception as err:
                msg = "Error Setting User Value: %s" % err
                if locals().get('logger') is not None:
                    locals().get('logger').debug("dbg: %s" % msg)

        try:
            conf_value = unknownGangarcFileValues[self.name]
        except KeyError:
            msg = "Error getting gangarc Option: %s" % self.name
            if locals().get('logger') is not None:
                locals().get('logger').debug("dbg: %s" % msg)
            conf_value = dict()

        if option.name in conf_value:
            gangarc_value = conf_value[option.name]
            try:
                option.setGangarcValue(gangarc_value) 
                del conf_value[option.name]
            except Exception as err:
                msg = "Error Setting Gangarc Value: %s" % err
                if locals().get('logger') is not None:
                    locals().get('logger').debug("dbg: %s" % msg)


    def setSessionValue(self, name, value):
        """  Add or  override options  as a  part of  second  phase of
        initialization of  this configuration module (PHASE  2) If the
        default type of the option  is not string, then the expression
        will be evaluated."""

        self.hasModified = True

        logger = getLogger()

        logger.debug('trying to set session option [%s]%s = %s', self.name, name, value)

        for h in self._session_handlers:
            value = h[0](name, value)

        try:
            this_opt = self.options[name]
        except KeyError:
            self.options[name] = ConfigOption(name)
            this_opt = self.options[name]

        this_opt.setSessionValue(value)

        logger.debug('sucessfully set session option [%s]%s = %s', self.name, name, value)

        for h in self._session_handlers:
            try:
                h[1](name, value)
            except Exception as err:
                traceback.print_stack()
                logger.error("h[1]: %s" % h[1])
                logger.error("Error in Setting Session Value!")
                logger.error("Name: %s Value: '%s'" % (name, value))
                logger.error("Err:\n%s" % err)
                raise err

    def setUserValue(self, name, value):
        """ Modify option  at runtime. This  method corresponds to  the user
        action so  the value of  the option is considered  'modified' If
        the  default  type  of  the  option  is  not  string,  then  the
        expression will be evaluated. """

        self.hasModified = True

        logger = getLogger()

        logger.debug('trying to set user option [%s]%s = %s', self.name, name, value)

        for handler in self._user_handlers:
            value = handler[0](name, value)

        self.options[name].setUserValue(value)

        logger.debug('successfully set user option [%s]%s = %s', self.name, name, value)

        for handler in self._user_handlers:
            handler[1](name, value)

    def setGangarcValue(self, name, value):
        """ Modify option  at runtime. This  method corresponds to  the gangarc
        action so  the value of  the option is considered  'modified' If
        the  default  type  of  the  option  is  not  string,  then  the
        expression will be evaluated. """

        self.hasModified = True

        logger = getLogger()

        logger.debug('trying to set user option [%s]%s = %s', self.name, name, value)

        for handler in self._user_handlers:
            value = handler[0](name, value)

        self.options[name].setGangarcValue(value)

        logger.debug('successfully set user option [%s]%s = %s', self.name, name, value)

        for handler in self._gangarc_handlers:
            handler[1](name, value)

    def overrideDefaultValue(self, name, val):
        self.hasModified = True
        self.options[name].overrideDefaultValue(val)

    def revertToSession(self, name):
        self.hasModified = True
        try:
            if hasattr(self.options[name], 'user_value'):
                del self.options[name].user_value
        except KeyError:
            pass

    def revertToDefault(self, name):
        self.hasModified = True
        self.revertToSession(name)
        try:
            if hasattr(self.options[name], 'session_value'):
                del self.options[name].session_value
        except KeyError:
            pass

    def revertToSessionOptions(self):
        self.hasModified = True
        for name in self.options:
            self.revertToSession(name)

    def revertToDefaultOptions(self):
        self.hasModified = True
        self.revertToSessionOptions()
        for name in self.options:
            self.revertToDefault(name)

    def getEffectiveOptions(self):
        eff = {}
        for name in self.options:
            eff[name] = self.options[name].value
        return eff

    def getEffectiveOption(self, name):
        try:
            return self.options[name].value
        except KeyError:
            raise ConfigError('option "%s" does not exist in "%s"' % (name, self.name))

    def getEffectiveLevel(self, name):
        """ Return 0 if option is effectively set at the user level, 1
        if at session level or 2 if at default level """
        try:
            return self.options[name].level
        except KeyError:
            raise ConfigError('option "%s" does not exist in "%s"' % (name, self.name))

    def attachUserHandler(self, pre, post):
        """ Attach a user handler:
        - pre(name,x) will be always called before setUserValue(name,x)
        - post(name,x2) will be always called after setUserValue(name,x)

        pre() acts as a filter for the value of the option: its return value (x2) will be set
        Before setting the value will be evaluated (unless a default value type is a string)
        post() will get x2 as the option value.

        It is OK to give None for pre or post. For example:
           config.attachUserHandler(None,post) attaches only the post handler. """

        if pre is None:
            pre = lambda opt, val: val
        if post is None:
            post = lambda opt, val: None

        self._user_handlers.append((pre, post))

    def attachSessionHandler(self, pre, post):
        """See attachUserHandler(). """
        # FIXME: this will NOT always work and should be redesigned, see
        # ConfigOption.filter
        if pre is None:
            pre = lambda opt, val: val
        if post is None:
            post = lambda opt, val: None

        self._session_handlers.append((pre, post))


    def attachGangarcHandler(self, pre, post):
        """See attachUserHandler(). """
        # FIXME: this will NOT always work and should be redesigned, see
        # ConfigOption.filter
        if pre is None:
            pre = lambda opt, val: val
        if post is None:
            post = lambda opt, val: None

        self._gangarc_handlers.append((pre, post))

    def deleteUndefinedOptions(self):
        for o in self.options.keys():
            if not self.options[o].check_defined():
                del self.options[o]

try:
    import configparser
    GangaConfigParser = configparser.ConfigParser
except ImportError:
    # For Python 3
    import configparser as ConfigParser
    GangaConfigParser = configparser.ConfigParser


def make_config_parser(system_vars):
    cfg = GangaConfigParser()
    cfg.optionxform = str  # case sensitive
    cfg.defaults().update(system_vars)
    return cfg


def transform_PATH_option(name, new_value, current_value):
    """
    Return the new value of the option 'name' taking into account special rules for PATH-like variables:

       A variable is PATH-like if the name ends in _PATH.
       Return 'new_value:current_value' unless new_value starts in ':::' or current_value is None.
       In that case return new_value.

       Example:
       if a name of a option terminates in _PATH then the value will not be overriden but
       appended:
       file1.ini:
         ANY_PATH = x
       file2.ini:
         ANY_PATH = y

       result of the merge is: ANY_PATH = x:y

       If you want to override the path you should use :::path, for example:
       file1.ini:
         ANY_PATH = x
       file2.ini
         ANY_PATH = :::y
       result of the merge is: ANY_PATH = y

    For other variables just return the new_value.
    """

    PATH_ITEM = '_PATH'
    if name[-len(PATH_ITEM):] == PATH_ITEM:
        logger = getLogger()
        logger.debug('PATH-like variable: %s %s %s', name, new_value, current_value)
        if current_value is None:
            ret_value = new_value
        elif new_value[:3] != ':::':
            logger.debug('Prepended %s to PATH-like variable %s', new_value, name)
            ret_value = new_value + ':' + current_value
            new_value = ""
        else:
            logger.debug('Resetting PATH-like variable %s to %s', name, new_value)
            ret_value = new_value  # [3:]
            new_value = ":::"

        for tok in ret_value.strip(":").split(":"):
            if new_value.find(tok) == -1 or tok == "":
                new_value += "%s:" % tok

    return new_value


def read_ini_files(filenames, system_vars):
    """ Return  a ConfigParser object  which contains  all options  from the
    sequence of files (which are parsed from left-to-right).
    Apply special rules for PATH-like variables - see transform_PATH_option() """

    import GangaCore.Utility.logging

    logger = getLogger()

    logger.debug('reading ini files: %s', filenames)

    main = make_config_parser(system_vars)

    # load all config files and apply special rules for PATH-like variables
    # note: main.read(filenames) cannot be used because of that

    if isinstance(filenames, str):
        filenames = [filenames]

    for f in filenames:
        if f is None or f == '':
            continue
        cc = make_config_parser(system_vars)
        logger.info('reading config file %s', f)
        try:
            with open(f) as file_f:
                cc.read_file(file_f)
        except Exception as x:
            logger.warning('Exception reading config file %s', x)

        for sec in cc.sections():
            if not main.has_section(sec):
                main.add_section(sec)

            for name in cc.options(sec):
                try:
                    value = cc.get(sec, name)
                except (configparser.InterpolationMissingOptionError, configparser.InterpolationSyntaxError) as err:
                    logger.debug("Parse Error!:\n  %s" % err)
                    value = cc.get(sec, name, raw=True)
                    #raise err

                for localvar in re.finditer('\$\{[^${}]*\}', value):
                    localvarstripped = re.sub(r'[^\w]', '', localvar.group(0))
                    try:
                        value = value.replace(localvar.group(0), cc.get(sec, localvarstripped))
                    except Exception as err:
                        GangaCore.Utility.logging.log_unknown_exception()
                        logger.debug('The variable \"' + localvarstripped + '\" is referenced but not defined in the ')
                        logger.debug('[' + sec + '] configuration section of ' + f)
                        logger.debug("err: %s" % err)

                # do not put the DEFAULTS into the sections (no need)
                if name in cc.defaults():
                    continue

                # special rules (NOT APPLIED IN DEFAULT SECTION):

                try:
                    current_value = main.get(sec, name)
                except configparser.NoOptionError:
                    current_value = None
                except (configparser.InterpolationMissingOptionError, configparser.InterpolationSyntaxError) as err:
                    logger.debug("Parse Error!:\n  %s" % err)
                    logger.debug("Failed to expand, Importing value %s:%s as raw" % (sec, name))
                    current_value = main.get(sec, name, raw=True)
                    current_value = current_value.replace('%', '%%')
                    #raise err

                value = transform_PATH_option(name, value, current_value)

                from GangaCore.Utility.Config import expandgangasystemvars
                value = expandgangasystemvars(None, value)
                # check for the use of environment vars
                re.search('\$\{[^${}]*\}', value)     # matches on ${...}
                for envvar in re.finditer('\$\$[^${}]*\$\$', value):
                    # yeah, if the same variable appears more than once, we'll look it up in the
                    # environment more than once too...but that's not too
                    # arduous.
                    envvar = envvar.group(0)
                    envvarclean = envvar.strip('$')

                    # is env variable
                    logger.debug('looking for ' + str(envvarclean) + ' in the shell environment')
                    if envvarclean in os.environ:
                        envval = os.environ[envvarclean]
                        logger.debug(str(envvarclean) + ' is set as ' + envval + ' in the shell environment')
                        value = value.replace(envvar, envval)
                    else:
                        logger.debug('The configuration file ' + f + ' references an unset environment variable: ' + str(envvarclean))

                # FIXME: strip trailing whitespaces -- SHOULD BE DONE BEFORE IF
                # AT ALL?
                value = value.rstrip()
                value = value.replace('%', '%%')
                try:
                    main.set(sec, name, value)
                except Exception as err:
                    value = value.replace('%', '%%')
                    logger.debug("Error Setting %s" % err)
                    try:
                        main.set(sec, name, value)
                    except Exception as err2:
                        logger.debug("Error setting #2: %s" % err2)
                        raise err

    return main


def setUserValue(config_name, option_name, value):
    """
    Sets the user value for the given config and option.
    If the given config has not already been set the it is added
    to the dict to be added later. The user values supersede the
    session values so are the command line or gangarc options.
    """
    if config_name in allConfigs:
        c = getConfig(config_name)
        if option_name in c.options:
            c.setUserValue(option_name, value)
            return
        if c.is_open:
            c._addOpenOption(option_name, value)
            c.setUserValue(option_name, value)
            return
    # put value in the buffer, it will be removed from the buffer when option
    # is added
    unknownUserConfigValues[config_name][option_name] = value

def setUserValueForTest(config_name, option_name, value):
    """
    Sets the user value for the given config and option for performing Tests.
    Used by (GangaCore/testlib/GangaUnitTest)
    If config_name exists & it's option_name also exists then modifies the option.
    If config_name exists & it's option_name does not exists but options are allowed to be added to config_name (.is_open=True) then adds the option.
    If config_name exists & it's option_name does not exists but options are not allowed to be added to config_name (.is_open=False) then doesn't add the option.
    If config_name does not exists then creates the config_name (with .is_open=True) and add option_name to it. <--- Created for testing purpose
    """

    if config_name in allConfigs:
        c = getConfig(config_name)
        if option_name in c.options:
            c.setUserValue(option_name, value)
            return
        if c.is_open:
            c._addOpenOption(option_name, value)
            c.setUserValue(option_name, value)
            return

    if config_name not in allConfigs:
        c = makeConfigForTest(config_name, '')
        c.is_open = True
        c._addOpenOption(option_name, value)
        c.setUserValue(option_name, value)


def setSessionValue(config_name, option_name, value):
    """
    Sets the session value for the given config and option.
    If the given config has not been set already then it is added
    to the dict to be added later. The session value is superseded by
    the user value.
    """
    if config_name in allConfigs:
        c = getConfig(config_name)
        if option_name in c.options:
            c.setSessionValue(option_name, value)
            return
        if c.is_open:
            c._addOpenOption(option_name, value)
            c.setSessionValue(option_name, value)
            return

    # put value in the buffer, it will be removed from the buffer when option
    # is added
    unknownConfigFileValues[config_name][option_name] = value

def setGangarcValue(config_name, option_name, value):
    """
    Sets the gangarc value for the given config and option.
    If the given config has not been set already then it is added
    to the dict to be added later. The session value is superseded by
    the user value.
    """
    if config_name in allConfigs:
        c = getConfig(config_name)
        if option_name in c.options:
            c.setGangarcValue(option_name, value)
            return
        if c.is_open:
            c._addOpenOption(option_name, value)
            c.setGangarcValue(option_name, value)
            return

    # put value in the buffer, it will be removed from the buffer when option
    # is added
    unknownGangarcFileValues[config_name][option_name] = value

def setSessionValuesFromFiles(filenames, system_vars):
    """ Sets session values for all options in all configuration units
    defined in the sequence of config files.  Initialize config parser
    object with system variables (such as GANGA_TOP, GANGA_VERSION and
    alike).  Contrary to standard  config parser behaviour the options
    from the  DEFAULTS section  are not visible  in the  config units.

    At the time of reading the initialization files, the default options in
    the configuration options (default values) may have not yet been defined.
    """
    gangarcFile = []
    for filename in filenames:
        if 'gangarc' in filename:
            gangarcFile.append(filename)
            filenames.remove(filename)

    #First take the gangarc values
    grcCfg = read_ini_files(gangarcFile, system_vars)

    for name in grcCfg.sections():
        for o in grcCfg.options(name):
            # Important: do not put the options from the DEFAULTS section into
            # the configuration units!
            if o in grcCfg.defaults():
                continue
            try:
                v = grcCfg.get(name, o)
            except (configparser.InterpolationMissingOptionError, configparser.InterpolationSyntaxError) as err:
                logger = getLogger()
                logger.debug("Parse Error!:\n  %s" % err)
                logger.warning("Can't expand the config file option %s:%s, treating it as raw" % (name, o))
                v = grcCfg.get(name, o, raw=True)
            setGangarcValue(name, o, v)


    #Now the others

    cfg = read_ini_files(filenames, system_vars)

    for name in cfg.sections():
        for o in cfg.options(name):
            # Important: do not put the options from the DEFAULTS section into
            # the configuration units!
            if o in cfg.defaults():
                continue
            try:
                v = cfg.get(name, o)
            except (configparser.InterpolationMissingOptionError, configparser.InterpolationSyntaxError) as err:
                logger = getLogger()
                logger.debug("Parse Error!:\n  %s" % err)
                logger.warning("Can't expand the config file option %s:%s, treating it as raw" % (name, o))
                v = cfg.get(name, o, raw=True)
            setSessionValue(name, o, v)


def load_user_config(filename, system_vars):
    logger = getLogger()
    if not os.path.exists(filename):
        return
    new_cfg = read_ini_files(filename, system_vars)
    for name in new_cfg.sections():
        try:
            current_cfg_section = getConfig(name)
        except KeyError:
            continue

        if not current_cfg_section.options:  # if this section does not exists
            # supressing these messages as depending on what stage of the bootstrap.py you
            # call the function more or less of the default options have been loaded
            # currently calling after initialise() could call after bootstrap()
            logger.debug("Section '%s' defined in '%s' is not valid exists and will be removed" % (name, filename))
            continue

        for o in new_cfg.options(name):
            if o not in current_cfg_section.options:
                logger.warning("Option '[%s] %s' defined in '%s' is not valid and will be removed" % (name, o, filename))
                continue
            try:
                v = new_cfg.get(name, o)
            except (configparser.InterpolationMissingOptionError, configparser.InterpolationSyntaxError) as err:
                logger.debug("Parse Error!:\n  %s" % err)
                logger.debug("Failed to expand %s:%s, loading it as raw" % (name, o))
                v = new_cfg.get(name, o, raw=True)
            current_cfg_section.setGangarcValue(o, v)


# KH 050725: Add possibility to overwrite at run-time option set in
#            configuration file
#            => useful for executables where particular actions need
#               to be forced or suppressed
def setConfigOption(section="", item="", value=""):
    """Function to overwrite option values set in configuration file:

       Arguments:
          section - Name of relevant section within configuration file
          item    - Item for which value is to be changed
          value   - Value to be assigned

       Function needs to be called after configuration file has been parsed.

       Return value: None"""

    if bool(section) and bool(item):
        try:
            config = getConfig(section)
            if item in config.getEffectiveOptions():
                config.setSessionValue(item, value)
        except Exception as err:
            getLogger().debug("Error setting Option: %s = %s  :: %s" % (item, value, err))

    return None
# KH 050725: End of addition


def expandConfigPath(path, top):
    """ Split the path and return a list, where all relative path components will have top prepended.
        Example: 'A:/B/C::D/E' -> ['top/A','/B/C','top/D/E']
    """
    l = []
    for p in path.split(':'):
        if p:
            if not os.path.isabs(p):
                p = os.path.join(top, p)
            l.append(p)
    return l


def sanityCheck():
    logger = getLogger()
    for c in allConfigs.values():
        if not c._config_made:
            logger.error("sanity check failed: %s: no makeConfig() found in the code", c.name)

    for name in unknownConfigFileValues:
        opts = unknownConfigFileValues[name]
        if name in allConfigs:
            cfg = allConfigs[name]
        else:
            logger.error("unknown configuration section: [%s]", name)
            continue

        if not cfg.is_open:
            if opts:
                logger.error("unknown options [%s]%s", name, ','.join(list(opts.keys())))
        else:
            # add all options for open sections
            for o, v in zip(list(opts.keys()), list(opts.values())):
                cfg._addOpenOption(o, v)
                cfg.setSessionValue(o, v)
