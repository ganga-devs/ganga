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

import Ganga.Utility.Config

config = Ganga.Utility.Config.getConfig('MyPackage')

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

print config['opt1']
print config.getEffectiveOption('opt1')
print config.getEffectiveOptions() # all options

The  effective value  takes  into account  default,  session and  user
settings  and  may change  at  runtime.  In  GPI  users  only get  the
effective values and may only set values at the user level.

You  may also attach  the callback  handlers at  the session  and user
level.     Pre-process   handlers   may    modify   what    has   been
set. Post-process handlers may be used to trigger extra actions.

def pre(opt,val):
    print 'always setting a square of the value'
    return val*2

def post(opt,val):
    print 'effectively set',val
    
config.attachUserHandler(pre,post)

4) How does user see all this in GPI ?

There is a GPI wrapper in Ganga.GPIDev.Lib.Config which:
     - internally uses setUserValue and getEffectiveOption
     - has a more appealing interface


"""

from Ganga.Core.exceptions import GangaException
class ConfigError(GangaException):
 """ ConfigError indicates that an option does not exist or it cannot be set.
 """
 def __init__(self,what):
     GangaException.__init__(self)
     self.what = what

 def __str__(self):
     return "ConfigError: %s "%(self.what)
    
# WARNING: avoid importing logging at the module level in this file
# for example, do not do here: from Ganga.Utility.logging import logger
# use getLogger() function defined below:

def getLogger():
    # for the configuration of the logging package itself (in the initial phases) the logging may be disabled
    try:
        import  Ganga.Utility.logging
        logger = Ganga.Utility.logging.getLogger()
    except AttributeError:
        # in such a case we return a mock proxy object which ignore all calls such as logger.info()...
        class X:
            def __getattr__(self,name):
                def f(*args,**kwds):
                    pass
                return f
        logger = X()
    return logger

# All configuration units
allConfigs = {}

# configuration session buffer
# this dictionary contains the value for the options which are defined in the configuration file and which have
# not yet been defined
allConfigFileValues = {}

# helper function which helps migrating the names from old naming convention
# to the stricter new one: spaces are removed, colons replaced by underscored
# returns a valid name or raises a ValueError
def _migrate_name(name):
        import Ganga.Utility.strings as strings

        if not strings.is_identifier(name):
            name2 = strings.drop_spaces(name)
            name2 = name2.replace(':','_')

            if not strings.is_identifier(name2):
                raise ValueError('config name %s is not a valid python identifier' % name)
            else:
                logger = getLogger()
                logger.warning('obsolete config name found: replaced "%s" -> "%s"'%(name,name2))
                logger.warning('config names must be python identifiers, please correct your usage in the future ')
                name = name2
        return name


def getConfig(name):
    """ Get an exisiting PackageConfig or create a new one if needed.
    Temporary name migration conversion applies -- see _migrate_name().
    Principle is the same as for getLogger() -- the config instances may
    be easily shared between different parts of the program."""
    
    name = _migrate_name(name)
    try:
        return allConfigs[name]
    except KeyError:
        #print 'getConfig',name
        allConfigs[name] = PackageConfig(name,'Documentation not available')
        return allConfigs[name]

def makeConfig(name,docstring,**kwds):
    """
    Create a config package and attach metadata to it. makeConfig() should be called once for each package.
    """

    if _after_bootstrap:
        raise ConfigError('attempt to create a configuration section [%s] after bootstrap'%name)
    
    name = _migrate_name(name)
    try:
        #print 'makeConfig',name
        c = allConfigs[name]
        c.docstring = docstring
        for k in kwds:
            setattr(c,k,kwds[k])
    except KeyError:
        c = allConfigs[name] = PackageConfig(name,docstring,**kwds)

##     # _after_bootstrap flag solves chicken-egg problem between logging and config modules
##     if _after_bootstrap:
##          # make the GPI proxy
##          from Ganga.GPIDev.Lib.Config.Config import createSectionProxy
##          createSectionProxy(name)
        
    c._config_made = True
    return c
    

class ConfigOption:
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
    def __init__(self,name):
        self.name = name
        
    def defineOption(self,default_value,docstring,**meta):

        #if self.check_defined():
        #    print 'option',self.name,'already defined'
            
        self.default_value = default_value
        self.docstring = docstring
        self.hidden=False
        self.cfile=True
        self.examples = None
        self.filter = None
        self.typelist = None
        
        for m in meta:
            setattr(self,m,meta[m])
        
        self.convert_type('session_value')
        self.convert_type('user_value')
        
    def setSessionValue(self,session_value):
        #try:
        if self.filter:
            session_value = self.filter(self,session_value)
        #except Exception,x:
        #    import  Ganga.Utility.logging
        #    logger = Ganga.Utility.logging.getLogger()
        #    logger.warning('problem with option filter: %s: %s',self.name,x)
                
        #print 'setting session value %s = %s',self.name,str(session_value)
        try:
            session_value = self.transform_PATH_option(session_value,self.session_value)
        except AttributeError:
            pass

        try:
            old_value = self.session_value
        except AttributeError:
            pass
        
        self.session_value = session_value
        try:
            self.convert_type('session_value')
        except Exception,x:
            #rollback if conversion failed
            try:
                self.session_value = old_value
            except NameError:
                del self.session_value
            raise x

    def setUserValue(self,user_value):

        try:
            if self.filter:
                user_value = self.filter(self,user_value)
        except Exception,x:
            logger = getLogger()
            logger.warning('problem with option filter: %s: %s',self.name,x)
        
        try:
            user_value = self.transform_PATH_option(user_value,self.user_value)
        except AttributeError:
            pass

        try:
            old_value = self.user_value
        except AttributeError:
            pass
        
        self.user_value = user_value
        try:
            self.convert_type('user_value')
        except Exception,x:
            #rollback if conversion failed
            try:
                self.user_value = old_value
            except NameError:
                del self.user_value
            raise x

    def overrideDefaultValue(self,default_value):
        try:
            default_value = self.transform_PATH_option(default_value,self.default_value)
        except AttributeError:
            pass
        self.default_value = default_value
        self.convert_type('user_value')
        self.convert_type('session_value')
        
    def __getattr__(self,name):
        if name == 'value':
            values = []

            for n in ['user','session','default']:
                try:
                    values.append(getattr(self,n+'_value'))
                except AttributeError:
                    pass
                
            if values:
                return reduce(self.transform_PATH_option,values)
            
            #for n in ['user','session','default']:
            #    try:
            #        return getattr(self,n+'_value')
            #    except AttributeError:
            #        pass
            
        if name == 'level':

            for level,name in [(0,'user'),(1,'session'),(2,'default')]:
                if hasattr(self,name+'_value'):
                    return level
        raise AttributeError,name

    def __setattr__(self,name,value):
        if name in ['value','level']:
            raise AttributeError('Cannot set "%s" attribute of the option object'%name)
        self.__dict__[name]=value

    def check_defined(self):
        return hasattr(self,'default_value')
    
    def transform_PATH_option(self,new_value,current_value):
        return transform_PATH_option(self.name,new_value,current_value)
    
    def convert_type(self,x_name):
        ''' Convert the type of session_value or user_value (referred to by x_name) according to the types defined by the self.
        If the option has not been defined or the x_name in question is not defined, then this method is no-op.
        If conversion cannot be performed (type mismatch) then raise ConfigError.
        '''
        
        try:
            value = getattr(self,x_name)
        except AttributeError:
            return

        logger = getLogger()

        # calculate the cast type, if it cannot be done then the option has not been yet defined (setDefaultValue)
        # in this case do not modify the value
        if not self.typelist is None:
            cast_type = self.typelist # in this case cast_type is a list of string dotnames (like for typelist property in schemas)
        else:
            try:
                cast_type = type(self.default_value) # in this case cast_type is a single type object
            except AttributeError:
                return
            
        new_value = value

        #optdesc = 'while setting option [%s]%s = %s ' % (self.name,o,str(value))
        optdesc = 'while setting option [.]%s = %s ' % (self.name,str(value))
        
        # eval string values only if the cast_type is not exactly a string
        if type(value) is type('') and not cast_type is type(''):
            try:
                new_value = eval(value,config_scope)
                logger.debug('applied eval(%s) -> %s (%s)',value,new_value,optdesc)
            except Exception,x:
                logger.debug('ignored failed eval(%s): %s (%s)',value,x,optdesc)

        # check the type of the value unless the cast_type is not NoneType
        logger.debug('checking value type: %s (%s)',str(cast_type),optdesc)

        import types
        def check_type(x,t):
            return type(x) is t or x is t
        
        type_matched = False

        # first we check using the same rules for typelist as for the GPI proxy objects
        try:
            import Ganga.GPIDev.TypeCheck
            type_matched = Ganga.GPIDev.TypeCheck._valueTypeAllowed(new_value,cast_type,logger)
        except TypeError: #cast_type is not a list
            type_matched = check_type(new_value,cast_type)
            
        from Ganga.Utility.logic import implies
        if not implies(not cast_type is type(None), type_matched):
            raise ConfigError('type mismatch: expected %s got %s (%s)'%(str(cast_type),str(type(new_value)),optdesc))

        setattr(self,x_name,new_value)

# indicate if the GPI proxies for the configuration have been created
_after_bootstrap = False
    
# Scope used by eval when reading-in the configuration.
# Symbols defined in this scope will be correctly evaluated. For example, File class adds itself here.
config_scope = {}

class PackageConfig:
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
   
    def __init__(self,name,docstring,**meta):
        """ Arguments:
         - name may not contain blanks and should be a valid python identifier otherwise ValueError is raised
         - temporary name migration conversion applies -- see _migrate_name()
         meta args have the same meaning as for the ConfigOption:
          - hidden
          - cfile
        """
        self.name = _migrate_name(name)
        self.options = {} #  {'name':Option('name',default_value,docstring)}
        self.docstring = docstring
        self.hidden = False
        self.cfile = True
        
        # processing handlers
        self._user_handlers = []
        self._session_handlers = []

        self.is_open = False
        
        for m in meta:
            setattr(self,m,meta[m])

        # sanity check to force using makeConfig()
        self._config_made = False

        if _configured and self.is_open:
            print 'cannot define open configuration section %s after configure() step'%self.name
            logger = getLogger()
            logger.error('cannot define open configuration section %s after configure() step',self.name)

    def _addOpenOption(self,name,value):
        self.addOption(name,value,"",override=True)
    
    def __iter__(self):
        """  Iterate over the effective options. """
        #return self.getEffectiveOptions().__iter__()
        return self.options.__iter__()
    
    def __getitem__(self,o):
        """ Get the effective value of option o. """
        return self.getEffectiveOption(o)

    def addOption(self,name,default_value, docstring, override=False, **meta):

        if _after_bootstrap and not self.is_open:
            raise ConfigError('attempt to add a new option [%s]%s after bootstrap'%(self.name,name))
 
        try:
            option = self.options[name]
        except KeyError:
            option = ConfigOption(name)

        if option.check_defined() and not override:
            logger = getLogger()
            logger.warning('attempt to add again the option [%s]%s (ignored)',self.name,name)
            return

        option.defineOption(default_value,docstring,**meta)
        self.options[option.name] = option

        try:
            session_value = allConfigFileValues[self.name][option.name]
            option.setSessionValue(session_value)
            del allConfigFileValues[self.name][option.name]
        except KeyError:
            pass
        

##         # set the GPI proxy object if already created, if not it will be created by bootstrap() function in the GPI Config module
##         if _after_bootstrap:
##             from Ganga.GPIDev.Lib.Config.Config import createOptionProxy
##             createOptionProxy(self.name,name)
                       
    def setSessionValue(self,name,value,raw=0):
        """  Add or  override options  as a  part of  second  phase of
        initialization of  this configuration module (PHASE  2) If the
        default type of the option  is not string, then the expression
        will be evaluated. Optional  argument raw indicates if special
        treatment  of  PATH-like  variables  is disabled  (enabled  by
        default).  The special treatment  applies to the session level
        values only (and not the default one!).  """
        
        logger = getLogger()

        logger.debug('trying to set session option [%s]%s = %s',self.name, name,value)
       
        for h in self._session_handlers:
            value = h[0](name,value)

        if not self.options.has_key(name):
            self.options[name] = ConfigOption(name)
            
        self.options[name].setSessionValue(value)
        
        logger.debug('sucessfully set session option [%s]%s = %s',self.name, name,value)

        for h in self._session_handlers:
            h[1](name,value)
        

    def setUserValue(self,name,value):
        """ Modify option  at runtime. This  method corresponds to  the user
        action so  the value of  the option is considered  'modified' If
        the  default  type  of  the  option  is  not  string,  then  the
        expression will be evaluated. """

        logger = getLogger()

        logger.debug('trying to set user option [%s]%s = %s',self.name, name,value)

        for h in self._user_handlers:
            value = h[0](name,value)
            
        self.options[name].setUserValue(value)
        
        logger.debug('successfully set user option [%s]%s = %s',self.name, name,value)

        for h in self._user_handlers:
            h[1](name,value)


    def overrideDefaultValue(self,name,val):
        self.options[name].overrideDefaultValue(val)
        
    def revertToSession(self,name):
        try:
            del self.options[name].user_value
        except AttributeError:
            pass

    def revertToDefault(self,name):
        self.revertToSession(name)
        try:
            del self.options[name].session_value
        except AttributeError:
            pass       

    def revertToSessionOptions(self):
        for name in self.options:
            self.revertToSession(name)

    def revertToDefaultOptions(self):
        self.revertToSessionOptions()
        for name in self.options:
            self.revertToDefault(name)

    def getEffectiveOptions(self):
        eff = {}
        for name in self.options:
            eff[name] = self.options[name].value
        return eff

    def getEffectiveOption(self,name):
        try:
            return self.options[name].value
        except KeyError:
            raise ConfigError('option "%s" does not exist in "%s"'%(name,self.name))

    def getEffectiveLevel(self,name):
        """ Return 0 if option is effectively set at the user level, 1
        if at session level or 2 if at default level """
        try:
            return self.options[name].level
        except KeyError,x:
            raise ConfigError('option "%s" does not exist in "%s"'%(x,self.name))
    
    def attachUserHandler(self,pre,post):
        """ Attach a user handler:
        - pre(name,x) will be always called before setUserValue(name,x)
        - post(name,x2) will be always called after setUserValue(name,x)

        pre() acts as a filter for the value of the option: its return value (x2) will be set
        Before setting the value will be evaluated (unless a default value type is a string)
        post() will get x2 as the option value.
    
        It is OK to give None for pre or post. For example:
           config.attachUserHandler(None,post) attaches only the post handler. """
    
        if pre is None: pre = lambda opt,val:val
        if post is None: post = lambda opt,val:None
            
        self._user_handlers.append((pre,post))

    def attachSessionHandler(self,pre,post):
        """See attachUserHandler(). """
        # FIXME: this will NOT always work and should be redesigned, see ConfigOption.filter
        if pre is None: pre = lambda opt,val:val
        if post is None: post = lambda opt,val:None
            
        self._session_handlers.append((pre,post))

    def deleteUndefinedOptions(self):
        for o in self.options.keys():
            if not self.options[o].check_defined():
                del self.options[o]
                
import ConfigParser

def make_config_parser(system_vars):
    cfg = ConfigParser.ConfigParser()
    cfg.optionxform = str # case sensitive
    cfg.defaults().update(system_vars)
    return cfg

def transform_PATH_option(name,new_value,current_value):
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

    logger = getLogger()
  
    PATH_ITEM = '_PATH'
    if name[-len(PATH_ITEM):] == PATH_ITEM:
        logger.debug('PATH-like variable: %s %s %s',name,new_value,current_value)
        if current_value is None:
            return new_value
        if new_value[:3] != ':::':
            logger.debug('Prepended %s to PATH-like variable %s',new_value,name)
            return new_value + ':' + current_value
        else:
            logger.debug('Resetting PATH-like variable %s to %s',name,new_value)
            return new_value[3:]
    return new_value

def read_ini_files(filenames,system_vars):
    """ Return  a ConfigParser object  which contains  all options  from the
    sequence of files (which are parsed from left-to-right).
    Apply special rules for PATH-like variables - see transform_PATH_option() """

    logger = getLogger()

    logger.debug('reading ini files: %s',str(filenames))

    main = make_config_parser(system_vars)

    #load all config files and apply special rules for PATH-like variables
    #note: main.read(filenames) cannot be used because of that

    if type(filenames) is type(''):
        filenames = [filenames]

    for f in filenames:
        if f is None or f=='':
            continue
        cc =  make_config_parser(system_vars)
        logger.info('reading config file %s',f)
        try:
            cc.readfp(file(f))
        except IOError,x:
            logger.warning('%s',str(x))

        for sec in cc.sections():
            if not main.has_section(sec):
                main.add_section(sec)

            for name in cc.options(sec):
                value = cc.get(sec,name)

                # do not put the DEFAULTS into the sections (no need)
                if name in cc.defaults().keys():
                    continue

                # special rules (NOT APPLIED IN DEFAULT SECTION):

                try:
                    current_value = main.get(sec,name)
                except ConfigParser.NoOptionError:
                    current_value = None
                    
                value = transform_PATH_option(name,value,current_value)

                # FIXME: strip trailing whitespaces -- SHOULD BE DONE BEFORE IF AT ALL?
                value = value.rstrip()
                main.set(sec,name,value)
        
    return main


def setSessionValue(config_name,option_name,value):
    if allConfigs.has_key(config_name):
        c = getConfig(config_name)
        if c.options.has_key(option_name):
            c.setSessionValue(option_name,value)
            return
        if c.is_open:
            c._addOpenOption(option_name,value)
            c.setSessionValue(option_name,value)
            return

    # put value in the buffer, it will be removed from the buffer when option is added
    allConfigFileValues.setdefault(config_name,{})
    allConfigFileValues[config_name][option_name] = value


_configured = False

def configure(filenames,system_vars):
    """ Sets session values for all options in all configuration units
    defined in the sequence of config files.  Initialize config parser
    object with system variables (such as GANGA_TOP, GANGA_VERSION and
    alike).  Contrary to standard  config parser behaviour the options
    from the  DEFAULTS section  are not visible  in the  config units.

    At the time of reading the initialization files, the default options in
    the configuration options (default values) may have not yet been defined.
    """


    cfg = read_ini_files(filenames,system_vars)

    for name in cfg.sections():
        for o in cfg.options(name):
            # Important: do not put the options from the DEFAULTS section into the configuration units!
            if o in cfg.defaults().keys():
                continue
            v = cfg.get(name,o)
            setSessionValue(name,o,v)

    _configured = True


# KH 050725: Add possibility to overwrite at run-time option set in
#            configuration file
#            => useful for executables where particular actions need
#               to be forced or suppressed
def setConfigOption( section = "", item = "", value = "" ):
   """Function to overwrite option values set in configuration file:

      Arguments:
         section - Name of relevant section within configuration file
         item    - Item for which value is to be changed
         value   - Value to be assigned

      Function needs to be called after configuration file has been parsed.

      Return value: None"""

   if bool( section ) and bool( item ):
      try:
         config = getConfig( section )
         if config.getEffectiveOptions().has_key( item ):
            config.setSessionValue( item, value )
      except:
         pass

   return None
# KH 050725: End of addition


def expandConfigPath(path,top):
    """ Split the path and return a list, where all relative path components will have top prepended.
        Example: 'A:/B/C::D/E' -> ['top/A','/B/C','top/D/E']
    """
    import os.path
    l = []
    for p in path.split(':'):
        if p:
            if not os.path.isabs(p):
                p = os.path.join(top,p)
            l.append(p)
    return l

def sanityCheck():

    logger = getLogger()
    for c in allConfigs.values():
        if not c._config_made:
            logger.error("sanity check failed: %s: no makeConfig() found in the code",c.name)

    for name in allConfigFileValues:
        opts = allConfigFileValues[name]
        try:
            cfg = allConfigs[name]
        except KeyError:
            logger.error("unknown configuration section: [%s]",name)
            continue

        if not cfg.is_open:
            if opts:
                logger.error("unknown options [%s]%s",name,','.join(opts.keys()))
        else:
            # add all options for open sections
            for o,v in zip(opts.keys(),opts.values()):
                cfg._addOpenOption(o,v)
                cfg.setSessionValue(o,v)

def getFlavour():

    runtimepath = getConfig('Configuration')['RUNTIME_PATH']

    if 'GangaLHCb' in runtimepath:
    	lhcb = True
    else:
    	lhcb = False
    
    if 'GangaAtlas' in runtimepath:
    	atlas = True
    else:
    	atlas = False

    if lhcb and atlas:
    	raise ConfigError('Atlas and LHCb conflict')

    if lhcb:
        return 'LHCb'

    if atlas:
        return 'Atlas'

    return ''
