
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Utility.Config import ConfigError

# GPI Proxy to manipulate configuration of all configuration units.
# The interface of this class should resemble the regular dictionary
# interface.

class ConfigDescriptor(object):
    def __init__(self,name):
        self._name = name
    def __get__(self,obj,cls):
        assert(obj)
        return obj._impl.getEffectiveOption(self._name)
    def __set__(self,obj,val):
        obj._impl.setUserValue(self._name,val)

from Ganga.Utility.Config import getConfig

display_config = getConfig('Display')

display_config.addOption('config_name_colour','fx.bold', 'colour print of the names of configuration sections and options')
display_config.addOption('config_docstring_colour','fg.green', 'colour print of the docstrings and examples')
display_config.addOption('config_value_colour','fx.bold', 'colour print of the configuration values')


class ConfigProxy(object):
    """ A proxy for a single configuration unit.
    This is a base class which is inherited by a concrete proxy class for each configuration unit.
    In each inherited class descriptors (attreibutes) are set for all configuration options.
    This is done by the bootstrap() function.
    """
    def __init__(self,impl):
        self.__dict__['_impl'] = impl

    def __getitem__(self,o):
        try:
            return getattr(self,o)
        except AttributeError,x:
            raise ConfigError('Undefined option %s (%s)'%(o,str(x)))

    def __setattr__(self,o,v):
        if o in dir(self.__class__):
            self._impl.setUserValue(o,v)
        else:
            if not self._impl.is_open:
                raise ConfigError('Cannot set undefined option [%s]%s'%(self._impl.name,o))
            else:
                self._impl._addOpenOption(o,v)
                self._impl.setUserValue(o,v)
                setattr(self.__class__, o, ConfigDescriptor(o))
            
    __setitem__ = __setattr__

    def __iter__(self):
        return self._impl.__iter__()

    def __str__(self):
        return self._display(False)

    def _display(self,colour):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, getColour, Foreground, Background, Effects
        import Ganga.Utility.external.textwrap as textwrap
        
        if colour:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        fg = Foreground()
        fx = Effects()

        from Ganga.Utility.Config import getConfig

        display_config = getConfig('Display')

        name_colour = getColour(display_config['config_name_colour']) #
        docstring_colour = getColour(display_config['config_docstring_colour']) #fg.boldgrey
        value_colour = getColour(display_config['config_value_colour']) #fx.normal
        
        levels = ['**','* ','  ']
        levels = map(lambda x: markup(x,fg.red),levels)
        from StringIO import StringIO
        sio = StringIO()
        print >>sio, '%s'%markup(self._impl.name,name_colour),':',markup(self._impl.docstring,docstring_colour)
        opts = self._impl.options.keys()
        opts.sort()
        INDENT = '     '*2
        for o in opts:
            print >>sio, levels[self._impl.getEffectiveLevel(o)],' ',markup(o,name_colour),'=',markup(repr(self._impl[o]),value_colour)
            print >>sio, textwrap.fill(markup(self._impl.options[o].docstring.strip(),docstring_colour),width=80, initial_indent=INDENT,subsequent_indent=INDENT)
            typelist = self._impl.options[o].typelist
            if not typelist:
                typedesc = 'Type: '+str(type(self._impl.options[o].default_value))
            else:
                typedesc = 'Allowed types: '+str([t.split('.')[-1] for t in typelist])
            print >>sio, markup(INDENT+typedesc,docstring_colour)
            filter = self._impl.options[o].filter
            if filter:
                filter_doc = filter.__doc__
                if not filter_doc: filter_doc = "undocumented"
                print >>sio, markup(INDENT+"Filter: "+filter_doc,docstring_colour)
            examples = self._impl.options[o].examples
            if examples:
                print >>sio,markup(INDENT+"Examples:",docstring_colour)
                for e in examples.splitlines():
                    print >>sio, markup(INDENT+e.strip(),docstring_colour)

            
        return sio.getvalue()
    
class MainConfigProxy:
    """ A proxy class for the main config object which contains all configuration sections.
    The configuration section proxies are set as attributes by the bootstrap() function.
    This class is used to create a singleton GPI config object.
    """
    def __init__(self):
        import Ganga.Utility.Config
        self.__dict__['_impl'] = Ganga.Utility.Config.allConfigs

    def __getitem__(self,p):
        try:
            return getattr(self,p)
        except AttributeError,x:
            msg = 'configuration section "%s" not found' % str(p)
            logger.error(msg)
            raise ConfigError(msg)

    def __setattr__(self,p,v):
        msg = 'cannot create new configuration sections in GPI'
        logger.error(msg)
        raise ConfigError(msg)

    __setitem__ = __setattr__

    def __iter__(self):
        return self._impl.__iter__()

    def __str__(self):
        return self._display(False)
    
    def _display(self,colour):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Background, Effects

        if colour:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()
            
        fg = Foreground()
        
        from StringIO import StringIO
        sio = StringIO()
        print >>sio, "Ganga Configuration"
        sections = self._impl.keys()
        sections.sort()
        maxcol = 0
        for p in sections:
            if len(p)>maxcol:
                maxcol = len(p)
        if maxcol>50:
            maxcol=50
        for p in sections:
            print >>sio,'%-*s : %s'%(maxcol,p,markup(self._impl[p].docstring.split('\n')[0],fg.boldgrey))
        return sio.getvalue()

# GPI config singleton.
config = MainConfigProxy()

def print_config_file():
    sections = config._impl.keys()
    sections.sort()
    def print_doc_text(text):
        for line in text.splitlines():
            print '#',line
            
    for p in sections:
        sect = config._impl[p]
        if not sect.cfile:
            continue
        print
        print "#======================================================================="
        print "[%s]"%p

        print_doc_text(sect.docstring)

        opts = sect.options.keys()
        opts.sort()
        for o in opts:
            if sect.options[o].cfile:
                print
                print_doc_text(sect.options[o].docstring)
                print '#%s = %s'%(o,sect.options[o].default_value)

def config_file_as_text():

    import Ganga.Utility.external.textwrap as textwrap

    text = ''

    sections = config._impl.keys()
    sections.sort()
    INDENT = "#  "      
    INDENT_value = "# "      
    for p in sections:

        sect = config._impl[p]
        if not sect.cfile:
            continue

        text += "\n"
        text += "#=======================================================================\n"
        text += textwrap.fill(sect.docstring.strip(),width=80, initial_indent=INDENT,subsequent_indent=INDENT)+"\n"
        text += "[%s]\n\n"%p

        opts = sect.options.keys()
        opts.sort()
        for o in opts:
            if sect.options[o].cfile:
                text += ""
                text += textwrap.fill(sect.options[o].docstring.strip(),width=80, initial_indent=INDENT,subsequent_indent=INDENT)+"\n"

                
                examples = sect.options[o].examples
                if examples:
                    text += INDENT+"Examples:\n"
                    for e in examples.splitlines():
                        text += INDENT+"  "+e.strip()+"\n"
                if sect.getEffectiveLevel(o) == 0:
                     value = sect[o] 
                     def_value = sect.options[o].default_value
                     try:
                          lines = value.splitlines()
                          def_lines = def_value.splitlines()
                          if len(lines)>1:
                            value = "\n# ".join(lines)
                            def_value = "\n# ".join(def_lines)
                     except:
                          pass
                     text +='#%s = %s\n'%(o,def_value)
                     text +='%s = %s\n\n'%(o,value)
                else:
                     value = sect.getEffectiveOption(o)
                     try:
                          lines = value.splitlines()
                          if len(lines)>1:
                            value = "\n# ".join(lines)
                     except:
                          pass
                     text +='#%s = %s\n\n'%(o,value)
 
    return text

 
def createSectionProxy(name):
    """ Create a class derived from ConfigProxy with all option descriptors and insert it into MainConfigProxy class.
    """
    cfg = config._impl[name]
    if not cfg.hidden:
        d = {}
        for opt in cfg:
            o = cfg.options[opt]
            if not o.check_defined():
                if not cfg.is_open:
                    logger.error('undefined option [%s]%s removed from configuration',name,opt)
                    continue
                else:
                    cfg.addOption(opt,o.value,'',override=True)
                    o = cfg.options[opt]
            if not o.hidden:
                d[opt] = ConfigDescriptor(opt)

        cfg.deleteUndefinedOptions()
            
        proxy_class = type(name,(ConfigProxy,), d)
        setattr(MainConfigProxy,name,proxy_class(config._impl[name]))

def createOptionProxy(section_name,option_name):
    """ Create an option description in an existing ConfigProxy class - an attribute of MainConfigProxy class.
    """
    if not config._impl[section_name].options[option_name].hidden:
        setattr(getattr(MainConfigProxy,section_name).__class__,option_name,ConfigDescriptor(option_name))

def bootstrap():
    """ Create GPI proxies for all configuration sections.
    """
    for name in config._impl:
        createSectionProxy(name)
    import Ganga.Utility.Config.Config
    Ganga.Utility.Config.Config._after_bootstrap = True
    Ganga.Utility.Config.Config.sanityCheck()
    
## def bootstrap():
##     for name in config._impl:
##         if config._impl[name].hidden:
##             continue
##         d = {}
##         cfg = config._impl[name]
##         for opt in cfg:
##             if not cfg.options[opt].hidden:
##                 d[opt] = ConfigDescriptor(opt)
            
##         proxy_class = type(name,(ConfigProxy,), d)
##         setattr(MainConfigProxy,name,proxy_class(config._impl[name]))

##     import Ganga.Utility.Config.Config

##     Ganga.Utility.Config.Config._after_bootstrap = True
        
