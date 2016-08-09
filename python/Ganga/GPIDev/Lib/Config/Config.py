import textwrap

import Ganga.Utility.logging

from Ganga.Utility.Config import ConfigError

from Ganga.GPIDev.Base.Proxy import stripProxy, implRef, getName

from Ganga.Utility.Config import getConfig

logger = Ganga.Utility.logging.getLogger()

# GPI Proxy to manipulate configuration of all configuration units.
# The interface of this class should resemble the regular dictionary
# interface.

class ConfigDescriptor(object):

    def __init__(self, name):
        self._name = name

    def __get__(self, obj, cls):
        assert(obj)
        return stripProxy(obj).getEffectiveOption(getName(self))

    def __set__(self, obj, val):
        stripProxy(obj).setUserValue(self._name, val)

class ConfigProxy(object):

    """ A proxy for a single configuration unit.
    This is a base class which is inherited by a concrete proxy class for each configuration unit.
    In each inherited class descriptors (attreibutes) are set for all configuration options.
    This is done by the bootstrap() function.
    """

    def __init__(self, impl):
        self.__dict__[implRef] = impl

    def __getitem__(self, o):
        try:
            return getattr(self, o)
        except AttributeError as x:
            raise ConfigError('Undefined option %s (%s)' % (o, str(x)))

    def __setattr__(self, o, v):
        if o in dir(self.__class__):
            stripProxy(self).setUserValue(o, v)
        else:
            if not stripProxy(self).is_open:
                raise ConfigError(
                    'Cannot set undefined option [%s]%s' % (stripProxy(self).name, o))
            else:
                stripProxy(self)._addOpenOption(o, v)
                stripProxy(self).setUserValue(o, v)
                setattr(self.__class__, o, ConfigDescriptor(o))

    __setitem__ = __setattr__

    def __iter__(self):
        return stripProxy(self).__iter__()

    def __str__(self):
        return self._display(False)

    def _display(self, colour):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, getColour, Foreground, Effects

        if colour:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        fg = Foreground()

        display_config = getConfig('Display')

        name_colour = getColour(display_config['config_name_colour'])
        docstring_colour = getColour(
            display_config['config_docstring_colour'])  # fg.boldgrey
        value_colour = getColour(
            display_config['config_value_colour'])  # fx.normal

        levels = ['**', '* ', '  ']
        levels = map(lambda x: markup(x, fg.red), levels)
        from cStringIO import StringIO
        sio = StringIO()
        sio.write('%s' % markup(stripProxy(self).name, name_colour) +
                  ' : ' + markup(stripProxy(self).docstring, docstring_colour) + '\n')
        opts = sorted(stripProxy(self).options.keys())
        INDENT = '     ' * 2
        for o in opts:
            sio.write(levels[stripProxy(self).getEffectiveLevel(
                o)] + '   ' + markup(o, name_colour) + ' = ' + markup(repr(stripProxy(self)[o]), value_colour) + '\n')
            sio.write(textwrap.fill(markup(stripProxy(self).options[o].docstring.strip(
            ), docstring_colour), width=80, initial_indent=INDENT, subsequent_indent=INDENT) + '\n')
            typelist = stripProxy(self).options[o].typelist
            if not typelist:
                typedesc = 'Type: ' + \
                    str(type(stripProxy(self).options[o].default_value))
            else:
                typedesc = 'Allowed types: ' + \
                    str([t.split('.')[-1] for t in typelist])
            sio.write(markup(INDENT + typedesc, docstring_colour) + '\n')
            filter = stripProxy(self).options[o].filter
            if filter:
                filter_doc = filter.__doc__
                if not filter_doc:
                    filter_doc = "undocumented"
                sio.write(
                    markup(INDENT + "Filter: " + filter_doc, docstring_colour) + '\n')
            examples = stripProxy(self).options[o].examples
            if examples:
                sio.write(
                    markup(INDENT + "Examples:", docstring_colour) + '\n')
                for e in examples.splitlines():
                    sio.write(
                        markup(INDENT + e.strip(), docstring_colour) + '\n')

        return sio.getvalue()

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('config...')
            return
        p.text(self._display(colour=True))


class MainConfigProxy(object):

    """ A proxy class for the main config object which contains all configuration sections.
    The configuration section proxies are set as attributes by the bootstrap() function.
    This class is used to create a singleton GPI config object.
    """

    def __init__(self):
        import Ganga.Utility.Config
        self.__dict__[implRef] = Ganga.Utility.Config.allConfigs

    def __getitem__(self, p):
        try:
            return getattr(self, p)
        except AttributeError as x:
            msg = 'configuration section "%s" not found' % str(p)
            logger.error(msg)
            raise ConfigError(msg)

    def __setattr__(self, p, v):
        msg = 'cannot create new configuration sections in GPI'
        logger.error(msg)
        raise ConfigError(msg)

    __setitem__ = __setattr__

    def __iter__(self):
        return stripProxy(self).__iter__()

    def __str__(self):
        return self._display(False)

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('registry...')
            return
        p.text(self._display(True))

    def _display(self, colour):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Foreground, Effects

        if colour:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        fg = Foreground()

        from cStringIO import StringIO
        sio = StringIO()
        sio.write("Ganga Configuration" + '\n')
        sections = sorted(stripProxy(self).keys())
        maxcol = 0
        for p in sections:
            if len(p) > maxcol:
                maxcol = len(p)
        if maxcol > 50:
            maxcol = 50
        for p in sections:
            sio.write(
                '%-*s : %s' % (maxcol, p, markup(stripProxy(self)[p].docstring.split('\n')[0], fg.boldgrey)) + '\n')
        return sio.getvalue()

# GPI config singleton.
config = MainConfigProxy()


def print_config_file():
    from cStringIO import StringIO
    sio = StringIO()

    sections = sorted(stripProxy(config).keys())

    def print_doc_text(text):
        for line in text.splitlines():
            sio.write('# ' + line + '\n')

    for p in sections:
        sect = stripProxy(config)[p]
        if not sect.cfile:
            continue
        sio.write('\n')
        sio.write(
            "#=======================================================================\n")
        sio.write("[%s]" % p + '\n')

        print_doc_text(sect.docstring)

        opts = sorted(sect.options.keys())
        for o in opts:
            if sect.options[o].cfile:
                sio.write('\n')
                print_doc_text(sect.options[o].docstring)
                sio.write('#%s = %s' %
                          (o, sect.options[o].default_value) + '\n')


def config_file_as_text():

    text = ''

    sections = sorted(stripProxy(config).keys())
    INDENT = "#  "
    for p in sections:

        sect = stripProxy(config)[p]
        if not sect.cfile:
            continue

        text += "\n"
        text += "#=======================================================================\n"
        text += textwrap.fill(sect.docstring.strip(), width=80,
                              initial_indent=INDENT, subsequent_indent=INDENT) + "\n"
        text += "[%s]\n\n" % p

        opts = sorted(sect.options.keys())
        for o in opts:
            if sect.options[o].cfile:
                text += ""
                text += textwrap.fill(sect.options[o].docstring.strip(
                ), width=80, initial_indent=INDENT, subsequent_indent=INDENT) + "\n"

                examples = sect.options[o].examples
                if examples:
                    text += INDENT + "Examples:\n"
                    for e in examples.splitlines():
                        text += INDENT + "  " + e.strip() + "\n"
                if sect.getEffectiveLevel(o) == 0:
                    value = sect[o]
                    def_value = sect.options[o].default_value
                    if isinstance(value, str):
                        try:
                            lines = value.splitlines()
                            def_lines = def_value.splitlines()
                            if len(lines) > 1:
                                value = "\n# ".join(lines)
                                def_value = "\n# ".join(def_lines)
                        except AttributeError as err:
                            pass
                    text += '#%s = %s\n' % (o, def_value)
                    text += '%s = %s\n\n' % (o, value)
                else:
                    value = sect.getEffectiveOption(o)
                    if isinstance(value, str):
                        lines = value.splitlines()
                        if len(lines) > 1:
                            value = "\n# ".join(lines)
                    text += '#%s = %s\n\n' % (o, value)

    return text


def createSectionProxy(name):
    """ Create a class derived from ConfigProxy with all option descriptors and insert it into MainConfigProxy class.
    """
    cfg = stripProxy(config)[name]
    if not cfg.hidden:
        d = {}
        for opt in cfg:
            o = cfg.options[opt]
            if not o.check_defined():
                if not cfg.is_open:
                    logger.error(
                        'undefined option [%s]%s removed from configuration', name, opt)
                    continue
                else:
                    cfg.addOption(opt, o.value, '', override=True)
                    o = cfg.options[opt]
            if not o.hidden:
                d[opt] = ConfigDescriptor(opt)

        cfg.deleteUndefinedOptions()

        proxy_class = type(name, (ConfigProxy,), d)
        setattr(MainConfigProxy, name, proxy_class(stripProxy(config)[name]))


def createOptionProxy(section_name, option_name):
    """ Create an option description in an existing ConfigProxy class - an attribute of MainConfigProxy class.
    """
    if not stripProxy(config)[section_name].options[option_name].hidden:
        setattr(getattr(MainConfigProxy, section_name).__class__,
                option_name, ConfigDescriptor(option_name))


def bootstrap():
    """ Create GPI proxies for all configuration sections.
    """
    for name in stripProxy(config):
        createSectionProxy(name)
    import Ganga.Utility.Config.Config
    Ganga.Utility.Config.Config._after_bootstrap = True
    Ganga.Utility.Config.Config.sanityCheck()
