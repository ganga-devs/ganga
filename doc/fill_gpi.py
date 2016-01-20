#! /usr/bin/env python

from __future__ import print_function

import sys
import os
import inspect
from itertools import izip_longest

doc_dir = os.path.dirname(os.path.realpath(__file__))
python_dir = doc_dir + '/../python'
sys.path.insert(0, os.path.abspath(os.path.join(doc_dir, '..', 'python')))

print('Generating GPI documentation')

## LOADING GANGA ##
import Ganga.PACKAGE
Ganga.PACKAGE.standardSetup()

# Start ganga by passing some options for unittesting
import Ganga.Runtime
this_argv = [
    'ganga',  # `argv[0]` is usually the name of the program so fake that here
    '-o[Configuration]RUNTIME_PATH=GangaTest',
    '-o[Configuration]gangadir=$HOME/gangadir_dummy',
]

# Actually parse the options
Ganga.Runtime._prog = Ganga.Runtime.GangaProgram(argv=this_argv)
Ganga.Runtime._prog.parseOptions()

# Perform the configuration and bootstrap steps in ganga
Ganga.Runtime._prog.configure()
Ganga.Runtime._prog.initEnvironment(opt_rexec=False)
Ganga.Runtime._prog.bootstrap(interactive=False)
## FINISHED LOADING GANGA ##


def trim(docstring):
    """
    From PEP 257
    """
    if not docstring:
        return ''
    # Convert tabs to spaces (following the normal Python rules)
    # and split into a list of lines:
    lines = docstring.expandtabs().splitlines()
    # Determine minimum indentation (first line doesn't count):
    indent = sys.maxint
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))
    # Remove indentation (first line is special):
    trimmed = [lines[0].strip()]
    if indent < sys.maxint:
        for line in lines[1:]:
            trimmed.append(line[indent:].rstrip())
    # Strip off trailing and leading blank lines:
    while trimmed and not trimmed[-1]:
        trimmed.pop()
    while trimmed and not trimmed[0]:
        trimmed.pop(0)
    # Return a single string:
    return '\n'.join(trimmed)


def indent(s, depth=''):
    """
    Adds `indent` to the beginning of every line
    """
    return '\n'.join(depth+l for l in s.splitlines())


def reindent(doctring, depth=''):
    return indent(trim(doctring), depth)


def format_entry(template, depth='', *args, **kwargs):
    args = (reindent(a, depth).lstrip() for a in args)
    kwargs = dict((k,reindent(str(v), depth).lstrip()) for k,v in kwargs.items())
    t = trim(template)
    t = t.format(*args, **kwargs)
    t = indent(t, depth)
    return t

from Ganga.GPIDev.Base.Proxy import GPIProxyObject, stripProxy, getName

all_gpi_classes = set()  # Track all the class names we find in the GPI

with open(doc_dir+'/GPI/classes.rst', 'w') as cf:
    # First we get all objects that are in Ganga.GPI and filter out any non-GangaObjects
    gpi_classes = (stripProxy(o) for name, o in Ganga.GPI.__dict__.items() if isinstance(o, type) and issubclass(o, GPIProxyObject))

    print('GPI classes', file=cf)
    print('===========', file=cf)
    print('', file=cf)

    # For each class we generate a set of ReST using '.. class::' etc.
    for c in gpi_classes:
        all_gpi_classes.add(getName(c))
        print('.. class:: {module_name}'.format(module_name=getName(c)), file=cf)

        if c.__doc__:
            print('', file=cf)
            print(reindent(c.__doc__, '    '), file=cf)

        items = ((name, item) for name, item in c._schema.allItems() if not item['hidden'])

        print('', file=cf)
        for name, item in items:
            properties_we_care_about = ['protected', 'defvalue', 'changable_at_resubmit']
            props = dict((k,v) for k,v in item._meta.items() if k in properties_we_care_about)
            s = '''
            .. attribute:: {name}

                {doc}
                {props}
            '''
            print(format_entry(s, depth='    ', name=name, doc=item['doc'], props=props), file=cf)
            print('', file=cf)

        for method_name in c._exportmethods:
            try:
                f = getattr(c, method_name).__func__
            except AttributeError as e:
                print('WARNING:', end=' ', file=sys.stderr)
                print(e, file=sys.stderr)
                continue

            args, varargs, varkw, defaults = inspect.getargspec(f)
            defaults = defaults or []
            defaults = [repr(d) for d in defaults]
            arg_pairs = reversed([(a, d) for a, d in izip_longest(reversed(args), reversed(defaults), fillvalue=None)])
            arg_strings = []
            for arg, default in arg_pairs:
                full_arg = arg
                if default is not None:
                    full_arg += '='+default
                arg_strings.append(full_arg)
            if varargs is not None:
                arg_strings.append('*'+varargs)
            if varkw is not None:
                arg_strings.append('**'+varkw)
            signature = '{name}({args})'.format(name=method_name, args=', '.join(arg_strings))
            print('    .. method:: {signature}'.format(signature=signature), file=cf)
            if c.__doc__:
                print('', file=cf)
                print(reindent(f.__doc__, '        '), file=cf)
                print('', file=cf)

        print('', file=cf)
        print('', file=cf)

all_plugin_classes = set()  # Track all the class names we find registered as plugins

with open(doc_dir+'/GPI/plugins.rst', 'w') as pf:
    from Ganga.Utility.Plugin.GangaPlugin import allPlugins

    print('Plugins', file=pf)
    print('=======', file=pf)
    print('', file=pf)

    for c, ps in allPlugins.allCategories().items():
        print(c, file=pf)
        print('-'*len(c), file=pf)
        print('', file=pf)

        for name, c in ps.items():
            if c._declared_property('hidden'):
                continue
            all_plugin_classes.add(name)
            print('* :class:`{name}`'.format(name=name), file=pf)
        print('', file=pf)

print()
print('Classes in GPI but not in plugins: ', ', '.join(all_gpi_classes - all_plugin_classes))
print('Classes in plugins but not in GPI: ', ', '.join(all_plugin_classes - all_gpi_classes))

## EXITING GANGA ##
from Ganga.Core.InternalServices import ShutdownManager

import Ganga.Core
Ganga.Core.change_atexitPolicy(interactive_session=False, new_policy='batch')
# This should now be safe
ShutdownManager._ganga_run_exitfuncs()
## FINISHED EXITING GANGA ##
