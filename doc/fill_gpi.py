#! /usr/bin/env python

"""
This script generates ReST files based on the contents of the GPI.
These files are then parsed by Sphinx to create the GPI documentation.
"""

from __future__ import print_function, absolute_import

import sys
import os
import inspect
from itertools import zip_longest
import shutil

doc_dir = os.path.dirname(os.path.realpath(__file__))
python_dir = doc_dir + '/../ganga'
sys.path.insert(0, os.path.abspath(os.path.join(doc_dir, '..', 'ganga')))

from GangaCore.GPIDev.Base.Proxy import GPIProxyObject, stripProxy, getName

print('Generating GPI documentation')

## LOADING GANGA ##
import GangaCore.PACKAGE
GangaCore.PACKAGE.standardSetup()

import GangaCore.Runtime
gangadir = os.path.expandvars('$HOME/gangadir_sphinx_dummy')
this_argv = [
    'ganga',  # `argv[0]` is usually the name of the program so fake that here
    '-o[Configuration]RUNTIME_PATH=GangaCore',
    '-o[Configuration]gangadir={gangadir}'.format(gangadir=gangadir),
]

# Actually parse the options
GangaCore.Runtime._prog = GangaCore.Runtime.GangaProgram(argv=this_argv)
GangaCore.Runtime._prog.parseOptions()

# Perform the configuration and bootstrap steps in ganga
GangaCore.Runtime._prog.configure()
GangaCore.Runtime._prog.initEnvironment()
GangaCore.Runtime._prog.bootstrap(interactive=False)
## FINISHED LOADING GANGA ##


def indent(s, depth=''):
    # type: (str, str) -> str
    """
    Adds `indent` to the beginning of every line
    """
    return '\n'.join(depth+l for l in s.splitlines())


def reindent(docstring, depth=''):
    # type: (str, str) -> str
    """
    Returns ``docstring`` trimmed and then each line prepended with ``depth``
    """
    return indent(inspect.cleandoc(docstring), depth)


def signature(func, name=None):
    """
    Args:
        func: a function object
        name: an optional name for the function in case the function has been aliased

    Returns: a string representing its signature as would be written in code

    """
    args, varargs, varkw, defaults = inspect.getargspec(func)
    defaults = defaults or []  # If there are no defaults, set it to an empty list
    defaults = [repr(d) for d in defaults]  # Type to get a useful string representing the default

    # Based on a signature like foo(a, b, c=None, d=4, *args, **kwargs)
    # we get args=['a', 'b', 'c', 'd'] and defaults=['None', '4']
    # We must match them backwards from the end and pad the beginning with None
    # to get arg_pairs=[('a', None), ('b', None), ('c', 'None'), ('d', '4')]
    arg_pairs = reversed([(a, d) for a, d in zip_longest(reversed(args), reversed(defaults), fillvalue=None)])
    # Based on arg_pairs we convert it into
    # arg_strings=['a', 'b', 'a=None', 'd=4']
    arg_strings = []
    for arg, default in arg_pairs:
        full_arg = arg
        if default is not None:
            full_arg += '='+default
        arg_strings.append(full_arg)
    # and append args and kwargs if necessary to get
    # arg_strings=['a', 'b', 'a=None', 'd=4', '*args', '**kwargs']
    if varargs is not None:
        arg_strings.append('*'+varargs)
    if varkw is not None:
        arg_strings.append('**'+varkw)

    # Signature is then 'foo(a, b, c=None, d=4, *args, **kwargs)'
    return '{name}({args})'.format(name=name or func.__name__, args=', '.join(arg_strings))


# First we get all objects that are in Ganga.GPI and filter out any non-GangaObjects
gpi_classes = [stripProxy(o) for name, o in GangaCore.GPI.__dict__.items() if isinstance(o, type) and issubclass(o, GPIProxyObject)]

with open(doc_dir+'/GPI/classes.rst', 'w') as cf:

    print('GPI classes', file=cf)
    print('===========', file=cf)
    print('', file=cf)

    # For each class we generate a set of ReST using '.. class::' etc.
    for c in gpi_classes:
        print('.. class:: {module_name}'.format(module_name=getName(c)), file=cf)

        if c.__doc__:
            print('', file=cf)
            print(reindent(c.__doc__, '    '), file=cf)

        print('', file=cf)

        print(reindent('Plugin category: ' + c._category, '    '), file=cf)

        print('', file=cf)

        # Go through each schema item and document it if it's not hidden
        visible_items = ((name, item) for name, item in c._schema.allItems() if not item['hidden'])
        for name, item in visible_items:
            # These are the only ones we want to show in the docs
            properties_we_care_about = ['protected', 'defvalue', 'changable_at_resubmit']
            props = dict((k, v) for k, v in item._meta.items() if k in properties_we_care_about)

            print('    .. attribute:: {name}'.format(name=name), file=cf)
            print('', file=cf)
            print(reindent(item['doc'], depth='        '), file=cf)
            print(reindent(str(props), depth='        '), file=cf)
            print('', file=cf)

        # Add documentation for each exported method
        for method_name in c._exportmethods:
            try:
                f = getattr(c, method_name).__func__
            except AttributeError as e:
                # Some classes have erroneous export lists so we catch them here and print a warning
                print('WARNING on class', getName(c), ':', end=' ', file=sys.stderr)
                print(e, file=sys.stderr)
                continue

            print('    .. method:: {signature}'.format(signature=signature(f)), file=cf)
            if f.__doc__:
                print('', file=cf)
                print(reindent(f.__doc__, '        '), file=cf)
                print('', file=cf)

        print('', file=cf)
        print('', file=cf)

# Looking through the plugin list helps categorise the GPI objects

with open(doc_dir+'/GPI/plugins.rst', 'w') as pf:
    from GangaCore.Utility.Plugin.GangaPlugin import allPlugins

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
            print('* :class:`{name}`'.format(name=name), file=pf)
        print('', file=pf)

print('')

# All objects that are not proxied GangaObjects
gpi_objects = dict((name, stripProxy(o)) for name, o in GangaCore.GPI.__dict__.items() if stripProxy(o) not in gpi_classes and not name.startswith('__'))

# Any objects which are not exposed as proxies (mostly exceptions)
non_proxy_classes = dict((k, v) for k, v in gpi_objects.items() if inspect.isclass(v))

# Anything which is callable
callables = dict((k, v) for k, v in gpi_objects.items() if callable(v) and v not in non_proxy_classes.values())

# Things which were declared as actual functions
functions = dict((k, v) for k, v in callables.items() if inspect.isfunction(v) or inspect.ismethod(v))

with open(doc_dir+'/GPI/functions.rst', 'w') as ff:

    print('Functions', file=ff)
    print('=========', file=ff)
    print('', file=ff)

    for name, func in functions.items():

        print('.. function:: {signature}'.format(signature=signature(func, name)), file=ff)
        print('', file=ff)
        print(reindent(func.__doc__ or '', '    '), file=ff)

        print('', file=ff)

## EXITING GANGA ##
from GangaCore.Core.InternalServices import ShutdownManager

# make sure we don't have an interactive shutdown policy
from GangaCore.Core.GangaThread import GangaThreadPool
GangaThreadPool.shutdown_policy = 'batch'

# This should now be safe
ShutdownManager._protected_ganga_exitfuncs()

shutil.rmtree(gangadir, ignore_errors=True)
## FINISHED EXITING GANGA ##
