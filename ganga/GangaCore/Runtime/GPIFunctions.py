import runpy
from GangaCore.GPI import *


def license():
    'Print the full license (GPL)'
    from GangaCore.Utility.logging import getLogger
    logger = getLogger()
    from os import path
    from GangaCore import _gangaPythonPath
    with open(path.join(_gangaPythonPath, '..', 'LICENSE_GPL')) as printable:
        logger.info(printable.read())

def typename(obj):
    """Return a name of Ganga object as a string, example: typename(j.application) -> 'DaVinci'"""
    from GangaCore.GPIDev.Base.Proxy import isProxy, stripProxy, implRef
    if isProxy(obj):
        if hasattr(stripProxy(obj), '_name'):
            return stripProxy(obj)._name
        else:
            from GangaCore.Utility.logging import getLogger
            logger = getLogger()
            logger.error("Object %s DOES NOT have the _name parameter set" % (str(obj)))
            return ""
    else:
        if hasattr(obj, '_name'):
            return obj._name
        else:
            from GangaCore.Utility.logging import getLogger
            logger = getLogger()
            logger.error("Object %s DOES NOT have the %s or _name parameter set" % (str(obj), str(implRef)))
            return ""

def categoryname(obj):
    """Return a category of Ganga object as a string, example: categoryname(j.application) -> 'applications'"""
    from GangaCore.GPIDev.Base.Proxy import isProxy, stripProxy, implRef
    if isProxy(obj):
        if hasattr(stripProxy(obj), '_category'):
            return stripProxy(obj)._category
        else:
            from GangaCore.Utility.logging import getLogger
            logger = getLogger()
            logger.error("Object %s DOES NOT have the _category parameter set" % (str(obj)))
            return ""
    else:
        if hasattr(obj, '_category'):
            return obj._category
        else:
            from GangaCore.Utility.logging import getLogger
            logger = getLogger()
            logger.error("Object %s DOES NOT have the %s or _category parameter set" % (str(obj), str(implRef)))
            return ""

def plugins(category=None):
    """List loaded plugins.

    If no argument is given return a dictionary of all loaded plugins.
    Keys are category name. Values are lists of plugin names in each
    category.

    If a category is specified (for example 'splitters') return a list
    of all plugin names in this category.
    """
    from GangaCore.Utility.Plugin import allPlugins
    if category:
        return list(allPlugins.allClasses(category).keys())
    else:
        d = {}
        for c in allPlugins.allCategories():
            d[c] = list(allPlugins.allCategories()[c].keys())
        return d

# FIXME: DEPRECATED
def list_plugins(category):
    """List all plugins in a given category, OBSOLETE: use plugins(category)"""
    raise DeprecationWarning("use plugins('%s')" % category)

def applications():
    """return a list of all available applications, OBSOLETE: use plugins('applications')"""
    raise DeprecationWarning("use plugins('applications')")

def backends():
    """return a list of all available backends, OBSOLETE: use plugins('backends')"""
    raise DeprecationWarning("use plugins('backends')")

# FIXME: END

def convert_merger_to_postprocessor(j):
    from GangaCore.GPIDev.Base.Proxy import stripProxy
    if len(stripProxy(j.postprocessors).process_objects):
        logger.info('job(%s) already has postprocessors' % j.fqid)
    if stripProxy(j).merger is None:
        logger.info('job(%s) does not have a merger to convert' % j.fqid)
    if not len(stripProxy(j.postprocessors).process_objects) and stripProxy(j).merger is not None:
        mp = MultiPostProcessor()
        mp.process_objects.append(stripProxy(j).merger)
        stripProxy(j).postprocessors = mp

def runfile(path_to_file):
    """
    A wrapper for the runpy.run_path() function.
    Usage:
    Ganga In[]: runfile('myfile.py')
    Note: If you want to run a file which includes something like:

    j = Job()
    j.submit()

    then you have to run the function with init_globals=globals() in the Ganga interpreter like this

     Ganga In[]: runfile('myfile.py')
    """
    if not isinstance(path_to_file, str):
        raise ValueError("path_to_file must be a string containing the path to the file to be executed. ")
    runpy.run_path(path_to_file, init_globals=globals())
