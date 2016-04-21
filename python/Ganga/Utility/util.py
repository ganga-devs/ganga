##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: util.py,v 1.1 2008-07-17 16:41:01 moscicki Exp $
##########################################################################

"""
 This file contains general-purpose utilities, mainly Python Cookbook recipes.
"""

# based on Python Cookbook recipe 5.11 -- OK for new- and old-style classes


def empty_obj(klass):
    class Empty(klass):

        def __init__(self): pass
    newcopy = Empty()
    newcopy.__class__ = klass
    return newcopy

# create a class based on klass but which may create empty objects
# based on Python Cookbook recipe 5.11 -- OK for new- and old-style classes


def empty_class(klass):
    class _Empty(klass):

        def __init__(self): pass
    return _Empty

# remove the duplicates from a list. From the Python cook book recipe 17.3


def unique(s):
    """Return a list of elements in s in arbitrary order, but without
    duplicates. """

    # get the special case of an empty list
    n = len(s)
    if n == 0:
        return []

    # try to use a dict
    u = {}
    try:
        for x in s:
            u[x] = 1
    except TypeError:
        del u  # move on th the next method
    else:
        return u.keys()

        # Sort to bring duplicate elements together and weed out the
        # duplcates in on sinle pass
    try:
        t = sorted(s)
    except TypeError:
        del t  # move on to the next method
    else:
        assert n > 0
        last = t[0]
        lasti = i = 1
        while i < n:
            if t[i] != last:
                t[lasti] = last = t[i]
                lasti += 1
            i += 1
        return t[:lasti]

    # Brute foce
    u = []
    for x in s:
        if x not in u:
            u.append(x)
    return u

# based on Python Cookbook recipe 1.12 -- OK for python2.2 and greater


def canLoopOver(maybeIterable):
    try:
        iter(maybeIterable)
    except:
        return 0
    else:
        return 1


def isStringLike(obj):
    try:
        obj + ''
    except TypeError:
        return 0
    else:
        return 1


def containsGangaObjects(obj):
    """Recursive call to find GangaObjects"""
    from Ganga.GPIDev.Base.Proxy import isType
    from Ganga.GPIDev.Base.Objects import GangaObject
    if not isStringLike(obj) and canLoopOver(obj):
        for o in obj:
            if containsGangaObjects(o):
                return True
    elif isType(obj, GangaObject):
        # order is special here as we ignore GangaLists
        return True
    return False


def isNestedList(obj):
    if not isStringLike(obj) and canLoopOver(obj):
        for o in obj:
            if not isStringLike(o) and canLoopOver(o):
                return True
    return False

# ------------------------

__executed_frames = {}


def execute_once():
    """ Return True if this function was not yet executed from a certain line in the program code.
     Example:
      execute_once() # -> True
      execute_once() # -> True

      for i in range(2):
       execute_once() # -> True (1st), False (2nd)

      if execute_once() and execute_once():  # -> False
    """
    import inspect

    frame = inspect.stack()[1]
    fid = hash((frame[1], frame[2], frame[3], tuple(frame[4]), frame[5]))
    del frame
    if fid in __executed_frames:
        return False
    __executed_frames[fid] = 1
    return True

# ------------------------


def hostname():
    """ Try to get the hostname in the most possible reliable way as described in the Python LibRef."""

    # cache the result to prevent lockups in gethostbyaddr calls with queues
    if hostname._hostname_cache == '':
        import socket
        try:
            hostname._hostname_cache = socket.gethostbyaddr(hostname_tmp)[0]
        # [bugfix #20333]:
        # while working offline and with an improper /etc/hosts configuration
        # the localhost cannot be resolved
        except:
            hostname._hostname_cache = 'localhost'

    return hostname._hostname_cache

hostname._hostname_cache = ''

# ------------------------


def setAttributesFromDict(d, prefix=None):
    """
    *Python Cookbook recipe 6.18*
    Helper function to automatically initialises instance variables 
    from __init_ arguments.
    """
    if prefix is None:
        prefix = ''
    self = d.pop('self')
    for n, v in d.iteritems():
        setattr(self, prefix + n, v)


# ------------------------

def wrap_callable(any_callable, before, after):
    def _wrapped(*args, **kwargs):
        before()
        try:
            return any_callable(*args, **kwargs)
        finally:
            after()
    return _wrapped


def wrap_callable_filter(any_callable, before, after):
    def _wrapped(*args, **kwargs):
        args, kwargs = before(list(args), kwargs)
        try:
            return any_callable(*args, **kwargs)
        finally:
            after()
    return _wrapped


class GenericWrapper(object):

    def __init__(self, obj, before, after, ignore=(), forced=(), wrapper_function=wrap_callable):
        classname = 'GenericWrapper'
        self.__dict__['_%s__methods' % classname] = {}
        self.__dict__['_%s__obj' % classname] = obj
        import inspect
        for name, method in inspect.getmembers(obj, inspect.ismethod):
            if name not in ignore and method not in ignore:
                if forced and (name in forced or method in forced):
                    self.__methods[name] = wrapper_function(
                        method, before, after)

    def __getattr__(self, name):
        try:
            return self.__methods[name]
        except KeyError:
            return getattr(self.__obj, name)

    def __setattr__(self, name, value):
        setattr(self.__obj, name, value)

# ------------------------


class Proxy(object):

    def __init__(self, obj):
        super(Proxy, self).__init__(obj)
        self._obj = obj

    def __getattr__(self, attrib):
        return getattr(self._obj, attrib)


def make_binder(unbounded_method):
    def f(self, *a, **k): return unbounded_method(self._obj, *a, **k)
    return f

known_proxy_classes = {}


def proxy(obj, *specials):
    obj_cls = obj.__class__
    key = obj_cls, specials
    cls = known_proxy_classes.get(key)
    if cls is None:
        from Ganga.GPIDev.Base.Proxy import getName
        cls = type("%sProxy" % getName(obj_cls), (Proxy, ), {})
        for name in specials:
            name = '__%s__' % name
            unbounded_method = getattr(obj_cls, name)
            setattr(cls, name, make_binder(unbounded_method))
        known_proxy_classes[key] = cls
    return cls(obj)

# ------------------------
# cookbook recipe

def importName(modulename, name):
    try:
        module = __import__(modulename, globals(), locals(), [name])
    except ImportError as err:
        import sys
        sys.stderr.write("importName, ImportError: %s\n" % str(err))
        return None
    if name in vars(module).keys():
        return vars(module)[name]
    else:
        return None
    #try:
    #    return vars(module)[name]
    #except KeyError, err:
    #    import sys
    #    sys.stderr.write("ImportName, KeyError: %s\n" % str(err))
    #    return None
# ------------------------


if __name__ == "__main__":
    import Ganga.Utility.logic as logic

    assert(execute_once())
    assert(execute_once())

    if execute_once() and execute_once():
        assert(0)

    for i in range(5):
        assert(logic.equivalent(execute_once(), i == 0))

#
#
# $Log: not supported by cvs2svn $
# Revision 1.11.4.3  2008/03/12 17:31:29  moscicki
# simplified recursion
#
# Revision 1.11.4.2  2008/03/12 12:42:39  wreece
# Updates the splitters to check for File objects in the list
#
# Revision 1.11.4.1  2007/12/18 09:08:19  moscicki
# factored out the importName cookbook recipe (for integrated typesystem from Alvin)
#
# Revision 1.11  2007/07/27 14:31:56  moscicki
# credential and clean shutdown updates from Adrian (from Ganga-4-4-0-dev-branch)
#
# Revision 1.10.2.1  2007/07/27 08:46:07  amuraru
# clean shutdown update
#
# Revision 1.10  2007/01/25 16:30:41  moscicki
# mergefrom_Ganga-4-2-2-bugfix-branch_25Jan07 (GangaBase-4-14)
#
# Revision 1.9  2006/10/27 15:15:27  amuraru
# [bugfix #20333]
#
# Revision 1.8.2.1  2006/10/27 15:31:08  amuraru
# Bugfix #20545
#
# Revision 1.9  2006/10/27 15:15:27  amuraru
# [bugfix #20333]
#
# Revision 1.8  2006/07/31 12:19:03  moscicki
# added callable filter wrapper
#
# Revision 1.7  2006/07/10 14:03:28  moscicki
# added many Cookbook patterns from Alvin
#
# Revision 1.6  2006/02/10 15:08:06  moscicki
# hostname
#
# Revision 1.5  2006/01/09 16:41:44  moscicki
# execute_once() function used to issue LEGACY warnings
#
# Revision 1.4  2005/11/01 16:40:04  moscicki
# isStringLike
#
# Revision 1.3  2005/11/01 14:05:21  moscicki
# canLoopOver
#
# Revision 1.2  2005/10/06 09:51:58  andrew
# Added the unique method for clearing out duplicates in a list
# (Python Cook book recipe 17.3)
#
#
#
