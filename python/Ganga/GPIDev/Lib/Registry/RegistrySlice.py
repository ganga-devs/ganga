import sys

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Core import GangaException
from Ganga.Core.GangaRepository.Registry import RegistryKeyError, RegistryIndexError, RegistryAccessError
import fnmatch
import collections
from Ganga.Utility.external.ordereddict import oDict

import Ganga.Utility.Config
config = Ganga.Utility.Config.makeConfig(
    'Display', 'control the printing style of the different registries ("jobs","box","tasks"...)')


class RegistrySlice(object):

    def __init__(self, name, display_prefix):
        self.objects = oDict()
        self.name = name
        self._display_columns = config[display_prefix + '_columns']
        self._display_columns_show_empty = config[
            display_prefix + "_columns_show_empty"]
        self._display_columns_width = config[display_prefix + "_columns_width"]
        self._display_columns_functions = {}
        try:
            cfs = config[display_prefix + '_columns_functions']
            for c in cfs:
                self._display_columns_functions[c] = eval(cfs[c])
        except Exception as x:
            logger.error("Error on evaluating display column functions from config file: %s: %s" % (
                x.__class__.__name__, x))

        from Ganga.Utility.ColourText import Effects
        self._colour_normal = Effects().normal
        self._proxyClass = None

    def _getColour(self, obj):
        """ Override this function in derived slices to colorize your job/task/... list"""
        return self._colour_normal

    def do_collective_operation(self, keep_going, method, *args, **kwds):
        """
        """
        if not isinstance(keep_going, bool):
            raise GangaException("The variable 'keep_going' must be a boolean. Probably you wanted to do %s(%s).%s()" % (
                self.name, keep_going, method))
        result = []
        for id, obj in self.objects.iteritems():
            try:
                if isinstance(method, str):
                    doc = method
                    result.append(getattr(obj, method)(*args, **kwds))
                else:
                    try:
                        doc = method.__doc__
                    except AttributeError:
                        doc = str(method)
                    result.append(method(obj, *args, **kwds))
            except GangaException as x:
                if not keep_going:
                    raise
            except Exception as x:
                logger.exception(
                    '%s %s %s: %s %s', doc, self.name, id, x.__class__.__name__, str(x))
                if not keep_going:
                    raise
        return result

    def ids(self, minid=None, maxid=None):
        "Get the list of job ids. 'minid' and 'maxid' specify optional (inclusive) slice range."
        if maxid is None:
            maxid = sys.maxsize
        if minid is None:
            minid = 0
        return [k for k in self.objects.keys() if minid <= k <= maxid]
        #ids = []
        # def callback(j):
        #    ids.append(j.id)
        # self.do_select(callback,minid,maxid)
        # return ids

    def clean(self, confirm=False, force=False):
        """Cleans the repository only if this slice represents the repository
        Returns True on success and False on failure"""
        if not hasattr(self.objects, "clean"):
            logger.error(
                "'clean' only works on whole registries, e.g. 'jobs.clean()'. Use remove() to delete job slices")
            return False
        if not confirm:
            logger.warning("You are about to irreversibly delete the WHOLE '%s' registry, without properly cleaning up individual jobs." % (
                self.objects.name))
            if force:
                logger.warning(
                    "You will also cause any other Ganga sessions accessing this repository to shut down their operations")
                logger.warning("If you just want to remove all jobs, type '%s.remove()'. If you really want to do this, type '%s.clean(confirm=True,force=True)" % (
                    self.objects.name, self.objects.name))
            else:
                logger.warning("If you just want to remove all jobs, type '%s.remove()'. If you really want to do this, type '%s.clean(confirm=True)" % (
                    self.objects.name, self.objects.name))
            return False
        return self.objects.clean(force)

    def select(self, minid=None, maxid=None, **attrs):
        import repr
        r = repr.Repr()
        attrs_str = "".join([',%s=%s' % (a, r.repr(attrs[a])) for a in attrs])
        slice = self.__class__("%s.select(minid=%s,maxid=%s%s)" % (
            self.name, r.repr(minid), r.repr(maxid), attrs_str))

        def append(id, obj):
            slice.objects[id] = obj
        self.do_select(append, minid, maxid, **attrs)
        return slice

    def do_select(self, callback, minid=None, maxid=None, **attrs):
        """Get the slice of jobs. 'minid' and 'maxid' specify optional (inclusive) slice range.
        The returned slice object has the job registry interface but it is not connected to
        persistent storage. 
        """
        import sys
        import fnmatch
        import re

        def select_by_list(id):
            return id in ids

        def select_by_range(id):
            return minid <= id <= maxid

        ids = None

        if isinstance(minid, collections.Container):
            ids = minid
            select = select_by_list
        else:
            if minid is None:
                minid = 0
            if maxid is None:
                maxid = sys.maxsize
            select = select_by_range

        for id, obj in self.objects.iteritems():
            if select(int(id)):
                selected = True
                for a in attrs:
                    if self.name == 'box':
                        attrvalue = attrs[a]
                        if a == 'name':
                            if not fnmatch.fnmatch(obj._getRegistry()._getName(obj), attrvalue):
                                selected = False
                                break
                        elif a == 'application':
                            if hasattr(obj, 'application'):
                                if not obj.application._name == attrvalue:
                                    selected = False
                                    break
                            else:
                                selected = False
                                break
                        elif a == 'type':
                            if not obj._name == attrvalue:
                                selected = False
                                break
                        else:
                            from Ganga.GPIDev.Base import GangaAttributeError
                            raise GangaAttributeError(
                                'undefined select attribute: %s' % str(a))
                    else:

                        if a == 'ids':
                            if int(id) not in attrs['ids']:
                                selected = False
                                break
                        else:
                            try:
                                item = obj._schema.getItem(a)
                            except KeyError:
                                from Ganga.GPIDev.Base import GangaAttributeError
                                raise GangaAttributeError(
                                    'undefined select attribute: %s' % str(a))
                            else:
                                attrvalue = attrs[a]

                                if item.isA('ComponentItem'):
                                    from Ganga.GPIDev.Base.Filters import allComponentFilters

                                    cfilter = allComponentFilters[
                                        item['category']]
                                    filtered_value = cfilter(attrs[a], item)
                                    if not filtered_value is None:
                                        attrvalue = filtered_value._name
                                    else:
                                        attrvalue = attrvalue._name

                                    if not getattr(obj, a)._name == attrvalue:
                                        selected = False
                                        break
                                else:
                                    if isinstance(attrvalue, str):
                                        regex = fnmatch.translate(attrvalue)
                                        reobj = re.compile(regex)
                                        # Compare the type of the attribute
                                        # against attrvalue
                                        if not reobj.match(str(getattr(obj, a))):
                                            selected = False
                                    else:
                                        if getattr(obj, a) != attrvalue:
                                            selected = False
                                            break
                if selected:
                    callback(id, obj)

    def copy(self, keep_going):
        slice = self.__class__("copy of %s" % self.name)
        for id, obj in self.objects.iteritems():
            #obj = _unwrap(obj)
            copy = obj.clone()
            # If the copied object is not automatically registered,
            # try to register it in the old objects registry
            new_id = copy._getRegistryID()
            if new_id is None:
                reg = obj._getRegistry()
                if reg is None:
                    new_id = id
                else:
                    reg._add(copy)
                    new_id = copy._getRegistryID()
            slice.objects[new_id] = copy
        return slice

    def __contains__(self, j):
        return j.id in self.objects

    def __call__(self, id):
        """ Retrieve an object by id.
        """
        if isinstance(id, str):
            if id.isdigit():
                id = int(id)
            else:
                matches = [o for o in self.objects if fnmatch.fnmatch(
                    o._getRegistry()._getName(o), id)]
                if len(matches) > 1:
                    logger.error(
                        'Multiple Matches: Wildcards are allowed for ease of matching, however')
                    logger.error(
                        '                  to keep a uniform response only one item may be matched.')
                    logger.error(
                        '                  If you wanted a slice, please use the select method')
                    raise RegistryKeyError("Multiple matches for id='%s':%s" % (
                        id, str(map(lambda x: x._getRegistry()._getName(x), matches))))
                if len(matches) < 1:
                    return
                return matches[0]
        try:
            return self.objects[id]
        except KeyError:
            raise RegistryKeyError('Object id=%d not found' % id)

    def __iter__(self):
        "Iterator for the objects. "
        class Iterator(object):

            def __init__(self, reg):
                self.it = reg.objects.values().__iter__()

            def __iter__(self): return self

            def next(self):
                return next(self.it)
        return Iterator(self)

    def __len__(self):
        "Number of objects in the registry"
        return len(self.objects)

    def __getitem__(self, x):
        """Retrieve the job object from the registry: registry[x].
         If 'x' is a job id (int) then a single job object is returned or IndexError.
         If 'x' is a name (string) then a unique same name is returned, otherwise [].
         If 'x' is a job object then it is returned if it belongs to the registry, otherwise None.
         If 'x' is not of any of the types above, raise TypeError.
          or by name. If retrieved by name then the job must be unique, otherwise the RegistryKeyError is raised.
         If the input is incorrect, RegistryAccessError is raised.
        """
        if isinstance(x, int):
            try:
                return self.objects.values()[x]
            except IndexError:
                raise RegistryIndexError('list index out of range')

        if isinstance(x, str):
            ids = []
            for j in self.objects.values():
                if j.name == x:
                    ids.append(j.id)
            if len(ids) > 1:
                raise RegistryKeyError('object "%s" not unique' % x)
            if len(ids) == 0:
                raise RegistryKeyError('object "%s" not found' % x)
            return self.objects[ids[0]]

        raise RegistryAccessError('Expected int or string (job name).')

    def __getslice__(self, i1, i2):
        import sys

        if i2 == sys.maxsize:
            endrange = ''
        else:
            endrange = str(i2)

        slice = self.__class__("%s[%d:%s]" % (self.name, i1, endrange))
        s = self.objects.items()[i1:i2]
        for id, obj in s:
            slice.objects[id] = obj
        return slice

    def _get_display_value(self, obj, item):
        def getatr(obj, members):
            val = getattr(obj, members[0])
            if len(members) > 1:
                return str(getatr(val, members[1:]))
            else:
                return str(val)
        try:
            try:
                f = self._display_columns_functions[item]
                val = f(obj)
            except KeyError:
                val = getatr(obj, item.split('.'))
            if not val and not item in self._display_columns_show_empty:
                val = ""
        except AttributeError:
            val = ""
        return str(val)

    def _display(self, interactive=0):
        from Ganga.Utility.ColourText import ANSIMarkup, NoMarkup, Effects
        if interactive:
            markup = ANSIMarkup()
        else:
            markup = NoMarkup()

        # default column width
        default_width = 10

        cnt = len(self)
        ds = "Registry Slice: %s (%d objects)\n" % (self.name, cnt)

        format = "#"
        flist = []
        for d in self._display_columns:
            width = self._display_columns_width.get(d, default_width)
            flist.append("%" + str(width) + "s ")
            #format += "%"+str(width)+"s  "
        format = "|".join(flist)
        format += "\n"

        if cnt > 0:
            ds += "--------------\n"
            ds += format % self._display_columns
            ds += "-" * len(format %
                            tuple([""] * len(self._display_columns))) + "\n"

        for obj in self.objects.values():
            colour = self._getColour(obj)

            vals = []
            for item in self._display_columns:
                width = self._display_columns_width.get(item, default_width)
                if obj._data is None and hasattr(obj, "_index_cache") and not obj._index_cache is None:
                    try:
                        if item == "fqid":
                            vals.append(
                                str(obj._index_cache["display:" + item]))
                        else:
                            vals.append(
                                str(obj._index_cache["display:" + item])[0:width])
                        continue
                    except KeyError:
                        pass
                if item == "fqid":
                    vals.append(self._get_display_value(obj, item))
                else:
                    vals.append(self._get_display_value(obj, item)[0:width])
            ds += markup(format % tuple(vals), colour)
        return ds

    __str__ = _display

    def _id(self):
        return id(self)
