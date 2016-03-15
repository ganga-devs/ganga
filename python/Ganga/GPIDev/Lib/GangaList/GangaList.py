from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility.util import containsGangaObjects
import copy
import sys

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


def makeGangaList(_list, mapfunction=None, parent=None, preparable=False):
    """Should be used for makeing full gangalists"""

    from Ganga.GPIDev.Base.Proxy import isType, getProxyAttr

    # work with a simple list always
    if isType(_list, list):
        _list = _list
    elif isType(_list, GangaList):
        _list = getProxyAttr(_list, '_list')
    else:
        _list = [_list]

    #logger.debug("_list: %s" % str(_list))

    if mapfunction is not None:
        _list = map(mapfunction, _list)

    #logger.debug("Making a GangaList of size: %s" % str(len(_list)))
    result = makeGangaListByRef(_list, preparable)
    result._is_a_ref = False

    # set the parent if possible
    if parent is not None:
        result._setParent(parent)

    return result


def stripGangaList(_list):
    """Gets the underlying list of non-proxy objects"""
    from Ganga.GPIDev.Base.Proxy import isType, getProxyAttr
    result = _list
    if isType(_list, GangaList):
        result = getProxyAttr(_list, '_list')
    return result


def makeGangaListByRef(_list, preparable=False):
    """Faster version of makeGangaList. Does not make a copy of _list but use it by reference."""
    from Ganga.GPIDev.Base.Proxy import stripProxy
    result = GangaList()
    if len(_list) == 0:
        return result
    temp_list = [stripProxy(element) for element in _list]
    result._list = temp_list
    result._is_a_ref = True
    result._is_preparable = preparable
    return result


def decorateListEntries(entries, typename):
    return "[%d Entries of type '%s']" % (entries, typename)


class GangaListIter(object):

    """Simple wrapper around the listiterator"""

    def __init__(self, it):
        self.it = it

    def next(self):
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(next(self.it))

    def __iter__(self):
        return self

class GangaList(GangaObject):

    _category = 'internal'
    _exportmethods = ['__add__', '__contains__', '__delitem__', '__delslice__', '__eq__', '__ge__',
                      '__getitem__', '__getslice__', '__gt__', '__iadd__', '__imul__',
                      '__iter__', '__le__', '__len__', '__lt__', '__mul__', '__ne__', '__reversed__', '__radd__', '__rmul__',
                      '__setitem__', '__setslice__', 'append', 'count', 'extend', 'index',
                      'insert', 'pop', 'remove', 'reverse', 'sort', '__hash__', 'get']
    _hidden = 1
    _enable_plugin = 1
    _name = 'GangaList'
    _schema = Schema(Version(1, 0), {'_list': ComponentItem(defvalue=[], doc='The raw list', hidden=1, category='internal'),
                                     '_is_preparable': SimpleItem(defvalue=False, doc='defines if prepare lock is checked', hidden=1),
                                    })
    _enable_config = 1
    _data={}

    def __init__(self):
        self._is_a_ref = False
        super(GangaList, self).__init__()

    def __construct__(self, args):

        #super(GangaList, self).__construct__(args)

        if len(args) == 1:
            from Ganga.GPIDev.Base.Proxy import isType
            if isType(args[0], (len, GangaList, tuple)):
                for element_i in args[0]:
                    self._list.expand(self.strip_proxy(element_i))
            elif args[0] is None:
                self._list = None
            else:
                raise GangaException("Construct: Attempting to assign a non list item: %s to a GangaList._list!" % str(args[0]))
        else:
            super(GangaList, self).__construct__(args)

        return

    # convenience methods
    @staticmethod
    def is_list(obj):
        from Ganga.GPIDev.Base.Proxy import isType
        result = (obj is not None) and isType(obj, (GangaList, list, tuple))
        return result

    @staticmethod
    def has_proxy_element(_list):
        from Ganga.GPIDev.Base.Proxy import isProxy
        return all([isProxy(element) for element in _list])

    ## Attempt to prevent raw assignment of _list causing Proxied objects to get inside the GangaList
    def _attribute_filter__set__(self, name, value):
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        logger.debug("GangaList filter")
        if name == "_list":
            if self.is_list(value):
                if self.has_proxy_element(value):
                    returnable_list = [stripProxy(element) for element in value]
                    for elem in returnable_list:
                        if isType(elem, GangaObject):
                            elem._setParent(self._getParent())
                    return returnable_list
                else:
                    return value
            elif self._list is None:
                return None
            else:
                raise GangaException("Attempting to assign a non list item: %s to a GangaList._list!" % str(value))
        else:
            return super(GangaList, self)._attribute_filter__set__(name, value)

    def _on_attribute__set__(self, obj_type, attrib_name):
        if self._is_a_ref is True:
            new_list = []
            for i in self._list:
                if hasattr(i, '_on_attribute__set__'):
                    new_list.append(i._on_attribute__set__(obj_type, attrib_name))
                    continue
                new_list.append(i)
            self._list = new_list
            self._is_a_ref = False
        return self

    def _getParent(self):
        return super(GangaList, self)._getParent()

    def _setParent(self, parent):
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        super(GangaList, self)._setParent(parent)
        for element in self._list:
            if isType(element, GangaObject):
                stripProxy(element)._setParent(parent)

    def get(self, to_match):
        def matching_filter(item):
            if '_list_get__match__' in dir(item):
                return item._list_get__match__(to_match)
            return to_match == item
        return makeGangaListByRef(filter(matching_filter, self._list), preparable=self._is_preparable)

    def _export_get(self, to_match):
        from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
        return addProxy(self.get(stripProxy(to_match)))

    def strip_proxy(self, obj, filter=False):
        """Removes proxies and calls shortcut if needed"""
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        def applyFilter(obj, item):
            category = item['category']
            from Ganga.GPIDev.Base.Filters import allComponentFilters
            this_filter = allComponentFilters[category]
            filter_obj = this_filter(obj, item)
            if filter_obj is None:
                from Ganga.GPIDev.Base.Proxy import TypeMismatchError
                raise TypeMismatchError('%s is not of type %s.' % (str(obj), category))
            return filter_obj

        raw_obj = stripProxy(obj)
        # apply a filter if possible
        if filter is True:
            parent = self._getParent()
            item = self.findSchemaParentSchemaEntry(parent)
            if item and item.isA(ComponentItem):  # only filter ComponentItems
                category = item['category']

                if isType(raw_obj, GangaObject):
                    if raw_obj._category != category:
                        raw_obj = applyFilter(raw_obj, item)
                    raw_obj._setParent(parent)
                else:
                    raw_obj = applyFilter(raw_obj, item)
        return raw_obj

    def strip_proxy_list(self, obj_list, filter=False):
        from Ganga.GPIDev.Base.Proxy import isType, getProxyAttr
        if isType(obj_list, GangaList):
            return getProxyAttr(obj_list, '_list')
        result = []
        for o in obj_list:
            result.append(self.strip_proxy(o, filter))
        return result

    def getCategory(self):
        """Returns a list of categories for the objects in the list. Returns [] for an empty list."""
        from Ganga.GPIDev.Base.Proxy import stripProxy
        result = []
        for o in self._list:
            if hasattr(stripProxy(o), 'category'):
                category = o._category
                if not category in result:
                    result.append(category)
            else:
                result.append(type(o))
        return result

    def _readonly(self):
        from Ganga.GPIDev.Base.Proxy import isType
        if self._is_preparable and hasattr(self, '_getParent'):
            if self._getParent()._category == 'applications' and hasattr(self._getParent(), 'is_prepared'):
                from Ganga.GPIDev.Lib.File.File import ShareDir
                return (isType(self._getParent().is_prepared, ShareDir) or super(GangaList, self)._readonly())
        return super(GangaList, self)._readonly()

    def checkReadOnly(self):
        """Puts a hook in to stop mutable access to readonly jobs."""
        if self._readonly():
            from Ganga.GPIDeb.Base.Proxy import ReadOnlyObjectError, getName
            raise ReadOnlyObjectError('object %s is readonly and attribute "%s" cannot be modified now' % (repr(self), getName(self)))
        else:
            self._getWriteAccess()
            # TODO: BUG: This should only be set _after_ the change has been
            # done! This can lead to data loss!
            self._setDirty()

    # list methods
    # All list methods should be overridden in a way that makes
    # sure that no proxy objects end up in the list, and no
    # unproxied objects make it out.

    def __add__(self, obj_list):
        # Savanah 32342
        if not self.is_list(obj_list):
            raise TypeError('Type %s can not be concatinated to a GangaList' % type(obj_list))

        return makeGangaList(self._list.__add__(self.strip_proxy_list(obj_list, True)), preparable=self._is_preparable)

    def _export___add__(self, obj_list):
        self.checkReadOnly()
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.__add__(obj_list))

    def __contains__(self, obj):
        return self._list.__contains__(self.strip_proxy(obj))

    def __clone__(self):
        return makeGangaListByRef(_list=copy.copy(self._list), preparable=self._is_preparable)

    def __copy__(self):
        """Bypass any checking when making the copy"""
        return makeGangaListByRef(_list=copy.copy(self._list), preparable=self._is_preparable)

    def __delitem__(self, obj):
        self._list.__delitem__(self.strip_proxy(obj))

    def _export___delitem__(self, obj):
        self.checkReadOnly()
        self.__delitem__(obj)

    def __delslice__(self, start, end):
        self._list.__delslice__(start, end)

    def _export___delslice__(self, start, end):
        self.checkReadOnly()
        self.__delslice__(start, end)

    def __deepcopy__(self, memo):
        """Bypass any checking when making the copy"""
        #logger.info("memo: %s" % str(memo))
        #logger.info("self.len: %s" % str(len(self._list)))
        if self._list != []:
            return makeGangaListByRef(_list=copy.deepcopy(self._list), preparable=self._is_preparable)
        else:
            new_list = GangaList()
            new_list._is_preparable = self._is_preparable
            return new_list

    @staticmethod
    def __getListToCompare(input_list):
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        if isType(input_list, GangaList):
            return stripProxy(input_list)._list
        elif isinstance(input_list, tuple):
            return list(input_list)
        else:
            return input_list

    def __eq__(self, obj_list):
        if obj_list is self:  # identity check
            return True
        return self._list == self.__getListToCompare(obj_list)

    def __ge__(self, obj_list):
        return self._list.__ge__(self.__getListToCompare(obj_list))

    def __getitem__(self, index):
        return self._list.__getitem__(index)

    def _export___getitem__(self, index):
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.__getitem__(index))

    def __getslice__(self, start, end):
        return makeGangaList(_list=self._list.__getslice__(start, end), preparable=self._is_preparable)

    def _export___getslice__(self, start, end):
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.__getslice__(start, end))

    def __gt__(self, obj_list):
        return self._list.__gt__(self.strip_proxy_list(obj_list))

    def __hash__(self):
        logger.info("hash")
        result = 0
        for element in self:
            result ^= hash(element)
        return result
        # return self._list.__hash__()

    def __iadd__(self, obj_list):
        self._list.__iadd__(self.strip_proxy_list(obj_list, True))
        return self

    def _export___iadd__(self, obj_list):
        self.checkReadOnly()
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.__iadd__(obj_list))

    def __imul__(self, number):
        self._list.__imul__(number)
        return self

    def _export___imul__(self, number):
        self.checkReadOnly()
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.__imul__(number))

    def __iter__(self):
        return self._list.__iter__()

    def _export___iter__(self):
        return GangaListIter(iter(self._list))

    def __le__(self, obj_list):
        return self._list.__le__(self.strip_proxy_list(obj_list))

    def __len__(self):
        return len(self._list)

    def __lt__(self, obj_list):
        return self._list.__lt__(self.strip_proxy_list(obj_list))

    def __mul__(self, number):
        return makeGangaList(self._list.__mul__(number), preparable=self._is_preparable)

    def _export___mul__(self, number):
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.__mul__(number))

    def __ne__(self, obj_list):
        if obj_list is self:  # identity check
            return True
        result = True
        if self.is_list(obj_list):
            result = self._list.__ne__(self.strip_proxy_list(obj_list))
        return result

    def __reversed__(self):
        """Implements the __reversed__ list method introduced in 2.4"""
        return reversed(self._list)

    def _export___reversed__(self):
        return GangaListIter(self.__reversed__())

    def __radd__(self, obj):
        return obj + self._list

    def _export___radd__(self, obj):
        # return the proxied objects
        cp = []
        for i in self._export___iter__():
            cp.append(i)
        return obj + cp

    def __rmul__(self, number):
        return makeGangaList(self._list.__rmul__(number), preparable=self._is_preparable)

    def _export___rmul__(self, number):
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.__rmul__(number))

    def __setitem__(self, index, obj):
        self._list.__setitem__(index, self.strip_proxy(obj, True))

    def _export___setitem__(self, index, obj):
        self.checkReadOnly()
        self.__setitem__(index, obj)

    def __setslice__(self, start, end, obj_list):
        self._list.__setslice__(start, end, self.strip_proxy_list(obj_list, True))

    def _export___setslice__(self, start, end, obj_list):
        self.checkReadOnly()
        self.__setslice__(start, end, obj_list)

    def __repr__(self):
        #logger.info("__repr__")
        #return self.toString()
        #import traceback
        #traceback.print_stack()
        from Ganga.GPIDev.Base.Proxy import isType
        containsObj = False
        for elem in self._list:
            if isType(elem, GangaObject):
                containsObj = True
                break
        if containsObj is False:
            return self.toString()
        else:
            return str("<GangaList at: %s>" % str(hex(abs(id(self)))))
        #return str("<GangaList at: %s>" % str(hex(abs(id(self)))))

    def __str__(self):
        #logger.info("__str__")
        return self.toString()

    def append(self, obj, my_filter=True):
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        if isType(obj, GangaList):
            self._list.append(stripProxy(obj))
            return
        elem = self.strip_proxy(obj, my_filter)
        list_objs = (list, tuple)
        if isType(elem, GangaObject):
            stripProxy(elem)._setParent(self._getParent())
            self._list.append(elem)
        elif isinstance(elem, list_objs):
            new_list = []
            for _obj in elem:
                if isType(_obj, GangaObject):
                    new_list.append(stripProxy(_obj))
                    stripProxy(_obj)._setParent(self._getParent())
                else:
                    new_list.append(_obj)
            self._list.append(new_list)
        else:
            self._list.append(elem)

    def _export_append(self, obj):
        self.checkReadOnly()
        self.append(obj)

    def count(self, obj):
        return self._list.count(self.strip_proxy(obj))

    def extend(self, ittr):
        for i in ittr:
            self.append(i)

    def _export_extend(self, ittr):
        self.checkReadOnly()
        self.extend(ittr)

    def index(self, obj):
        return self._list.index(self.strip_proxy(obj))

    def insert(self, index, obj):
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        if isType(obj, GangaObject):
            stripProxy(obj)._setParent(stripProxy(self)._getParent())
        self._list.insert(index, self.strip_proxy(obj, True))

    def _export_insert(self, index, obj):
        self.checkReadOnly()
        self.insert(index, obj)

    def pop(self, index=-1):
        return self._list.pop(index)

    def _export_pop(self, index=-1):
        self.checkReadOnly()
        from Ganga.GPIDev.Base.Proxy import addProxy
        return addProxy(self.pop(index))

    def remove(self, obj):
        self._list.remove(self.strip_proxy(obj))

    def _export_remove(self, obj):
        self.checkReadOnly()
        self.remove(obj)

    def reverse(self):
        self._list.reverse()

    def _export_reverse(self):
        self.checkReadOnly()
        self.reverse()

    def sort(self, cmpfunc=None):
        # TODO: Should comparitor have access to unproxied objects?
        self._list.sort(cmpfunc)

    def _export_sort(self, cmpfunc=None):
        self.checkReadOnly()
        self.sort(cmpfunc)

    # now some more ganga specific methods
    def findSchemaParentSchemaEntry(self, parent):
        """Finds the schema entry for this GangaList"""
        result = None
        if parent and parent._schema:
            for k, v in parent._schema.allItems():
                if getattr(parent, k) is self:
                    result = v
                    break
        return result

    def printSummaryTree(self, level=0, verbosity_level=0, whitespace_marker='', out=sys.stdout, selection='', interactive=False):
        parent = self._getParent()
        schema_entry = self.findSchemaParentSchemaEntry(parent)

        if parent is None:
            from Ganga.GPIDev.Base.VPrinter import full_print
            full_print(self, out)
            return

        if schema_entry:
            self_len = len(self)
            print_summary = schema_entry['summary_print']
            maxLen = schema_entry['summary_sequence_maxlen']

            if print_summary:
                fp = getattr(parent, print_summary)
                str_val = fp(self._list, verbosity_level)
                out.write(str_val)
                return

            if (maxLen != -1) and (self_len > maxLen):
                from Ganga.GPIDev.Base.Proxy import getName
                out.write(decorateListEntries(self_len, getName(type(self[0]))))
                return
            else:
                from Ganga.GPIDev.Base.VPrinter import summary_print
                summary_print(self, out)
                return

        out.write(str(self._list))
        return

    def toString(self):
        """Returns a simple str of the _list."""
        from Ganga.GPIDev.Base.Proxy import isType, stripProxy
        returnable_str = "["
        for element in self._list:
            if isType( element, GangaObject):
                returnable_str += repr(stripProxy(element))
            else:
                returnable_str += "'"
                returnable_str += str(stripProxy(element))
                returnable_str += "'"
            returnable_str += ", "
        returnable_str += "]"
        return returnable_str


#from Ganga.Runtime.GPIexport import exportToGPI
#exportToGPI('GangaList', GangaList, 'Classes')

