from Ganga.Core import GangaException
from Ganga.GPIDev.Base.Objects import GangaObject, Node
from Ganga.GPIDev.Base.Filters import allComponentFilters
from Ganga.GPIDev.Base.Proxy import isProxy, addProxy, isType, getProxyAttr, stripProxy, TypeMismatchError, ReadOnlyObjectError, getName
from Ganga.GPIDev.Base.VPrinter import full_print, summary_print
from Ganga.GPIDev.Schema.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.GPIDev.Base.Objects import synchronised
from Ganga.Utility.util import containsGangaObjects
import copy
import sys
from functools import partial

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)


def makeGangaList(_list, mapfunction=None, parent=None, preparable=False, extra_args=None):
    """Should be used for makeing full gangalists
    Args:
        _list (list): This is the iterable list object which
        mapfunction (function): This is a function used to construct new elements based upon the elements in _list
        parent (GangaObject): This is the object to assign as the parent of the new GangaList (still needed)
        preparable (bool): Is the GangaList preparable?
        extra_args (dict): When defined this contains extra args to pass to mapfunction
    """

    # work with a simple list always
    if isinstance(_list, list):
        _list = _list
    elif isinstance(_list, GangaList):
        _list = getProxyAttr(_list, '_list')
    else:
        _list = [_list]

    if mapfunction is not None:
        if extra_args is None:
            _list = [mapfunction(l) for l in _list]
        else:
            new_mapfunction = partial(mapfunction, extra_args=extra_args)
            _list = [new_mapfunction(l) for l in _list]

    result = GangaList()
    # Subvert tests and modify the ._list here ourselves
    # This is potentially DANGEROUS if proxies aren't correctly stripped
    result._list.extend([stripProxy(l) for l in _list])
    result._is_preparable = preparable
    result._is_a_ref = False

    # set the parent if possible
    if parent is not None:
        result._setParent(parent)

    return result


def stripGangaList(_list):
    """Gets the underlying list of non-proxy objects
    Args:
        _list (list): This is the iterable list of objects which will have their Ganga proxies (if any) stripped
    """
    result = _list
    if isType(_list, GangaList):
        result = getProxyAttr(_list, '_list')
    return result


def makeGangaListByRef(_list, preparable=False):
    """Faster version of makeGangaList. Does not make a copy of _list but use it by reference.
    Args:
        _list (list): List of objects to add to a new Ganga list
        preparable (bool): Is the new object preparable?
    """
    result = GangaList()
    # Subvert tests and modify the ._list here ourselves
    # This is potentially DANGEROUS is proxies aren't correctly stripped
    result._list.extend(_list)
    result._is_a_ref = True
    result._is_preparable = preparable
    return result


def decorateListEntries(entries, typename):
    """
    Generate a small description of the list object
    Args:
        entries (int): Number of entries
        typename (str): Typename of the entries
    """
    return "[%d Entries of type '%s']" % (entries, typename)


class GangaListIter(object):

    """Simple wrapper around the listiterator"""

    def __init__(self, it):
        self.it = it

    def next(self):
        # TODO determine if this is correct or needed?
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
    _schema = Schema(Version(1, 0), {'_list': SimpleItem(defvalue=[], doc='The raw list', hidden=1, category='internal'),
                                     '_is_preparable': SimpleItem(defvalue=False, doc='defines if prepare lock is checked', hidden=1),
                                    })
    _enable_config = 1

    def __init__(self):
        self._is_a_ref = False
        super(GangaList, self).__init__()

    # convenience methods
    @staticmethod
    def is_list(obj):
        """
        This returns a boolean as to if this object is a list or not
        Args:
            obj (object): object to be tested against known list types
        """
        result = (obj is not None) and isType(obj, (GangaList, list, tuple))
        return result

    @staticmethod
    def has_proxy_element(_list):
        """
        Returns if a proxy object has crept into the list
        Args:
            _list (list): Any iterable object
        """
        return all([isProxy(l) for l in _list])

    ## Attempt to prevent raw assignment of _list causing Proxied objects to get inside the GangaList
    def _attribute_filter__set__(self, name, value):
        logger.debug("GangaList filter")
        if name == "_list":
            if self.is_list(value):
                if self.has_proxy_element(value):
                    returnable_list = [stripProxy(l) for l in value]
                    my_parent = self._getParent()
                    for elem in returnable_list:
                        if isinstance(elem, GangaObject):
                            elem._setParent(my_parent)
                        return
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
                else:
                    new_list.append(i)

            self._list = new_list
            self._is_a_ref = False
        return self

    def _getParent(self):
        return super(GangaList, self)._getParent()

    def _setParent(self, parent):
        """
        Set the parent of this object and it's children to the given parent
        Args:
            parent (GangaObject): Sets this object as the parent of the list and it's children
        """
        super(GangaList, self)._setParent(parent)
        for elem in self._list:
            if isinstance(elem, GangaObject):
                elem._setParent(parent)

    def get(self, to_match):
        def matching_filter(item):
            if '_list_get__match__' in dir(item):
                return item._list_get__match__(to_match)
            return to_match == item
        return makeGangaListByRef(filter(matching_filter, self._list), preparable=self._is_preparable)

    def _export_get(self, to_match):
        return addProxy(self.get(stripProxy(to_match)))

    def strip_proxy(self, obj, filter=False):
        """Removes proxies and calls shortcut if needed"""

        def applyFilter(obj, item):
            category = item['category']
            this_filter = allComponentFilters[category]
            filter_obj = this_filter(obj, item)
            if filter_obj is None:
                raise TypeMismatchError('%s is not of type %s.' % (str(obj), category))
            return filter_obj

        raw_obj = stripProxy(obj)
        # apply a filter if possible
        if filter is True:
            parent = self._getParent()
            item = self.findSchemaParentSchemaEntry(parent)
            if item and item.isA(ComponentItem):  # only filter ComponentItems
                category = item['category']
                if isinstance(raw_obj, GangaObject):
                    if raw_obj._category != category:
                        raw_obj = applyFilter(raw_obj, item)
                    raw_obj._setParent(parent)
                else:
                    raw_obj = applyFilter(raw_obj, item)
        return raw_obj

    def strip_proxy_list(self, obj_list, filter=False):

        if isType(obj_list, GangaList):
            return getProxyAttr(obj_list, '_list')
        result = [self.strip_proxy(l, filter=filter) for l in obj_list]
        return result

    def getCategory(self):
        """Returns a list of categories for the objects in the list. Returns [] for an empty list."""

        def return_cat(elem):
            if hasattr(elem, 'category'):
                return elem._category
            else:
                return type(elem)
        return unique([return_cat(l) for l in self._list])

    def _readonly(self):
        """
        Return if this object is read-only based upon the Schema
        """
        if self._is_preparable and hasattr(self, '_getParent'):
            if self._getParent()._category == 'applications' and hasattr(self._getParent(), 'is_prepared'):
                from Ganga.GPIDev.Lib.File.File import ShareDir
                return (isinstance(self._getParent().is_prepared, ShareDir) or super(GangaList, self)._readonly())
        return super(GangaList, self)._readonly()

    def checkReadOnly(self):
        """Puts a hook in to stop mutable access to readonly jobs."""
        if self._readonly():
            raise ReadOnlyObjectError(
                'object %s is readonly and attribute "%s" cannot be modified now' % (repr(self), getName(self)))
        else:
            self._getSessionLock()
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
        return addProxy(self.__add__(obj_list))

    def __contains__(self, obj):
        return self._list.__contains__(self.strip_proxy(obj))

    def __clone__(self):
        """ clone this object in a similar way to copy """
        # TODO deterine if silently calling __copy__ is more correct
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

    def __deepcopy__(self, memo=None):
        """Bypass any checking when making the copy"""
        #logger.info("memo: %s" % str(memo))
        #logger.info("self.len: %s" % str(len(self._list)))
        if self._list != []:
            return makeGangaListByRef(_list=copy.deepcopy(self._list, memo), preparable=self._is_preparable)
        else:
            new_list = GangaList()
            new_list._is_preparable = self._is_preparable
            return new_list

    def __getListToCompare(self, input_list):

        # if the arg isn't a list, just give it back
        if not self.is_list(self.strip_proxy(input_list)):
            return input_list

        # setup up the list correctly
        tmp_list = input_list
        if isType(input_list, GangaList):
            # GangaLists should never contain proxied objects so just return the list
            return stripProxy(input_list)._list
        elif isinstance(input_list, tuple):
            tmp_list = list(input_list)

        # Now return the list after stripping any objects of proxies
        return self.strip_proxy_list(tmp_list)

    def __eq__(self, obj_list):
        if obj_list is self:  # identity check
            return True
        return self._list == self.__getListToCompare(obj_list)

    def __ge__(self, obj_list):
        return self._list.__ge__(self.__getListToCompare(obj_list))

    def __getitem__(self, index):
        return self._list.__getitem__(index)

    def _export___getitem__(self, index):
        return addProxy(self.__getitem__(index))

    def __getslice__(self, start, end):
        return makeGangaList(_list=self._list.__getslice__(start, end), preparable=self._is_preparable)

    def _export___getslice__(self, start, end):
        return addProxy(self.__getslice__(start, end))

    def __gt__(self, obj_list):
        return self._list.__gt__(self.strip_proxy_list(obj_list))

    def __hash__(self):
        logger.info("hash")
        result = 0
        for element in result:
            result ^= hash(element)
        return result

    def __iadd__(self, obj_list):
        self._list.__iadd__(self.strip_proxy_list(obj_list, True))
        return self

    def _export___iadd__(self, obj_list):
        self.checkReadOnly()
        return addProxy(self.__iadd__(obj_list))

    def __imul__(self, number):
        self._list.__imul__(number)
        return self

    def _export___imul__(self, number):
        self.checkReadOnly()
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
        return addProxy(self.__mul__(number))

    def __ne__(self, obj_list):
        if obj_list is self:  # identity check
            return False
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
        cp = [i for i in self._export___iter__()]
        return obj + cp

    def __rmul__(self, number):
        return makeGangaList(self._list.__rmul__(number), preparable=self._is_preparable)

    def _export___rmul__(self, number):
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
        containsObj = False
        for elem in self._list:
            if isinstance(elem, GangaObject):
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
        if isType(obj, GangaList):
            stripped_o = stripProxy(obj)
            stripped_o._setParent(self._getParent())
            self._list.append(stripped_o)
            return
        elem = self.strip_proxy(obj, my_filter)
        list_objs = (list, tuple)
        if isType(elem, GangaObject):
            stripped_e = stripProxy(elem)
            stripped_e._setParent(self._getParent())
            self._list.append(stripped_e)
        elif isType(elem, list_objs):
            new_list = []
            def my_append(_obj):
                if isType(_obj, GangaObject):
                    stripped_o = stripProxy(_obj)
                    stripped_o._setParent(self._getParent())
                    return stripped_o
                else:
                    return _obj
            self._list.append([my_append(l) for l in elem])
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
        return addProxy(self.pop(index))

    def remove(self, obj):
        """
        Remove a given object from a list
        Args:
            obj (unknown): Remove this object from the list if it exists
        """
        self._list.remove(self.strip_proxy(obj))

    def _export_remove(self, obj):
        """
        Remove an object from a list after checking if it's read-only
        Args:
            obj (unknown): Remove this object from the list if it exists
        """
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
        """
        Args:
            compfunc (function): Function to be used in sorting list elements
        """
        self.checkReadOnly()
        self.sort(cmpfunc)

    # now some more ganga specific methods
    def findSchemaParentSchemaEntry(self, parent):
        """Finds the schema entry for this GangaList
        Args:
            parent (GangaObject): This is the parent object which controls this object
        """
        result = None
        if parent and parent._schema:
            for k, v in parent._schema.allItems():
                if getattr(parent, k) is self:
                    result = v
                    break
        return result

    def printSummaryTree(self, level=0, verbosity_level=0, whitespace_marker='', out=sys.stdout, selection='', interactive=False):
        """
        This funtion displays a summary of the contents of this file.
        (Docs from Gaga.GPIDev.Base.Objects # TODO determine how mch of this may be duplicated from there)
        Args:
            level (int): the hierachy level we are currently at in the object tree.
            verbosity_level (int): How verbose the print should be. Currently this is always 0.
            whitespace_marker (str): If printing on multiple lines, this allows the default indentation to be replicated.
                                     The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
            out (stream): An output stream to print to. The last line of output should be printed without a newline.'
            selection: See VPrinter for an explaintion of this.
            interactive (bool): Is this printing code being called from the interactive IPython prompt?
        """
        parent = self._getParent()
        schema_entry = self.findSchemaParentSchemaEntry(parent)

        if parent is None:
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
                out.write(decorateListEntries(self_len, getName(type(self[0]))))
                return
            else:
                summary_print(self, out)
                return

        out.write(str(self._list))
        return

    def toString(self):
        """Returns a simple str of the _list."""
        returnable_str = "["
        for element in self._list:
            if isType(element, GangaObject):
                returnable_str += repr(stripProxy(element))
            else:
                returnable_str += "'"
                returnable_str += str(stripProxy(element))
                returnable_str += "'"
            returnable_str += ", "
        returnable_str += "]"
        return returnable_str

    @synchronised
    def accept(self, visitor):
        """
        accept a visitor pattern - overrides GangaObject because we need to process _list as a ComponentItem in this
        case to allow save/load of nested lists to work.
        Could just get away with SimpleItem checks here but have included Shared and Component just in case these
        are added in the future

        # TODO investigate if it's possible to call GangaObject.accept for 90% of this functionality rather than duplicating it here

        This accepts a VPrinter or VStreamer class instance visitor which is used to display the contents of the object according the Schema
        Args:
            visitor (VPrinter, VStreamer): An instance which will produce a display of this contents of this object
        """
        visitor.nodeBegin(self)

        for (name, item) in self._schema.simpleItems():
            if name == "_list":
                visitor.componentAttribute(self, name, getattr(self, name), 1)
            elif item['visitable']:
                visitor.simpleAttribute(self, name, getattr(self, name), item['sequence'])

        for (name, item) in self._schema.sharedItems():
            if item['visitable']:
                visitor.sharedAttribute(self, name, getattr(self, name), item['sequence'])

        for (name, item) in self._schema.componentItems():
            if item['visitable']:
                visitor.componentAttribute(self, name, getattr(self, name), item['sequence'])

        visitor.nodeEnd(self)

    def _setFlushed(self):
        """Set flushed like the Node but do the _list by hand to avoid problems"""
        self._dirty = False
        for elem in self._list:
            if isinstance(elem, Node):
                elem._setFlushed()
        super(GangaList, self)._setFlushed()

# export to GPI moved to the Runtime bootstrap

