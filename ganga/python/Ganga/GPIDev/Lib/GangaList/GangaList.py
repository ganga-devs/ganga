from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Base.Filters import allComponentFilters
from Ganga.GPIDev.Base.Proxy import addProxy,isType,getProxyAttr,stripProxy, TypeMismatchError
from Ganga.GPIDev.Base.VPrinter import full_print
from Ganga.GPIDev.Schema.Schema import ComponentItem,Schema,SimpleItem,Version
from Ganga.Utility.Plugin.GangaPlugin import allPlugins
from Ganga.Utility.util import containsGangaObjects,isNestedList
from Ganga.GPIDev.Base.Proxy import ReadOnlyObjectError
import copy,sys

def makeGangaList(_list, mapfunction = None, parent = None):
    """Should be used for makeing full gangalists"""
    
    #work with a simple list always
    if isType(_list,list):
        _list = _list
    elif isType(_list,GangaList):
        _list = getProxyAttr(_list,'_list')
    else:
        _list = [_list]
    
    if mapfunction is not None:
        _list = map(mapfunction,_list)
    
    result = GangaList()
    result.extend(_list)
    
    #set the parent if possible
    if parent is not None:
        result._setParent(parent)
        
        for r in result:
            if isinstance(r,GangaObject) and r._getParent() is None:
                r._setParent(parent)
    
    return result

def stripGangaList(_list):
    """Gets the underlying list of non-proxy objects"""
    result = _list
    if isType(_list, GangaList):
        result = getProxyAttr(_list, '_list')
    return result

def makeGangaListByRef(_list):
    """Faster version of makeGangaList. Does not make a copy of _list but use it by reference."""
    result = GangaList()
    result._list = _list
    return result    

def decorateListEntries(entries, typename):
    return "[%d Entries of type '%s']" % (entries,typename)

class GangaListIter(object):
    """Simple wrapper around the listiterator"""
    def __init__(self, it):
        self.it = it
    def next(self):
        return addProxy(self.it.next())
    def __iter__(self):
        return self

class GangaList(GangaObject):
    
    _category = 'internal'
    _exportmethods = ['__add__', '__contains__', '__delitem__', '__delslice__', '__eq__', '__ge__',\
                      '__getitem__', '__getslice__', '__gt__', '__iadd__', '__imul__',\
                      '__iter__', '__le__', '__len__', '__lt__', '__mul__', '__ne__','__reversed__','__radd__','__rmul__',\
                      '__setitem__', '__setslice__', 'append', 'count', 'extend', 'index',\
                      'insert', 'pop', 'remove', 'reverse', 'sort','__hash__']
    _hidden = 1
    _enable_plugin = 1
    _name = 'GangaList'
    _schema = Schema(Version(1, 0), {
        '_list' : SimpleItem(defvalue=[], doc='The raw list', hidden = 1),
        })
    _enable_config = 1
    
    def __init__(self):
        super(GangaList, self).__init__()

    # convenience methods
    def is_list(self, obj):
        result = (obj != None) and (isType(obj, GangaList) or isinstance(obj,list))
        return result
    
    def strip_proxy(self, obj, filter = False):
        """Removes proxies and calls shortcut if needed"""
        
        def applyFilter(obj, item):
            category = item['category']
            filter = allComponentFilters[category]
            filter_obj = filter(obj,item)
            if filter_obj is None:
                raise TypeMismatchError('%s is not of type %s.' % (str(obj),category))
            return filter_obj
            
        obj = stripProxy(obj)
        #apply a filter if possible
        if filter:
            parent = self._getParent()
            item = self.findSchemaParentSchemaEntry(parent)
            if item and item.isA(ComponentItem):#only filter ComponentItems
                category = item['category']
                if isType(obj, GangaObject):
                    if obj._category != category:
                        obj = applyFilter(obj, item)
                    obj._setParent(parent)
                else:
                    obj = applyFilter(obj, item)
        return obj 
                

    def strip_proxy_list(self, obj_list, filter = False):
       
        if isType(obj_list, GangaList):
            return getProxyAttr(obj_list,'_list')
        result = []
        for o in obj_list:
            result.append(self.strip_proxy(o,filter))
        return result
       
    def getCategory(self):
        """Returns a list of categories for the objects in the list. Returns [] for an empty list."""

        result = []
        for o in self._list:
            category = o._category
            if not category in result:
                result.append(category)
        return result
    
    
    def checkReadOnly(self):
        """Puts a hook in to stop mutable access to readonly jobs."""
        if self._readonly():
            raise ReadOnlyObjectError('object %s is readonly and attribute "%s" cannot be modified now'%(repr(self),self._name))
        else:
            root = GangaObject.__getattribute__(self,'_getRoot')()
            root._setDirty(True)
            
    def checkNestedLists(self,value):
        """The rule is that if there are nested lists then they 
        must not contain GangaObjects, as this corrupts the repository"""
        if isNestedList(value) and containsGangaObjects(value):
            raise TypeMismatchError('Assigning nested lists which contain Ganga GPI Objects is not supported.')
    
    # list methods
    # All list methods should be overridden in a way that makes
    # sure that no proxy objects end up in the list, and no
    # unproxied objects make it out.
    
    def __add__(self, obj_list):
        #Savanah 32342
        if not self.is_list(obj_list):
            raise TypeError('Type %s can not be concatinated to a GangaList' % type(obj_list))
        
        return makeGangaList(self._list.__add__(self.strip_proxy_list(obj_list, True)))   
    def _export___add__(self, obj_list):
        self.checkReadOnly()
        self.checkNestedLists(obj_list)
        return addProxy(self.__add__(obj_list))
    
    def __contains__(self, obj):
        return self._list.__contains__(self.strip_proxy(obj))
    
    def __copy__(self):
        """Bypass any checking when making the copy"""
        return makeGangaList(_list = copy.copy(self._list))
       
    def __delitem__(self, obj):
        self._list.__delitem__(self.strip_proxy(obj))
    def _export___delitem__(self,obj):
        self.checkReadOnly()
        self.__delitem__(obj)
        
    def __delslice__(self, start, end):
        self._list.__delslice__(start, end)
    def _export___delslice__(self, start, end):
        self.checkReadOnly()
        self.__delslice__(start,end)
        
    def __deepcopy__(self, memo):
        """Bypass any checking when making the copy"""
        return makeGangaList(_list = copy.deepcopy(self._list, memo))
       
    def __eq__(self, obj_list):
        if obj_list is self:#identity check
            return True
        result = False    
        if self.is_list(self.strip_proxy(obj_list)):
            result = self._list.__eq__(self.strip_proxy_list(obj_list))
        return result
       
    def __ge__(self, obj_list):
        return self._list.__ge__(self.strip_proxy_list(obj_list))
       
    def __getitem__(self, index):
        return self._list.__getitem__(index)
    def _export___getitem__(self, index):
        return addProxy(self.__getitem__(index))
       
    def __getslice__(self, start, end):
        return makeGangaList(_list = self._list.__getslice__(start, end))
    def _export___getslice__(self, start, end):
        return addProxy(self.__getslice__(start, end))
       
    def __gt__(self, obj_list):
        return self._list.__gt__(self.strip_proxy_list(obj_list))
    
    def __hash__(self):
        #will always throw an error
        return self._list.__hash__()
       
    def __iadd__(self, obj_list):
        self._list.__iadd__(self.strip_proxy_list(obj_list, True))
        return self
    def _export___iadd__(self, obj_list):
        self.checkReadOnly()
        self.checkNestedLists(obj_list)
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
        return makeGangaList(self._list.__mul__(number))
    def _export___mul__(self, number):
        return addProxy(self.__mul__(number))
       
    def __ne__(self, obj_list):
        if obj_list is self:#identity check
            return True
        result = True   
        if  self.is_list(obj_list):
            result = self._list.__ne__(self.strip_proxy_list(obj_list))
        return result
        
    def __reversed__(self):
        """Implements the __reversed__ list method introduced in 2.4"""

        try:
            return reversed(self._list)
        except NameError:
            #workaround for pythion < 2.4
            class GangaReverseIter(object):
                """Simple wrapper around the list"""
                def __init__(self, data):
                    self.data = data
                    self.count = len(self.data) - 1
                def next(self):
                    self.count -= 1
                    if self.count:
                        result = self.data[self.count + 1]
                        return result
                    else:
                        raise StopIteration()
                    
                def __iter__(self):
                    return self
            
            return GangaReverseIter(self._list)
            
    def _export___reversed__(self):
       return GangaListIter(self.__reversed__())
    
    def __radd__(self, obj):
        return obj + self._list
    def _export___radd__(self, obj):
        #return the proxied objects
        cp = []
        for i in self._export___iter__():
            cp.append(i)
        return obj + cp
       
    def __rmul__(self, number):
        return makeGangaList(self._list.__rmul__(number))
    def _export___rmul__(self, number):
        return addProxy(self.__rmul__(number))
       
    def __setitem__(self, index, obj):
        self._list.__setitem__(index, self.strip_proxy(obj, True))
    def _export___setitem__(self, index, obj):
        self.checkReadOnly()
        self.checkNestedLists(obj)
        self.__setitem__(index, obj)
       
    def __setslice__(self, start, end, obj_list):
        self._list.__setslice__(start, end, self.strip_proxy_list(obj_list, True))
    def _export___setslice__(self, start, end, obj_list):
        self.checkReadOnly()
        self.checkNestedLists(obj_list)
        self.__setslice__(start,end,obj_list)
        
    def __repr__(self):
        return self.toString()
    def __str__(self):
        return self.__repr__()
           
    def append(self, obj):
        self._list.append(self.strip_proxy(obj, True))
    def _export_append(self, obj):
        self.checkReadOnly()
        self.checkNestedLists(obj)
        self.append(obj)
        
    def count(self, obj):
        return self._list.count(self.strip_proxy(obj))
       
    def extend(self, ittr):
        for i in ittr:
            self.append(i)
    def _export_extend(self, ittr):
        self.checkReadOnly()
        self.checkNestedLists(ittr)
        self.extend(ittr)
       
    def index(self, obj):
        return self._list.index(self.strip_proxy(obj))
       
    def insert(self, index, obj):
        self._list.insert(index, self.strip_proxy(obj, True))
    def _export_insert(self, index, obj):
        self.checkReadOnly()
        self.checkNestedLists(obj)
        self.insert(index, obj)
       
    def pop(self, index = -1):
        return self._list.pop(index)
    def _export_pop(self, index = -1):
        self.checkReadOnly()
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
        
    def sort(self, cmpfunc = None):
        #TODO: Should comparitor have access to unproxied objects?
        self._list.sort(cmpfunc)
    def _export_sort(self, cmpfunc = None):
        self.checkReadOnly()
        self.sort(cmpfunc)
        
    #now some more ganga specific methods
    def findSchemaParentSchemaEntry(self, parent):
        """Finds the schema entry for this GangaList"""
        result = None
        if parent and parent._schema:
            for k, v in parent._schema.allItems():
                if getattr(parent,k) is self:
                    result = v
                    break
        return result
        
    def printSummaryTree(self,level = 0, verbosity_level = 0, whitespace_marker = '', out = sys.stdout, selection = ''):
        parent = self._getParent()
        schema_entry = self.findSchemaParentSchemaEntry(parent)
        
        if parent is None:
            full_print(self,out)
            return
        
        if schema_entry:
            self_len = len(self)
            print_summary = schema_entry['summary_print']
            maxLen = schema_entry['summary_sequence_maxlen']
            
            if print_summary:
                fp = getattr(parent,print_summary)
                str_val = fp(self._list,verbosity_level)
                print >>out, str_val,
                return
            
            if (maxLen != -1) and (self_len > maxLen):
                print >>out, decorateListEntries(self_len, type(self[0]).__name__),
                return

        print >>out, str(self._list),
        return
    
    def toString(self):
        """Returns a simple str of the _list."""
        return str(self._list)

