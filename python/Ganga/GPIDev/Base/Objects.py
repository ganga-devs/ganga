##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Objects.py,v 1.5.2.10 2009-07-24 13:35:53 ebke Exp $
##########################################################################
# NOTE: Make sure that _data and __dict__ of any GangaObject are only referenced
# here - this is necessary for write locking and lazy loading!
# IN THIS FILE:
# * Make sure every time _data or __dict__ is accessed that _data is not None. If yes, do:
#    obj._getReadAccess()
# * Make sure every write access is preceded with:
#    obj._getWriteAccess()
#   and followed by
#    obj._setDirty()

import threading
from contextlib import contextmanager
import functools

import Ganga.Utility.logging

from copy import deepcopy, copy
import inspect

import Ganga.GPIDev.Schema as Schema

from Ganga.Core.exceptions import GangaValueError, GangaException

from Ganga.Utility.Plugin import allPlugins

def _getName(obj):
    """ Return the name of an object based on what we prioritise"""
    returnable = getattr(obj, '_name', getattr(obj, '__name__', None))
    if returnable is None:
        returnable = getattr(getattr(obj, '__class__', None), '__name__', None)
    if returnable is None:
        returnable = str(obj)
    return returnable

logger = Ganga.Utility.logging.getLogger(modulename=1)

_imported_GangaList = None

do_not_copy = ['_index_cache', '_parent', '_registry', '_data', '_lock', '_proxyObject']

def _getGangaList():
    global _imported_GangaList
    if _imported_GangaList is None:
        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
        _imported_GangaList = GangaList
    return _imported_GangaList


def synchronised(f):
    """
    This decorator must be attached to a method on a ``Node`` subclass
    It uses the object's lock to make sure that the object is held for the duration of the decorated function
    """
    @functools.wraps(f)
    def decorated(self, *args, **kwargs):
        with self.lock:
            return f(self, *args, **kwargs)
    return decorated


class Node(object):
    _ref_list = ['_parent', '_registry', '_index_cache']

    def __init__(self, parent=None):
        self._data = {}
        self._parent = parent
        self._index_cache = {}
        self._lock = threading.RLock()
        super(Node, self).__init__()
        #logger.info("Node __init__")

    def __getstate__(self):
        d = self.__dict__
        d['_data'] = d['_data'].copy()
        for r in self._ref_list:
            d[r] = None
        return d

    def __setstate__(self, this_dict):
        for key, val in this_dict['_data'].iteritems():
            if isinstance(val, Node) and key not in self._ref_list:
                val._setParent(self)

        for attr in self._ref_list:
            if not hasattr(self, attr):
                setattr(self, attr, None)

        for key, val in this_dict.iteritems():
            setattr(self, key, val)

    def __copy__(self, memo=None):
        cls = self.__class__
        obj = cls()
        # FIXME: this is different than for deepcopy... is this really correct?
        this_dict = self.__dict__.copy()
        global do_not_copy
        for elem in this_dict.keys():
            if elem not in do_not_copy:
                this_dict[elem] = copy(this_dict[elem])
            else:
                this_dict[elem] = None
        #obj.__setstate__(this_dict)
        obj._setParent(self._getParent())
        setattr(obj, '_index_cache', {})
        setattr(obj, '_registry', self._registry)
        return obj

    def __deepcopy__(self, memo=None):
        cls = self.__class__
        obj = cls()
        this_dict = self.__getstate__()
        global do_not_copy
        for elem in this_dict.keys():
            if elem not in do_not_copy:
                this_dict[elem] = deepcopy(this_dict[elem], memo)  # FIXED

        obj.__dict__ = this_dict
        if self._getParent() is not None:
            obj._setParent(self._getParent())
        setattr(obj, '_registry', self._registry)
        return obj

    @synchronised
    def _getParent(self):
        return self._parent

    @synchronised
    def _setParent(self, parent):
        setattr(self, '_parent', parent)

    @property
    @contextmanager
    def lock(self):
        """
        This is a context manager which acquires the lock on the object and all it's ancestors.
        When the context manager exits, all the locks are released in the reverse order to that which they were acquired.
        """
        self._lock.acquire()
        ancestors = []
        p = self._parent
        while p is not None:
            p._lock.acquire()
            ancestors.append(p)
            p = p._parent
        try:
            yield
        finally:
            for p in reversed(ancestors):
                p._lock.release()
            self._lock.release()

    # get the root of the object tree
    # if parent does not exist then the root is the 'self' object
    # cond is an optional function which may cut the search path: when it
    # returns True, then the parent is returned as root
    def _getRoot(self, cond=None):
        if self._getParent() is None:
            return self
        root = None
        obj = self
        cond_test = cond is not None
        while obj is not None:
            root = obj
            if cond_test:
                if cond(root):
                    break
            obj = obj._getParent()
        return root

    def _getdata(self, name):
        #logger.debug("Getting: %s" % name)
        if hasattr(self, name):
            return getattr(self, name)
        else:
            if name in self._data:
                return self._data[name]
            else:
                return None

    # accept a visitor pattern
    @synchronised
    def accept(self, visitor):

        if not hasattr(self, '_schema'):
            return
        elif self._schema is None:
            visitor.nodeBegin(self)
            visitor.nodeEnd(self)
            return

        visitor.nodeBegin(self)

        for (name, item) in self._schema.simpleItems():
            if item['visitable']:
                visitor.simpleAttribute(self, name, self._getdata(name), item['sequence'])

        for (name, item) in self._schema.sharedItems():
            if item['visitable']:
                visitor.sharedAttribute(self, name, self._getdata(name), item['sequence'])

        for (name, item) in self._schema.componentItems():
            if item['visitable']:
                visitor.componentAttribute(self, name, self._getdata(name), item['sequence'])

        visitor.nodeEnd(self)

    # clone self and return a properly initialized object
    def clone(self):
        new_obj = deepcopy(self)

        return new_obj

    # copy all the properties recursively from the srcobj
    # if schema of self and srcobj are not compatible raises a ValueError
    # ON FAILURE LEAVES SELF IN INCONSISTENT STATE
    def copyFrom(self, srcobj, _ignore_atts=None):

        if _ignore_atts is None:
            _ignore_atts = []
        _srcobj = srcobj
        # Check if this object is derived from the source object, then the copy
        # will not throw away information

        if not hasattr(_srcobj, '__class__') and not inspect.isclass(_srcobj.__class__):
            raise GangaValueError("Can't copyFrom a non-class object: %s isclass: %s" % (str(_srcobj), str(inspect.isclass(_srcobj))))

        if not isinstance(self, _srcobj.__class__) and not isinstance(_srcobj, self.__class__):
            raise GangaValueError("copyFrom: Cannot copy from %s to %s!" % (_getName(_srcobj), _getName(self)))

        if not hasattr(self, '_schema'):
            logger.debug("No Schema found for myself")
            return

        if self._schema is None and _srcobj._schema is None:
            logger.debug("Schema object for one of these classes is None!")
            return

        if _srcobj._schema is None:
            self._schema = None
            return

        self._actually_copyFrom(_srcobj, _ignore_atts)

        ## Fix some objects losing parent knowledge
        src_dict = srcobj.__dict__
        for key, val in src_dict.iteritems():
            this_attr = getattr(srcobj, key)
            if isinstance(this_attr, Node) and key not in Node._ref_list:
                #logger.debug("k: %s  Parent: %s" % (str(key), (srcobj)))
                this_attr._setParent(srcobj)

    def _actually_copyFrom(self, _srcobj, _ignore_atts):

        for name, item in self._schema.allItems():
            if name in _ignore_atts:
                continue

            #logger.debug("Copying: %s : %s" % (str(name), str(item)))
            if name == 'application' and hasattr(_srcobj.application, 'is_prepared'):
                _app = _srcobj.application
                if _app.is_prepared not in [None, True]:
                    _app.incrementShareCounter(_app.is_prepared.name)

            if not self._schema.hasAttribute(name):
                #raise ValueError('copyFrom: incompatible schema: source=%s destination=%s'%(_getName(_srcobj), _getName(self)))
                if not hasattr(self, name):
                    setattr(self, name, self._schema.getDefaultValue(name))
                this_attr = getattr(self, name)
                if isinstance(this_attr, Node) and name not in Node._ref_list:
                    this_attr._setParent(self)
            elif not item['copyable']: ## Default of '1' instead of True...
                if not hasattr(self, name):
                    setattr(self, name, self._schema.getDefaultValue(name))
                this_attr = getattr(self, name)
                if isinstance(this_attr, Node) and name not in Node._ref_list:
                    this_attr._setParent(self)
            else:
                copy_obj = deepcopy(getattr(_srcobj, name))
                setattr(self, name, copy_obj)

    def printTree(self, f=None, sel=''):
        from Ganga.GPIDev.Base.VPrinter import VPrinter
        self.accept(VPrinter(f, sel))

    #printPrepTree is only ever run on applications, from within IPrepareApp.py
    #if you (manually) try to run printPrepTree on anything other than an application, it will not work as expected
    #see the relevant code in VPrinter to understand why
    def printPrepTree(self, f=None, sel='preparable' ):
        ## After fixing some bugs we are left with incompatible job hashes. This should be addressd before removing
        ## This particular class!
        from Ganga.GPIDev.Base.VPrinterOld import VPrinterOld
        self.accept(VPrinterOld(f, sel))

    def printSummaryTree(self, level=0, verbosity_level=0, whitespace_marker='', out=None, selection='', interactive=False):
        """If this method is overridden, the following should be noted:

        level: the hierachy level we are currently at in the object tree.
        verbosity_level: How verbose the print should be. Currently this is always 0.
        whitespace_marker: If printing on multiple lines, this allows the default indentation to be replicated.
                           The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
        out: An output stream to print to. The last line of output should be printed without a newline.'
        selection: See VPrinter for an explaintion of this.
        """
        from Ganga.GPIDev.Base.VPrinter import VSummaryPrinter
        self.accept(VSummaryPrinter(level, verbosity_level, whitespace_marker, out, selection, interactive))

    def __eq__(self, _node):

        node = _node

        if self is node:
            return 1
        if not node:  # or not self._schema.isEqual(node._schema):
            return 0

        if not isinstance(node, type(self)):
            return 0

        # Compare the schemas against each other
        if (hasattr(self, '_schema') and self._schema is None) and (hasattr(node, '_schema') and node._schema is None):
            return 1  # If they're both `None`
        elif (hasattr(self, '_schema') and self._schema is None) or (hasattr(node, '_schema') and node._schema is None):
            return 0  # If just one of them is `None`
        elif not self._schema.isEqual(node._schema):
            return 0  # Both have _schema but do not match

        # Check each schema item in turn and check for equality
        for (name, item) in self._schema.allItems():
            if item['comparable'] == True:
                #logger.info("testing: %s::%s" % (str(_getName(self)), str(name)))
                if getattr(self, name) != getattr(node, name):
                    #logger.info( "diff: %s::%s" % (str(_getName(self)), str(name)))
                    return 0

        return 1

    def __ne__(self, node):
        return not self.__eq__(node)

    def getNodeData(self):
        return self._data

    def setNodeData(self, new_data):
        self._data = new_data
        for k, v in self._data.iteritems():
            if isinstance(v, Node):
                v._setParent(self)

    def getNodeAttribute(self, attrib_name):
        return self.getNodeData()[attrib_name]

    def setNodeAttribute(self, attrib_name, attrib_value):
        self.getNodeData()[attrib_name] = attrib_value
        ## ALL of the functions are in the NODE class not GANGAOBJECT
        if isinstance(attrib_value, Node):
            self.getNodeData()[attrib_name]._setParent(self)

    def removeNodeAttribute(self, attrib_name):
        if attrib_name in self._data.keys():
            del self._data[attrib_name]

    def removeNodeIndexCacheAttribute(self, attrib_name):
        if self._index_cache and attrib_name in self._index_cache.keys():
            del self._index_cache[attrib_name]

    def setNodeIndexCache(self, new_index_cache):
        setattr(self, '_index_cache', new_index_cache)

    def getNodeIndexCache(self):
        return self._index_cache

##########################################################################


def synchronised_descriptor(get_or_set):
    """
    This decorator should only be used on ``__get__`` or ``__set__`` methods of a ``Descriptor``.
    """
    @functools.wraps(get_or_set)
    def decorated(self, obj, type_or_value):
        if obj is None:
            return get_or_set(self, obj, type_or_value)
        with obj.lock:
            return get_or_set(self, obj, type_or_value)
    return decorated


class Descriptor(object):

    def __init__(self, name, item):
        self._name = name
        self._item = item
        self._getter_name = None
        self._checkset_name = None
        self._filter_name = None

        self._getter_name = item['getter']
        self._checkset_name = item['checkset']
        self._filter_name = item['filter']

    @staticmethod
    def _bind_method(obj, name):
        if name is None:
            return None
        return getattr(obj, name)

    def _check_getter(self):
        if self._getter_name:
            raise AttributeError('cannot modify or delete "%s" property (declared as "getter")' % _getName(self))

    @synchronised_descriptor
    def __get__(self, obj, cls):
        name = _getName(self)

        # If obj is None then the getter was called on the class so return the Item
        if obj is None:
            return cls._schema[name]

        if self._getter_name:
            return self._bind_method(obj, self._getter_name)()

        # First we want to try to get the information without prompting a load from disk

        # ._data takes priority ALWAYS over ._index_cache
        # This access should not cause the object to be loaded
        obj_data = obj.getNodeData()
        if name in obj_data:
            return obj_data[name]

        # Then try to get it from the index cache
        obj_index = obj.getNodeIndexCache()
        if name in obj_index:
            return obj_index[name]

        # Since we couldn't find the information in the cache, we will need to fully load the object

        # Guarantee that the object is now loaded from disk
        obj._getReadAccess()

        # First try to load the object from the attributes on disk
        if name in obj.getNodeData():
            return obj.getNodeAttribute(name)

        # Finally, get the default value from the schema
        if obj._schema.hasItem(name):
            return obj._schema.getDefaultValue(name)

        raise AttributeError('Could not find attribute {0} in {1}'.format(name, obj))

    def __cloneVal(self, v, obj):

        item = obj._schema[_getName(self)]

        if v is None:
            if item.hasProperty('category'):
                assertion = item['optional'] and (item['category'] != 'internal')
            else:
                assertion = item['optional']
            #assert(assertion)
            if assertion is False:
                logger.warning("Item: '%s'. of class type: '%s'. Has a Default value of 'None' but is NOT optional!!!" % (_getName(self), type(obj)))
                logger.warning("Please contact the developers and make sure this is updated!")
            return None
        elif isinstance(v, str):
            return str(v)
        elif isinstance(v, int):
            return int(v)
        elif isinstance(v, dict):
            new_dict = {}
            for key, item in new_dict.iteritems():
                new_dict[key] = self.__cloneVal(v, obj)
            return new_dict
        else:
            if not isinstance(v, Node) and isinstance(v, (list, tuple)):
                try:
                    GangaList = _getGangaList()
                    new_v = GangaList()
                except ImportError:
                    new_v = []
                for elem in v:
                    new_v.append(self.__cloneVal(elem, obj))
                #return new_v
            elif not isinstance(v, Node):
                if inspect.isclass(v):
                    new_v = v()
                else:
                    new_v = v
                if not isinstance(new_v, Node):
                    logger.error("v: %s" % str(v))
                    raise GangaException("Error: found Object: %s of type: %s expected an object inheriting from Node!" % (str(v), str(type(v))))
                else:
                    new_v = self.__copyNodeObject(new_v, obj)
            else:
                new_v = self.__copyNodeObject(v, obj)

            return new_v

    def __copyNodeObject(self, v, obj):
        """This deals with the actual deepcopy of an object which has inherited from Node class"""

        item = obj._schema[_getName(self)]
        GangaList = _getGangaList()
        if isinstance(v, GangaList):
            categories = v.getCategory()
            len_cat = len(categories)
            if (len_cat > 1) or ((len_cat == 1) and (categories[0] != item['category'])) and item['category'] != 'internal':
                # we pass on empty lists, as the catagory is yet to be defined
                from Ganga.GPIDev.Base.Proxy import GangaAttributeError
                raise GangaAttributeError('%s: attempt to assign a list containing incompatible objects %s to the property in category "%s"' % (_getName(self), v, item['category']))
        else:
            if v._category not in [item['category'], 'internal'] and item['category'] != 'internal':
                from Ganga.GPIDev.Base.Proxy import GangaAttributeError
                raise GangaAttributeError('%s: attempt to assign an incompatible object %s to the property in category "%s found cat: %s"' % (_getName(self), v, item['category'], v._category))


        v_copy = deepcopy(v)

        #logger.info("Cloned Object Parent: %s" % v_copy._getParent())
        #logger.info("Original: %s" % v_copy._getParent())

        return v_copy

    @synchronised_descriptor
    def __set__(self, _obj, _val):
        ## self: attribute being changed or Ganga.GPIDev.Base.Objects.Descriptor in which case _getName(self) gives the name of the attribute being changed
        ## _obj: parent class which 'owns' the attribute
        ## _val: value of the attribute which we're about to set

        obj_reg = None
        obj_prevState = None
        obj = _obj
        if isinstance(obj, GangaObject):
            obj_reg = obj._getRegistry()
            if obj_reg is not None and hasattr(obj_reg, 'isAutoFlushEnabled'):
                obj_prevState = obj_reg.isAutoFlushEnabled()
                if obj_prevState is True and hasattr(obj_reg, 'turnOffAutoFlushing'):
                    obj_reg.turnOffAutoFlushing()

        val_reg = None
        val_prevState = None
        val = _val
        if isinstance(val, GangaObject):
            val_reg = val._getRegistry()
            if val_reg is not None and hasattr(val_reg, 'isAutoFlushEnabled'):
                val_prevState = val_reg.isAutoFlushEnabled()
                if val_prevState is True and hasattr(val_reg, 'turnOffAutoFlushing'):
                    val_reg.turnOffAutoFlushing()

        if type(_val) is str:
            from Ganga.GPIDev.Base.Proxy import stripProxy, runtimeEvalString
            new_val = stripProxy(runtimeEvalString(_obj, _getName(self), _val))
        else:
            new_val = _val

        self.__atomic_set__(_obj, new_val)

        if isinstance(new_val, Node):
            val._setDirty()

        if val_reg is not None:
            if val_prevState is True and hasattr(val_reg, 'turnOnAutoFlushing'):
                val_reg.turnOnAutoFlushing()

        if obj_reg is not None:
            if obj_prevState is True and hasattr(obj_reg, 'turnOnAutoFlushing'):
                obj_reg.turnOnAutoFlushing()

    def __atomic_set__(self, _obj, _val):
        ## self: attribute being changed or Ganga.GPIDev.Base.Objects.Descriptor in which case _getName(self) gives the name of the attribute being changed
        ## _obj: parent class which 'owns' the attribute
        ## _val: value of the attribute which we're about to set

        #if hasattr(_obj, _getName(self)):
        #    if not isinstance(getattr(_obj, _getName(self)), GangaObject):
        #        if type( getattr(_obj, _getName(self)) ) == type(_val):
        #            object.__setattr__(_obj, _getName(self), deepcopy(_val))
        #            return
#
#        if not isinstance(_obj, GangaObject) and type(_obj) == type(_val):
#            _obj = deepcopy(_val)
#            return

        obj = _obj
        temp_val = _val

        from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList

        if hasattr(obj, '_checkset_name'):
            checkSet = self._bind_method(obj, self._checkset_name)
            if checkSet is not None:
                checkSet(temp_val)
        if hasattr(obj, '_filter_name'):
            this_filter = self._bind_method(obj, self._filter_name)
            if this_filter:
                val = this_filter(temp_val)
            else:
                val = temp_val
        else:
            val = temp_val

        # LOCKING
        obj._getWriteAccess()

        #self._check_getter()

        item = obj._schema[_getName(self)]

        def cloneVal(v):
            GangaList = _getGangaList()
            if isinstance(v, (list, tuple, GangaList)):
                new_v = GangaList()
                for elem in v:
                    new_v.append(self.__cloneVal(elem, obj))
                return new_v
            else:
                return self.__cloneVal(v, obj)

        ## If the item has been defined as a sequence great, let's continue!
        if item['sequence']:
            _preparable = True if item['preparable'] else False
            if len(val) == 0:
                GangaList = _getGangaList()
                new_val = GangaList()
            else:
                if isinstance(item, Schema.ComponentItem):
                    new_val = makeGangaList(val, cloneVal, parent=obj, preparable=_preparable)
                else:
                    new_val = makeGangaList(val, parent=obj, preparable=_preparable)
        else:
            ## Else we need to work out what we've got.
            if isinstance(item, Schema.ComponentItem):
                GangaList = _getGangaList()
                if isinstance(val, (list, tuple, GangaList)):
                    ## Can't have a GangaList inside a GangaList easily so lets not
                    if isinstance(_obj, GangaList):
                        newListObj = []
                    else:
                        newListObj = GangaList()

                    Descriptor.__createNewList(newListObj, val, cloneVal)
                    #for elem in val:
                    #    newListObj.append(cloneVal(elem))
                    new_val = newListObj
                else:
                    new_val = cloneVal(val)
            else:
                new_val = val
                pass
            #val = deepcopy(val)

        if isinstance(new_val, Node):
            new_val._setParent(obj)

        obj.setNodeAttribute(_getName(self), new_val)

        obj._setDirty()

    def __delete__(self, obj):
        obj.removeNodeAttribute(_getName(self))

    @staticmethod
    def __createNewList(final_list, input_elements, action=None):

        def addToList(_input_elements, _final_list, action=None):
            if action is not None:
                for element in _input_elements:
                    _final_list.append(action(element))
            else:
                for element in _input_elements:
                    _final_list.append(element)
            return

        ## This makes it stick to 1 thread, useful for debugging problems
        addToList(input_elements, final_list, action)
        return


class ObjectMetaclass(type):
    _descriptor = Descriptor

    def __init__(cls, name, bases, this_dict):

        super(ObjectMetaclass, cls).__init__(name, bases, this_dict)

        # all Ganga classes must have (even empty) schema
        if cls._schema is None:
            cls._schema = Schema.Schema(None, None)

        this_schema = cls._schema

        # Add all class members of type `Schema.Item` to the _schema object
        # TODO: We _could_ add base class's Items here by going through `bases` as well.
        # We can't just yet because at this point the base class' Item has been overwritten with a Descriptor
        for member_name, member in this_dict.items():
            if isinstance(member, Schema.Item):
                this_schema.datadict[member_name] = member

        # sanity checks for schema...
        if '_schema' not in this_dict.keys():
            s = "Class %s must _schema (it cannot be silently inherited)" % (name,)
            logger.error(s)
            raise ValueError(s)

        # If a class has not specified a '_name' then default to using the class '__name__'
        if not cls.__dict__.get('_name'):
            cls._name = name

        if this_schema._pluginclass is not None:
            logger.warning('Possible schema clash in class %s between %s and %s', name, _getName(cls), _getName(this_schema._pluginclass))

        # export visible properties... do not export hidden properties
        for attr, item in this_schema.allItems():
            setattr(cls, attr, cls._descriptor(attr, item))

        # additional check of type
        # bugfix #40220: Ensure that default values satisfy the declared types
        # in the schema
        for attr, item in this_schema.simpleItems():
            if not item['getter']:
                item._check_type(item['defvalue'], '.'.join([name, attr]))

        # create reference in schema to the pluginclass
        this_schema._pluginclass = cls

        # if we've not even declared this we don't want to use it!
        if not cls._declared_property('hidden') or cls._declared_property('enable_plugin'):
            allPlugins.add(cls, cls._category, _getName(cls))

        # create a configuration unit for default values of object properties
        if not cls._declared_property('hidden') or cls._declared_property('enable_config'):
            this_schema.createDefaultConfig()


class GangaObject(Node):
    __metaclass__ = ObjectMetaclass
    _schema = None  # obligatory, specified in the derived classes
    _category = None  # obligatory, specified in the derived classes
    _registry = None  # automatically set for Root objects
    _exportmethods = []  # optional, specified in the derived classes

    # by default classes are not hidden, config generation and plugin
    # registration is enabled
    # optional, specify in the class if you do not want to export it publicly
    # in GPI,
    _hidden = 1
    # the class will not be registered as a plugin unless _enable_plugin is defined
    # the test if the class is hidden is performed by x._declared_property('hidden')
    # which makes sure that _hidden must be *explicitly* declared, not
    # inherited

    # additional properties that may be set in derived classes which were declared as _hidden:
    #   _enable_plugin = 1 -> allow registration of _hidden classes in the allPlugins dictionary
    # _enable_config = 1 -> allow generation of [default_X] configuration
    # section with schema properties

    # must be fully initialized
    def __init__(self):
        super(GangaObject, self).__init__(None)

        # IMPORTANT: if you add instance attributes like in the line below
        # make sure to update the __getstate__ method as well
        # dirty flag is true if the object has been modified locally and its
        # contents is out-of-sync with its repository
        self._dirty = False

        #Node.__init__(self, None)

        if self._schema is not None and hasattr(self._schema, 'allItems'):
            for attr, item in self._schema.allItems():
                ## If an object is hidden behind a getter method we can't assign a parent or defvalue so don't bother - rcurrie
                if item.getProperties()['getter'] is None:
                    defVal = self._schema.getDefaultValue(attr)
                    self.setNodeAttribute(attr, defVal)

        # Overwrite default values with any config values specified
        # self.setPropertiesFromConfig()

    def __construct__(self, args):
        # type: (Sequence) -> None
        """
        This acts like a secondary constructor for proxy objects.
        Any positional (non-keyword) arguments are passed to this function to construct the object.

        This default implementation performs a copy if there was only one item in the list
        and raises an exception if there is more than one.

        Args:
            args: a list of objects

        Raises:
            TypeMismatchError: if there is more than one item in the list
        """
        # FIXME: This should probably be move to Proxy.py

        if len(args) == 0:
            return
        elif len(args) == 1:
            if not isinstance(args[0], type(self)):
                logger.warning("Performing a copyFrom from: %s to: %s" % (type(args[0]), type(self)))
            self.copyFrom(args[0])
        else:
            from Ganga.GPIDev.Base.Proxy import TypeMismatchError
            raise TypeMismatchError("Constructor expected one or zero non-keyword arguments, got %i" % len(args))

    def __getstate__(self):
        # IMPORTANT: keep this in sync with the __init__
        #self._getReadAccess()
        this_dict = super(GangaObject, self).__getstate__()
        #this_dict['_dirty'] = False
        return this_dict

    def __setstate__(self, this_dict):
        #self._getWriteAccess()
        super(GangaObject, self).__setstate__(this_dict)
        #if '_parent' in this_dict:
        #    self._setParent(this_dict['_parent'])
        #self._setParent(None)
        self._dirty = False

    @staticmethod
    def __incrementShareRef(obj, attr_name):
        shared_dir = getattr(obj, attr_name)

        if hasattr(shared_dir, 'name'):

            from Ganga.Core.GangaRepository import getRegistry
            from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
            shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())

            logger.debug("Increasing shareref")
            shareref.increase(shared_dir.name)

    # on the deepcopy reset all non-copyable properties as defined in the
    # schema
    def __deepcopy__(self, memo=None):
        true_parent = self._getParent()
        ## This triggers a read of the job from disk
        self._getReadAccess()
        classname = _getName(self)
        category = self._category
        cls = self.__class__#allPlugins.find(category, classname)

        self_copy = cls()

        global do_not_copy
        if self._schema is not None:
            for name, item in self._schema.allItems():
                if not item['copyable'] or name in do_not_copy:
                    setattr(self_copy, name, self._schema.getDefaultValue(name))
                else:
                    if hasattr(self, name):
                        setattr(self_copy, name, deepcopy(getattr(self, name)))
                    else:
                        setattr(self_copy, name, self._schema.getDefaultValue(name))

                this_attr = getattr(self_copy, name)
                if isinstance(this_attr, Node):
                    this_attr._setParent(self_copy)

                if item.isA(Schema.SharedItem):
                    self.__incrementShareRef(self_copy, name)

        for k, v in self.__dict__.iteritems():
            if k not in do_not_copy:
                try:
                    self_copy.__dict__[k] = deepcopy(v)
                except:
                    self_copy.__dict__[k] = v

        if true_parent is not None:
            self._setParent(true_parent)
            self_copy._setParent(true_parent)
        return self_copy

    def _getIOTimeOut(self):
        from Ganga.Utility.Config.Config import getConfig, ConfigError
        try:
            _timeOut = getConfig('Configuration')['DiskIOTimeout']
        except ConfigError, err:
            _timeOut = 5. # 5sec hardcoded default
        return _timeOut

    def _getWriteAccess(self):
        """ tries to get write access to the object.
        Raise LockingError (or so) on fail """
        root = self._getRoot()
        reg = root._getRegistry()
        if reg is not None:
            _haveLocked = False
            _counter = 1
            _sleep_size = 1
            _timeOut = self._getIOTimeOut()
            while not _haveLocked:
                err = None
                from Ganga.Core.GangaRepository.Registry import RegistryLockError, RegistryAccessError
                reg._write_access(root)
                try:
                    reg._write_access(root)
                    _haveLocked = True
                except (RegistryLockError, RegistryAccessError) as x:
                    #import traceback
                    #traceback.print_stack()
                    from time import sleep
                    sleep(_sleep_size)  # Sleep 2 sec between tests
                    logger.info("Waiting on Write access to registry: %s" % reg.name)
                    logger.debug("err: %s" % str(x))
                    err = x
                _counter = _counter + 1
                # Sleep 2 sec longer than the time taken to bail out
                if _counter * _sleep_size >= _timeOut + 2:
                    logger.error("Failed to get access to registry: %s. Reason: %s" % (reg.name, str(err)))
                    if err is not None:
                        raise err

    def _releaseWriteAccess(self):
        """ releases write access to the object.
        Raise LockingError (or so) on fail
        Please use only if the object is expected to be used by other sessions"""
        root = self._getRoot()
        reg = root._getRegistry()
        if reg is not None:
            logger.debug("Releasing: %s" % (reg.name))
            reg._release_lock(root)

    def _getReadAccess(self):
        """ makes sure the objects _data is there and the object itself has a recent state.
        Raise RepositoryError"""
        root = self._getRoot()
        reg = root._getRegistry()
        if reg is not None:
            reg._read_access(root, self)

    # define when the object is read-only (for example a job is read-only in
    # the states other than new)
    def _readonly(self):
        r = self._getRoot()
        # is object a root for itself? check needed otherwise infinite
        # recursion
        if r is None or r is self:
            return 0
        else:
            return r._readonly()

    # set the registry for this object (assumes this object is a root object)
    def _setRegistry(self, registry):
        assert self._getParent() is None
        self._registry = registry

    # get the registry for the object by getting the registry associated with
    # the root object (if any)
    def _getRegistry(self):
        r = self._getRoot()
        return r._registry

    def _getRegistryID(self):
        try:
            return self._registry.find(self)
        except AttributeError, err:
            logger.debug("_getRegistryID Exception: %s" % str(err))
            return None

    # mark object as "dirty" and inform the registry about it
    # the registry is always associated with the root object
    def _setDirty(self, dummy=1):
        self._dirty = True
        parent = self._getParent()
        if parent is not None:
            parent._setDirty()

    def _setFlushed(self):
        self._dirty = False

    # post __init__ hook automatically called by GPI Proxy __init__
    def _auto__init__(self):
        pass

    # return True if _name attribute was explicitly defined in the class
    # this means that implicit (inherited) _name attribute has no effect in the derived class
    # example: cls._declared_property('hidden') => True if there is class
    # attribute _hidden explicitly declared
    @classmethod
    def _declared_property(cls, name):
        return '_' + name in cls.__dict__

    # get the job object associated with self or raise an assertion error
    # the FIRST PARENT Job is returned...
    # this method is for convenience and may well be moved to some subclass
    def getJobObject(self):
        from Ganga.GPIDev.Lib.Job import Job
        r = self._getRoot(cond=lambda o: isinstance(o, Job))
        if not isinstance(r, Job):
            raise AssertionError('no job associated with object ' + repr(self))
        return r

    # Customization of the GPI attribute assignment: Attribute Filters
    #
    # Example of usage:
    # if some properties depend on the value of other properties in a complex way such as:
    # changing platform of Gaudi should change the version if it is not supported... etc.
    #
    # Semantics:
    #  gpi_proxy.x = v        --> stripProxy(gpi_proxy)._attribute_filter__set__('x',v)
    #  gpi_proxy.y = [v1,v2]  --> stripProxy(gpi_proxy)._attribute_filter__set__('x',[v1,v2])
    #
    #  is used for *all* kinds of attributes (component and simple)
    #
    # Attribute Filters are used mainly for side effects such as modification of the state of the self object
    # (as described above).
    #
    # Attribute Filter may also perform an additional conversion of the value which is being assigned.
    #
    # Returns the attribute value (converted or original)
    #
    def _attribute_filter__set__(self, name, v):
        if hasattr(v, '_on_attribute__set__'):
            return v._on_attribute__set__(self, name)
        return v


# define the default component object filter:
# obj.x = "Y"   <=> obj.x = Y()


def string_type_shortcut_filter(val, item):
    if isinstance(val, type('')):
        if item is None:
            raise ValueError('cannot apply default string conversion, probably you are trying to use it in the constructor')
        from Ganga.Utility.Plugin import allPlugins, PluginManagerError
        try:
            obj = allPlugins.find(item['category'], val)()
            obj._auto__init__()
            return obj
        except PluginManagerError as err:
            logger.debug("string_type_shortcut_filter Exception: %s" % str(err))
            raise ValueError(err)
    return None

# FIXME: change into classmethod (do they inherit?) and then change stripComponentObject to use class instead of
# FIXME: object (object model clearly fails with sequence of Files)
# FIXME: test: ../bin/ganga -c local_lhcb.ini run.py
# TestNativeSpecific.testFileSequence


from .Filters import allComponentFilters
allComponentFilters.setDefault(string_type_shortcut_filter)

#
#
# $Log: not supported by cvs2svn $
# Revision 1.5.2.9  2009/07/14 14:44:17  ebke
# * several bugfixes
# * changed indexing for XML/Pickle
# * introduce index update minimal time of 20 seconds (reduces lag for typing 'jobs')
# * subjob splitting and individual flushing for XML/Pickle
#
# Revision 1.5.2.8  2009/07/13 22:10:53  ebke
# Update for the new GangaRepository:
# * Moved dict interface from Repository to Registry
# * Clearly specified Exceptions to be raised by Repository
# * proper exception handling in Registry
# * moved _writable to _getWriteAccess, introduce _getReadAccess
# * clarified locking, logic in Registry, less in Repository
# * index reading support in XML (no writing, though..)
# * general index reading on registry.keys()
#
# Revision 1.5.2.7  2009/07/10 12:14:10  ebke
# Fixed wrong sequence in __set__: only dirty _after_ writing!
#
# Revision 1.5.2.6  2009/07/10 11:33:06  ebke
# Preparations and fixes for lazy loading
#
# Revision 1.5.2.5  2009/07/08 15:27:50  ebke
# Removed load speed bottleneck for pickle - reduced __setstate__ time by factor 3.
#
# Revision 1.5.2.4  2009/07/08 12:51:52  ebke
# Fixes some bugs introduced in the latest version
#
# Revision 1.5.2.3  2009/07/08 12:36:54  ebke
# Simplified _writable
#
# Revision 1.5.2.2  2009/07/08 11:18:21  ebke
# Initial commit of all - mostly small - modifications due to the new GangaRepository.
# No interface visible to the user is changed
#
# Revision 1.5.2.1  2009/06/04 12:00:37  moscicki
# *** empty log message ***
#
# Revision 1.5  2009/05/20 13:40:22  moscicki
# added filter property for GangaObjects
#
# added Utility.Config.expandgangasystemvars() filter which expands @{VAR} in strings, where VAR is a configuration option defined in the System section
# Usage example: specify @{GANGA_PYTHONPATH} in the configuration file to make pathnames relative to the location of Ganga release; specify @{GANGA_VERSION} to expand to current Ganga version. etc.
#
# modified credentials package (ICommandSet) to use the expandgangasystemvars() filter.
#
# Revision 1.4  2009/04/27 09:22:56  moscicki
# fix #29745: use __mro__ rather than first-generation of base classes
#
# Revision 1.3  2009/02/24 14:57:56  moscicki
# set parent correctly for GangaList items (in __setstate__)
#
# Revision 1.2  2008/09/09 14:37:16  moscicki
# bugfix #40220: Ensure that default values satisfy the declared types in the schema
#
# factored out type checking into schema module, fixed a number of wrongly declared schema items in the core
#
# Revision 1.1  2008/07/17 16:40:52  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.27.4.10  2008/03/31 15:30:26  kubam
# More flexible internal logic of hiding the plugin classes derived from GangaObject:
#
# # by default classes are not hidden, config generation and plugin registration is enabled
#
# _hidden       = 1  # optional, specify in the class if you do not want to export it publicly in GPI,
#                    # the class will not be registered as a plugin unless _enable_plugin is defined
#                    # the test if the class is hidden is performed by x._declared_property('hidden')
#
# # additional properties that may be set in derived classes which were declared as _hidden:
# #   _enable_plugin = 1 -> allow registration of _hidden classes in the allPlugins dictionary
# #   _enable_config = 1 -> allow generation of [default_X] configuration section with schema properties
#
# This fixes: bug #34470: config [defaults_GridProxy] missing (issue of _hidden property of GangaObject)
#
# Revision 1.27.4.9  2008/02/28 15:44:31  moscicki
# fixed set parent problem for GangaList (and removed Will's hack which was already commented out)
#
# Revision 1.27.4.8  2008/02/12 09:25:24  amuraru
# removed Will's hack to set the parent, it causes side effects in subjobs accessor(to be checked)
#
# Revision 1.27.4.7  2008/02/06 17:02:00  moscicki
# small fix
#
# Revision 1.27.4.6  2008/02/06 13:00:21  wreece
# Adds a bit of a HACK as a tempory measure. The parent of a GangaList is being lost somewhere (I suspect due to a copy). I've added it back manually in __get__.
#
# Revision 1.27.4.5  2008/02/06 09:28:48  wreece
# First pass at a cleanup of the gangalist stuff. I've made changes so the diffs with the 4.4 series are more transparent. Currently still test failures.
#
# Revision 1.27.4.4  2007/12/18 09:05:04  moscicki
# integrated typesystem from Alvin and made more uniform error reporting
#
# Revision 1.27.4.3  2007/11/14 13:03:54  wreece
# Changes to make shortcuts work correctly with gangalists. all but one tests should now pass.
#
# Revision 1.27.4.2  2007/11/07 15:10:02  moscicki
# merged in pretty print and GangaList support from ganga-5-dev-branch-4-4-1-will-print branch
#
#
# Revision 1.27.4.1  2007/10/30 15:25:53  moscicki
# fixed #29745: Inherited functions of GangaObjects cannot be exported via _exportmethods
#
# Revision 1.27.8.3  2007/10/30 21:22:02  wreece
# Numerous small fixes and work arounds. And a new test.
#
# Revision 1.27.8.2  2007/10/30 14:30:23  wreece
# Non-working update. Adds in Kuba's exported methods dodge. It is now possible to define a _export_ version of a method for external use and a undecorated method for internal use.
#
# Revision 1.27.8.1  2007/10/30 12:12:08  wreece
# First version of the new print_summary functionality. Lots of changes, but some known limitations. Will address in next version.
#
# Revision 1.27  2007/07/27 13:52:00  moscicki
# merger updates from Will (from GangaMergers-1-0 branch)
#
# Revision 1.26.12.1  2007/05/14 13:32:11  wreece
# Adds the merger related code on a seperate branch. All tests currently
# run successfully.
#
# Revision 1.26  2006/07/27 20:09:54  moscicki
# _getJobObject() renamed -> getJobObject() and finding FIRST PARENT JOB
# _getRoot() : cond optional argument (to cut the search path e.g. for getJobObject())
#
# getDefaultValue() moved to Schema object
#
# "checkset" metaproperty implemented in schema (to check the direct updates to job.status)
# modified the "getter" metaproperty implementation
# create automatically the configuration units with default property values
#
# Revision 1.25  2006/02/10 14:18:44  moscicki
# removed obsoleted eval for default properties from config file
#
# Revision 1.24  2006/01/09 16:36:55  moscicki
#  - support for defining default properties in the configuration
#    config file example:
#
#      [LSF_Properties]
#      queue = myQueue
#
#      [LCG_Properties]
#      requirements.CE = myCE
#
#      [Job_Properties]
#      application = DaVinci
#      application.version = v99r2
#
# Revision 1.23  2005/12/02 15:27:13  moscicki
# support for "visitable" metaproperty
# support for hidden GPI classes
# "hidden" properties not visible in the proxies
# support for 'getter' type of property (a binding) for job.master
#
# Revision 1.22  2005/11/14 10:29:55  moscicki
# adapted to changed interface of allPlugins.add() method
#
# Revision 1.21  2005/11/01 16:43:51  moscicki
# added isStringLike condition to avoid unnecesary looping over strings
#
# Revision 1.20  2005/11/01 14:04:55  moscicki
# added missing implementation of GangaObject.__setstate__
# fixed the implementation of Node.__setstate__ to correctly set parent for objects which are contained in iterable simple items (lists, dictionaries) etc. (contributed by AS)
#
# Revision 1.19  2005/11/01 11:09:48  moscicki
# changes to support import/export (KH):
#   - The printTree method accepts an additional argument, which
#     it passes on to VPrinter as the value for "selection".
#
# Revision 1.18  2005/08/26 09:53:24  moscicki
# copy __doc__ for exported method
# _getJobObject() helper method in GangaObject
#
# Revision 1.17  2005/08/24 15:41:19  moscicki
# automatically generated help for properties, disabled the SchemaHelper and few other improvements to the help system
#
# Revision 1.16  2005/08/23 17:15:06  moscicki
# *** empty log message ***
#
#
#
