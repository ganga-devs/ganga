##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Objects.py,v 1.5.2.10 2009-07-24 13:35:53 ebke Exp $
##########################################################################
# NOTE: Make sure that _data and __dict__ of any GangaObject are only referenced
# here - this is necessary for write locking and lazy loading!

from functools import partial
import inspect
import abc
import threading
import _thread
from contextlib import contextmanager
import functools

from GangaCore.Utility.logging import getLogger

from copy import deepcopy, copy
from inspect import isclass

from GangaCore.GPIDev.Schema import Schema, Item, ComponentItem, SharedItem

from GangaCore.Core.exceptions import GangaValueError, GangaException

from GangaCore.Utility.Plugin import allPlugins

from GangaCore.Utility.Profiling import cpu_profiler, mem_profiler, call_counter

def _getName(obj):
    """ Return the name of an object based on what we prioritise 
    Name is defined as the first in the list of:
    obj._name, obj.__name__, obj.__class__.__name__ or str(obj)
    Args:
        obj (unknown):
    """
    try:
        return obj._name
    except AttributeError:
        try:
            return obj.__name__
        except AttributeError:
            try:
                return obj.__class__.__name__
            except AttributeError:
                return str(obj)

logger = getLogger()

do_not_copy = ['_index_cache_dict', '_parent', '_registry', '_data_dict', '_lock', '_proxyObject']

def synchronised(f):
    """
    This decorator must be attached to a method on a ``Node`` subclass
    It uses the object's lock to make sure that the object is held for the duration of the decorated function
    Args:
        f (function): This is the function which we want to wrap
    """
    @functools.wraps(f)
    def sync_decorated(self, *args, **kwargs):
        with self.const_lock:
            return f(self, *args, **kwargs)

    return sync_decorated


class Node(object, metaclass=abc.ABCMeta):
    """
    The Node class is the code of the Ganga heirachy. It allows objects to keep
    track of their parent, whether they're dirty and take part in the visitor
    pattern.

    It also provides access to tree-aware read/write locks to provide
    thread-safe usage.
    """
    __slots__ = ('_parent', '_lock', '_dirty')

    def __init__(self, parent=None):
        super(Node, self).__init__()
        self._parent = parent
        self._lock = threading.RLock()  # Don't write from out of thread when modifying an object
        self._dirty = False  # dirty flag is true if the object has been modified locally and its contents is out-of-sync with its repository

    def __deepcopy__(self, memo=None):
        cls = self.__class__
        obj = cls()
        this_dict = copy(self.__dict__)
        for elem, val in this_dict.items():
            if elem not in do_not_copy:
                this_dict[elem] = deepcopy(val, memo)

        obj.__dict__ = this_dict
        if self._getParent() is not None:
            obj._setParent(self._getParent())
        return obj

    def _getParent(self):
        """
        This gets the parent object defined for this Node
        """
        # type: () -> Node
        return self._parent

    @synchronised  # This will lock the _current_ (soon to be _old_) root object
    def _setParent(self, parent):
        """
        This sets the parent of this Node to be a new object
        Args:
            parent (GangaObject): This is the GangaObject to be taken as the objects new parent
        """
        # type: (Node) -> None
        if parent is None:
            setattr(self, '_parent', parent)
        else:
            with parent.const_lock: # This will lock the _new_ root object
                setattr(self, '_parent', parent)
            # Finally the new and then old root objects will be unlocked

    @property
    @contextmanager
    def const_lock(self):
        """
        This is a context manager which acquires the const write lock on the
        object's root object.

        This lock acquires exclusive access over an object tree to prevent it
        from changing. Reading schema attributes on the object is still allowed
        but changing them is not. Only one thread can hold this lock at once.
        """
        root = self._getRoot()
        with root._lock:
            yield

    def _getRoot(self, cond=None):
        # type: () -> Node
        """
        get the root of the object tree
        if parent does not exist then the root is the 'self' object
        cond is an optional function which may cut the search path: when it
        returns True, then the parent is returned as root
        """

        self_parent = self._getParent()
        if self_parent is None:
            return self
        obj = self_parent

        if cond is None:
            while True:
                parent = obj._getParent()
                if parent is not None:
                    obj = parent
                else:
                    break
                return obj
        else:
                                                    
            if cond(obj):
                return obj
                                                                                  
            escape = False
            while True:
                parent = obj._getObject()
                if parent is None or cond(obj):
                    break
                obj = parent
            return obj

        if cond is None:
            while obj._getParent() is not None:
                obj = obj._getParent()
            return obj
        else:

            if cond(obj):
                return obj

            while obj._getParent() is not None:
                if cond(obj):
                    break
                obj = obj._getParent()
            return obj

    # accept a visitor pattern
    @abc.abstractmethod
    def accept(self, visitor):
        """
        This can probably be removed if it doesn't do anything, only GangaObjet should inherit from Node
        """
        pass

    # mark object as "dirty" and inform the registry about it
    # the registry is always associated with the root object
    def _setDirty(self):
        """ Set the dirty flag all the way up to the parent"""
        self._dirty = True
        parent = self._getParent()
        if parent is not None:
            parent._setDirty()

    def _setFlushed(self):
        """
        This returns whether this object has been marked as dirty or not
        """
        self._dirty = False

    def printTree(self, f=None, sel=''):
        """
        This method will print a full tree which contains a full description of the class which of interest
        Args:
            f (stream): file-like output stream
            sel (str): Selection to display 
        """
        from GangaCore.GPIDev.Base.VPrinter import VPrinter
        self.accept(VPrinter(f, sel))

    def printSummaryTree(self, level=0, verbosity_level=0, whitespace_marker='', out=None, selection='', interactive=False):
        """If this method is overridden, the following should be noted:

        Args:
            level (int): the hierachy level we are currently at in the object tree.
            verbosity_level (int): How verbose the print should be. Currently this is always 0.
            whitespace_marker (str): If printing on multiple lines, this allows the default indentation to be replicated.
                               The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
            out (stream): An output stream to print to. The last line of output should be printed without a newline.'
            selection (str): See VPrinter for an explaintion of this.
            interactive (bool): Is this being printed to the interactive prompt
        """
        from GangaCore.GPIDev.Base.VPrinter import VSummaryPrinter
        self.accept(VSummaryPrinter(level, verbosity_level, whitespace_marker, out, selection, interactive))

    @abc.abstractmethod
    def __eq__(self, node):
        """
        This can probably be removed if it doesn't do anything, only GangaObjet should inherit from Node
        """
        pass

    def __ne__(self, node):
        """
        This can probably be removed if it doesn't do anything, only GangaObjet should inherit from Node
        """
        return not self.__eq__(node)

##########################################################################


def synchronised_get_descriptor(get_function):
    """
    This decorator should only be used on ``__get__`` method of the ``Descriptor``.
    Args:
        get_function (function): Function we intend to wrap with the soft/read lock
    """
    @functools.wraps(get_function)
    def get_decorator(self, obj, type_or_value):
        if obj is None:
            return get_function(self, obj, type_or_value)

        with obj._getRoot()._lock:
            return get_function(self, obj, type_or_value)

    return get_decorator


def synchronised_set_descriptor(set_function):
    """
    This decorator should only be used on ``__set__`` method of the ``Descriptor``.
    Args:
        set_function (function): Function we intend to wrap with the hard/write lock
    """
    def set_decorator(self, obj, type_or_value):
        root_obj = obj._getRoot()
        if obj is None:
            return set_function(self, obj, type_or_value, root_obj)

        with root_obj._lock:
            return set_function(self, obj, type_or_value)

    return set_decorator


class Descriptor(object):

    """
    This is the Descriptor class used to deal with object assignment ot attribtues of the GangaObject.
    This class handles the lazy-loading of an object from disk when needed to return a value stored in the Registry
    This class also handles thread-locking of a class including both the getter and setter methods to ensure object consistency
    """

    __slots__ = ('_name', '_item', '_checkset_name', '_filter_name', '_getter_name', '_checkset_name')

    def __init__(self, name, item):
        """
        Lets build a descriptor for this item with this name
        Args:
            name (str): Name of the attribute being wrapped
            item (Item): The Schema entry describing this attribute (Not currently used atm)
        """
        super(Descriptor, self).__init__()

        self._name = name
        self._item = item
        self._checkset_name = None
        self._filter_name = None

        self._getter_name = item['getter']
        self._checkset_name = item['checkset']
        self._filter_name = item['filter']

    @staticmethod
    def _bind_method(obj, name):
        """
        Method which returns the value for a given attribute of a name
        Args:
            name (str): This is the name of the attribute of interest
        """
        if name is None:
            return None
        return getattr(obj, name)

    def _check_getter(self):
        """
        This attribute checks to see if a getter has been assigned to an attribute or not to avoid it's assignment
        """
        if self._getter_name:
            raise AttributeError('cannot modify or delete "%s" property (declared as "getter")' % _getName(self))

    @synchronised_get_descriptor
    def __get__(self, obj, cls):
        """
        Get method of Descriptor
        This wraps the object in question with a read-lock which ensures object onsistency across multiple threads
        Args:
            obj (GangaObject): This is the object which controls the attribute of interest
            cls (class): This is the class of the Ganga Object which is being called
        """
        name = _getName(self)

        # If obj is None then the getter was called on the class so return the Item
        if obj is None:
            return cls._schema[name]

        if self._getter_name:
            # Fixme set the parent of Node objects?!?!
            return self._bind_method(obj, self._getter_name)()

        # First we want to try to get the information without prompting a load from disk

        # ._data takes priority ALWAYS over ._index_cache
        # This access should not cause the object to be loaded
        obj_data = obj._data
        try:
            return obj_data[name]
        except KeyError:
            pass

        # Then try to get it from the index cache
        obj_index = obj._index_cache
        try:
            return obj_index[name]
        except KeyError:
            pass

        # Since we couldn't find the information in the cache, we will need to fully load the object
        obj._loadObject()

        # Do we have the attribute now?
        try:
            return obj._data[name]
        except KeyError:
            pass

        # Last option: get the default value from the schema
        if obj._schema.hasItem(name):
            return obj._schema.getDefaultValue(name)

        raise AttributeError('Could not find attribute {0} in {1}'.format(name, obj))

    @staticmethod
    def cloneObject(v, obj, name):
        """
        Clone v using knowledge of the obj the attr is being set on and the name of self is the attribute name
        return a new instance of v equal to v
        Args:
            v (unknown): This is the object we want to clone
            obj (GangaObject): This is the parent object of the attribute
            name (str): This is the name of the attribute we want to assign the value of v to
        """
        item = obj._schema[name]

        if v is None:
            if item.hasProperty('category'):
                assertion = item['optional'] and (item['category'] != 'internal')
            else:
                assertion = item['optional']
            #assert(assertion)
            if assertion is False:
                logger.warning("Item: '%s'. of class type: '%s'. Has a Default value of 'None' but is NOT optional!!!" % (name, type(obj)))
                logger.warning("Please contact the developers and make sure this is updated!")
            return None
        elif isinstance(v, str):
            return str(v)
        elif isinstance(v, int):
            return int(v)
        elif isinstance(v, dict):
            new_dict = {}
            for key, item in v.items():
                new_dict[key] = Descriptor.cloneObject(item, obj, name)
            return new_dict
        else:
            if not isinstance(v, Node) and isinstance(v, (list, tuple)):
                try:
                    # must import here as will fail at the top
                    from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
                    new_v = GangaList()
                except ImportError:
                    new_v = []
                for elem in v:
                    new_v.append(Descriptor.cloneObject(elem, obj, name))
                #return new_v
            elif not isinstance(v, Node):
                if isclass(v):
                    new_v = v()
                else:
                    new_v = v
                if not isinstance(new_v, Node):
                    logger.error("v: %s" % v)
                    raise GangaException("Error: found Object: %s of type: %s expected an object inheriting from Node!" % (v, type(v)))
                else:
                    new_v = Descriptor.cloneNodeObject(new_v, obj, name)
            else:
                new_v = Descriptor.cloneNodeObject(v, obj, name)

            return new_v

    @staticmethod
    def cloneNodeObject(v, obj, name):
        """This copies objects inherited from Node class
        This is a call to deepcopy after testing to see if the object can be copied to the attribute
        Args:
            v (GangaObject): This is the value which we want to copy from
            obj (GangaObject): This is the object which controls the attribute we want to assign
            name (str): This is th name of the attribute which we're setting
        """

        item = obj._schema[name]
        if isinstance(v, GangaList):
            categories = v.getCategory()
            len_cat = len(categories)
            if (len_cat > 1) or ((len_cat == 1) and (categories[0] != item['category'])) and item['category'] != 'internal':
                # we pass on empty lists, as the catagory is yet to be defined
                from GangaCore.GPIDev.Base.Proxy import GangaAttributeError
                raise GangaAttributeError('%s: attempt to assign a list containing incompatible objects %s to the property in category "%s"' % (name, _getName(v), item['category']))
        else:
            if v._category not in [item['category'], 'internal'] and item['category'] != 'internal':
                from GangaCore.GPIDev.Base.Proxy import GangaAttributeError
                raise GangaAttributeError('%s: attempt to assign an incompatible object %s to the property in category "%s found cat: %s"' % (name, _getName(v), item['category'], v._category))

        v_copy = deepcopy(v)

        return v_copy

    @synchronised_set_descriptor
    def __set__(self, obj, val, root_obj=None):
        """
        Set method
        This wraps the given object with a lock preventing both read+write until this transaction is complete for consistency
        Args:
            self: attribute being changed or GangaCore.GPIDev.Base.Objects.Descriptor in which case _getName(self) gives the name of the attribute being changed
            obj (GanagObject): parent class which 'owns' the attribute
            val (unknown): value of the attribute which we're about to set
            root_obj (GangaObject): a pointer to the root object of obj
        """

        _set_name = _getName(self)

        if isinstance(val, str):
            if val:
                from GangaCore.GPIDev.Base.Proxy import stripProxy, runtimeEvalString
                val = stripProxy(runtimeEvalString(obj, _set_name, val))

        if hasattr(obj, '_checkset_name'):
            checkSet = self._bind_method(obj, self._checkset_name)
            if checkSet is not None:
                checkSet(val)

        if hasattr(obj, '_filter_name'):
            this_filter = self._bind_method(obj, self._filter_name)
            if this_filter:
                val = this_filter(val)

        self._check_getter()

        # make sure we have the session lock
        obj._getSessionLock(root_obj)

        # make sure the object is loaded if it's attached to a registry
        obj._loadObject()

        basic=False
        for i in [int, str, bool, type]:
            if isinstance(val, i):
                new_value = deepcopy(val)
                basic=True
                break
        if not basic:
            new_value = Descriptor.cleanValue(obj, val, _set_name)

        obj.setSchemaAttribute(_set_name, new_value)

        obj._setDirty()

    @staticmethod
    def cleanValue(obj, val, name):
        """
        This returns a new instance of the value which has been correctly copied if needed so we can assign it to the attribute of the class
        Args:
            obj (GangaObject): This is the parent object of the attribute which is being set
            val (unknown): This is the value we want to assign to the attribute
            name (str): This is the name of the attribute which we're changing
        """

        item = obj._schema[name]

        ## If the item has been defined as a sequence great, let's continue!
        if item['sequence']:
            # These objects are lists
            _preparable = True if item['preparable'] else False
            if val is None:
                new_val = None
            elif len(val) == 0:
                new_val = GangaList()
            else:
                if isinstance(item, ComponentItem):
                    new_val = makeGangaList(val, Descriptor.cloneListOrObject, obj, _preparable, (name, obj))
                else:
                    new_val = makeGangaList(val, None, obj, _preparable)
        else:
            ## Else we need to work out what we've got.
            if isinstance(item, ComponentItem):
                if isinstance(val, (list, tuple, GangaList)):
                    ## Can't have a GangaList inside a GangaList easily so lets not
                    if isinstance(obj, GangaList):
                        new_val = []
                    else:
                        new_val = GangaList()
                    # Still don't know if val list of object here
                    Descriptor.createNewList(new_val, val, Descriptor.cloneListOrObject, (name, obj))
                else:
                    # Still don't know if val list of object here
                    new_val = Descriptor.cloneListOrObject(val, (name, obj))
            else:
                new_val = val
                pass

        if isinstance(new_val, Node) and new_val._getParent() is not obj:
            new_val._setParent(obj)

        return new_val

    @staticmethod
    def cloneListOrObject(v, extra_args):
        """
        This clones the value v by determining if the value of v is a list or not.
        If v is a list then a new list is returned with elements copied via cloneObject
        if v is not a list then a new instance of the list is copied via cloneObject
        Args:
            v (unknown): Object we want a new copy of
            extra_args (tuple): Contains the name of the attribute being copied and the object which owns the object being copied
        """

        name=extra_args[0]
        obj=extra_args[1]
        if isinstance(v, (list, tuple, GangaList)):
            new_v = GangaList()
            for elem in v:
                new_v.append(Descriptor.cloneObject(elem, obj, name))
            return new_v
        else:
            return Descriptor.cloneObject(v, obj, name)

    def __delete__(self, obj):
        """
        Delete an attribute from the Descriptor(?) and Node
        Args:
            obj (GangaObject): This is the object which wants to have an attribute removed from it
        """
        del obj._data[_getName(self)]

    @staticmethod
    def createNewList(_final_list, _input_elements, action=None, extra_args=None):
        """ Create a new list object which contains the old object with a possible action parsing the elements before they're added
        Args:
            _final_list (list): The list object which is to have the new elements appended to it
            _input_elements (list): This is a list of the objects which are to be used to create new objects in _final_list
            action (function): A function which is to be called to create new objects for a list
            extra_args (tuple): Contains the name of the attribute being copied and the object which owns the object being copied
        """

        if extra_args:
            new_action = partial(action, extra_args=extra_args)

        if action is not None:
            for element in _input_elements:
                if extra_args:
                    _final_list.append(new_action(element))
                else:
                    _final_list.append(action(element))
        else:
            for element in _input_elements:
                _final_list.append(element)


class ObjectMetaclass(abc.ABCMeta):
    """
    This is a MetaClass...
    TODO explain what this does"""

    __slots__ = list()

    def __init__(cls, name, bases, this_dict):
        """
        Init method for a class of name, name
        TODO, explain what bases and this_dict are used for
        """

        super(ObjectMetaclass, cls).__init__(name, bases, this_dict)

        # all Ganga classes must have (even empty) schema
        if cls._schema is None:
            cls._schema = Schema(None, None)

        this_schema = cls._schema

        # This reduces the memory footprint
        # TODO explore migrating the _data_dict object to a non-dictionary type as it's a fixed size and we can potentially save big here!
        # Adding the __dict__ here is an acknowledgement that we don't control all Ganga classes higher up.
        cls.__slots__ = ('_index_cache_dict', '_registry', '_data_dict', '__dict__', '_proxyObject')

        # Add all class members of type `Schema.Item` to the _schema object
        # TODO: We _could_ add base class's Items here by going through `bases` as well.
        # We can't just yet because at this point the base class' Item has been overwritten with a Descriptor
        for member_name, member in this_dict.items():
            if isinstance(member, Item):
                this_schema.datadict[member_name] = member

        # sanity checks for schema...
        if '_schema' not in this_dict:
            s = "Class %s must _schema (it cannot be silently inherited)" % (name,)
            logger.error(s)
            raise GangaValueError(s)

        attrs_to_add = [ attr for attr, item in this_schema.allItems()]

        if hasattr(cls, '_additional_slots'):
            attrs_to_add += [_ for _ in cls._additional_slots]

        cls.__slots__ = ('_index_cache_dict', '_registry', '_data_dict', '__dict__', '_proxyObject') + tuple(attrs_to_add)

        # If a class has not specified a '_name' then default to using the class '__name__'
        if not cls.__dict__.get('_name'):
            cls._name = name

        if this_schema._pluginclass is not None:
            logger.warning('Possible schema clash in class %s between %s and %s', name, _getName(cls), _getName(this_schema._pluginclass))

        # export visible properties... do not export hidden properties
        # This constructs one Descriptor for each attribute which can be set for this class
        for attr, item in this_schema.allItems():
            setattr(cls, attr, Descriptor(attr, item))

        # additional check of type
        # bugfix #40220: Ensure that default values satisfy the declared types
        # in the schema
        for attr, item in this_schema.simpleItems():
            if not item['getter']:
                item._check_type(item['defvalue'], '.'.join([name, attr]), False)

        # create reference in schema to the pluginclass
        this_schema._pluginclass = cls

        # if we've not even declared this we don't want to use it!
        if not cls._declared_property('hidden') or cls._declared_property('enable_plugin'):
            allPlugins.add(cls, cls._category, _getName(cls))

        # create a configuration unit for default values of object properties
        if not cls._declared_property('hidden') or cls._declared_property('enable_config'):
            this_schema.createDefaultConfig()


@call_counter
@cpu_profiler
@mem_profiler
class GangaObject(Node, metaclass=ObjectMetaclass):
    _schema = None  # obligatory, specified in the derived classes
    _category = None  # obligatory, specified in the derived classes
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

    # This boolean is used to stop a GangaObject from populating it's ._data dict with
    # default entries as evaluated from the schema.
    # This is useful when reading many objects from disk as this wastes CPU as the entry is being overridden
    _should_init = True
    _should_load = False

    @classmethod
    def getNew(cls, should_load=False, should_init=False):
        """
        Returns a new instance of this class type without a populated Schema.
        This should be an object which has all of the core logic initialized correctly.
        Args:
            should_load (bool): Should the class __init__ method be called by this method or should we return the base object?
        """
        # Build an object of this type
        returnable = cls.__new__(cls, (), {})

        # We have implicit knowledge about whether we should attempt to load this object from disk
        setattr(returnable, '_should_load', should_load)

        Node.__init__(returnable)

        setattr(returnable, '_should_init', should_init)
        GangaObject.__init__(returnable)
        try:
            # Initialize the most derived class to get all of the goodness needed higher up.
            returnable.__class__.__init__(returnable)
        except:
            logger.debug("Broken init method for class: %s trying to proceed silently" % cls.__name__)
        setattr(returnable, '_should_init', False)

        # Return the newly initialized object
        return returnable

    # must be fully initialized
    def __init__(self):
        """
        Main GangaObject that many classes inherit from
        """
        super(GangaObject, self).__init__(None)

        self._index_cache_dict = {}
        self._registry = None
        self._data_dict = {}

        # Just a flag to prevent expensive double-declarations and to avoid this where needed
        if self._should_init:
            self.populate_from_schema()

    def populate_from_schema(self):
        """
        Populate the data dict from the schema defaults
        """
        self._data_dict = dict.fromkeys(self._schema.datadict)
        for attr, item in self._schema.allItems():
            ## If an object is hidden behind a getter method we can't assign a parent or defvalue so don't bother - rcurrie
            if item.getProperties()['getter'] is None:
                setattr(self, attr, self._schema.getDefaultValue(attr))

    @synchronised
    def accept(self, visitor):
        """
        This accepts a VPrinter or VStreamer class instance visitor which is used to display the contents of the object according the Schema
        Args:
            visitor (VPrinter, VStreamer): An instance which will produce a display of this contents of this object
        """

        if not hasattr(self, '_schema'):
            return
        elif self._schema is None:
            visitor.nodeBegin(self)
            visitor.nodeEnd(self)
            return

        visitor.nodeBegin(self)

        for (name, item) in self._schema.simpleItems():
            if item['visitable']:
                visitor.simpleAttribute(self, name, getattr(self, name), item['sequence'])

        for (name, item) in self._schema.sharedItems():
            if item['visitable']:
                visitor.sharedAttribute(self, name, getattr(self, name), item['sequence'])

        for (name, item) in self._schema.componentItems():
            if item['visitable']:
                visitor.componentAttribute(self, name, getattr(self, name), item['sequence'])

        visitor.nodeEnd(self)

    def copyFrom(self, srcobj, _ignore_atts=None):
        # type: (GangaObject, Optional[Sequence[str]]) -> None
        """
        copy all the properties recursively from the srcobj
        if schema of self and srcobj are not compatible raises a ValueError
        ON FAILURE LEAVES SELF IN INCONSISTENT STATE
        Args:
            srcobj (GangaObject): This is the ganga object which is to have it's contents from
            _ignore_atts (list): This is a list of attribute names which are to not be copied
        """

        if _ignore_atts is None:
            _ignore_atts = []
        _srcobj = srcobj
        # Check if this object is derived from the source object, then the copy
        # will not throw away information

        if not hasattr(_srcobj, '__class__') and not inspect.isclass(_srcobj.__class__):
            raise GangaValueError("Can't copyFrom a non-class object: %s isclass: %s" % (_srcobj, inspect.isclass(_srcobj)))

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
        for key, val in src_dict.items():
            this_attr = getattr(srcobj, key)
            if isinstance(this_attr, Node) and key not in do_not_copy:
                #logger.debug("k: %s  Parent: %s" % (key, (srcobj)))
                if this_attr._getParent() is not srcobj:
                    this_attr._setParent(srcobj)

    def _actually_copyFrom(self, _srcobj, _ignore_atts):
        # type: (GangaObject, Optional[Sequence[str]]) -> None

        for name, item in self._schema.allItems():
            if name in _ignore_atts:
                continue

            #logger.debug("Copying: %s : %s" % (name, item))
            if name == 'application' and hasattr(_srcobj.application, 'is_prepared'):
                _app = _srcobj.application
                if _app.is_prepared not in [None, True]:
                    _app.incrementShareCounter(_app.is_prepared)

            if not self._schema.hasAttribute(name):
                #raise ValueError('copyFrom: incompatible schema: source=%s destination=%s'%(_getName(_srcobj), _getName(self)))
                if not hasattr(self, name):
                    setattr(self, name, self._schema.getDefaultValue(name))
                this_attr = getattr(self, name)
                if isinstance(this_attr, Node) and name not in do_not_copy:
                    if this_attr._getParent() is not self:
                        this_attr._setParent(self)
            elif not item['copyable']: ## Default of '1' instead of True...
                if not hasattr(self, name):
                    setattr(self, name, self._schema.getDefaultValue(name))
                this_attr = getattr(self, name)
                if isinstance(this_attr, Node) and name not in do_not_copy:
                    if this_attr._getParent() is not self:
                        this_attr._setParent(self)
            else:
                copy_obj = deepcopy(getattr(_srcobj, name))
                setattr(self, name, copy_obj)

    def __eq__(self, obj):
        """
        Compare this object to an other object obj
        Args:
            obj (unknown): Compare which self is to be compared to
        """
        if self is obj:
            return True

        if not isinstance(obj, type(self)):
            return False

        # Compare the schemas against each other
        if self._schema is None and obj._schema is None:
            return True  # If they're both `None`
        elif self._schema is None or obj._schema is None:
            return False  # If just one of them is `None`
        elif not self._schema.isEqual(obj._schema):
            return False  # Both have _schema but do not match

        # Check each schema item in turn and check for equality
        for (name, item) in self._schema.allItems():
            if item['comparable']:
                #logger.info("testing: %s::%s" % (_getName(self), name))
                if getattr(self, name) != getattr(obj, name):
                    #logger.info( "diff: %s::%s" % (_getName(self), name))
                    return False

        return True

    @property
    def _data(self):
        """
        This returns the internal data dictionary of this class
        This should be treated as strictly internal and shouldn't be modified outside of GangaCore.GPIDev.Base or GangaCore.Core.GangaRegistry
        """
        # type: () -> Dict[str, Any]
        return self._data_dict

    @_data.setter
    def _data(self, new_data):
        """
        Set the ._data attribute correctly for this object
        This should be treated as strictly internal and shouldn't be modified outside of GangaCore.GPIDev.Base or GangaCore.Core.GangaRegistry
        Args:
            new_data (dict): This is the external dictionary which is to be assigned to the internal dictionary
        """
        # type: (Dict[str, Any]) -> None
        for v in new_data.values():
            if isinstance(v, Node) and v._getParent() is not self:
                v._setParent(self)
        self._data_dict = new_data

    def setSchemaAttribute(self, attrib_name, attrib_value):
        # type: (str, Any) -> None
        """
        This sets the value of a schema attribute directly by circumventing the descriptor

        Args:
            attrib_name (str): the name of the schema attribute
            attrib_value (unknown): the value to set it to
        """
        self._data[attrib_name] = attrib_value
        if isinstance(attrib_value, Node) and attrib_value._getParent() is not self:
            self._data[attrib_name]._setParent(self)

    @property
    def _index_cache(self):
        """
        This returns the index cache.
        If this object has been fully loaded into memory and has a Registry assoicated with it it's created dynamically
        If this object isn't fully loaded or has no Registry associated with it the index_cache should be returned whatever it is
        """
        if self._fullyLoadedFromDisk():
            if self._getRegistry() is not None:
                # Fully loaded so lets regenerate this on the fly to avoid losing data
                return self._getRegistry().getIndexCache(self)
            else:
                # No registry therefore can't work out the Cache, probably empty, lets return that
                return self._index_cache_dict
        # Not in registry or not loaded, so can't re-generate if requested
        return self._index_cache_dict

    @_index_cache.setter
    def _index_cache(self, new_index_cache):
        """
        Set the index cache to have some new value which is defined externally.
        (This should __NEVER__ be done on live in-memory objects
        Args:
            new_inde_cache (dict): Dict of new entries for the new_index_cache
        """
        if self._fullyLoadedFromDisk():
            logger.debug("Warning: Setting IndexCache data on live object, please avoid!")
        self._index_cache_dict = new_index_cache

    def _fullyLoadedFromDisk(self):
        # type: () -> bool
        """This returns a boolean. and it's related to if self has_loaded in the Registry of this object"""
        if self._getRegistry() is not None:
            return self._getRegistry().has_loaded(self)
        return True

    @staticmethod
    def __incrementShareRef(obj, attr_name):
        """
        This increments the shareRef of the prep registry according to the attr_name.name
        Args:
            obj (GangaObject): This is the object which has the attr_name attribute
            attr_name (str): This is the name of the attribute which contains a SharedDir
        """
        shared_dir = getattr(obj, attr_name)

        if hasattr(shared_dir, 'name'):

            from GangaCore.Core.GangaRepository import getRegistry
            shareref = getRegistry("prep").getShareRef()

            logger.debug("Increasing shareref")
            shareref.increase(shared_dir)

    def __copy__(self):
        obj = self.getNew()
        # FIXME: this is different than for deepcopy... is this really correct?
        this_dict = copy(self.__dict__)
        for elem in this_dict.keys():
            if elem not in do_not_copy:
                this_dict[elem] = copy(this_dict[elem])
            else:
                this_dict[elem] = None
        obj._setParent(self._getParent())
        obj._index_cache = {}
        obj._registry = self._registry
        return obj

    # on the deepcopy reset all non-copyable properties as defined in the
    # schema
    def __deepcopy__(self, memo=None):
        """
        Perform a deep copy of the GangaObject class
        Args:
            memo (unknown): Used to track infinite loops etc in the deep-copy of objects
        """

        true_parent = self._getParent()

        self_copy = self.getNew()

        global do_not_copy
        if self._schema is not None:
            for name, item in self._schema.allItems():
                if not item['copyable'] or name in do_not_copy or not hasattr(self, name):
                    setattr(self_copy, name, self._schema.getDefaultValue(name))
                else:
                    setattr(self_copy, name, deepcopy(getattr(self, name)))

                this_attr = getattr(self_copy, name)
                if isinstance(this_attr, Node) and this_attr._getParent() is not self_copy:
                    this_attr._setParent(self_copy)

                if item.isA(SharedItem):
                    self.__incrementShareRef(self_copy, name)

        for k, v in self.__dict__.items():
            if k not in do_not_copy:
                try:
                    self_copy.__dict__[k] = deepcopy(v)
                except:
                    self_copy.__dict__[k] = v

        if true_parent is not None:
            if self._getParent() is not true_parent:
                self._setParent(true_parent)
            if self_copy._getParent() is not true_parent:
                self_copy._setParent(true_parent)
        setattr(self_copy, '_registry', self._registry)
        return self_copy

    def clone(self):
        """Clone self and return a properly initialized object"""
        return deepcopy(self)

    def _getIOTimeOut(self):
        """
        Get the DiskIOTimeout or 5 if this is not defined in the config
        """
        from GangaCore.Utility.Config.Config import getConfig, ConfigError
        try:
            _timeOut = getConfig('Configuration')['DiskIOTimeout']
        except ConfigError as err:
            _timeOut = 5. # 5sec hardcoded default
        return _timeOut

    def _getSessionLock(self, root=None):
        """Acquires the session lock on this object"""
        if root:
            r=root
        else:
            r = self._getRoot()
        reg = r._registry
        if reg is not None:
            reg._acquire_session_lock(r)

    def _releaseSessionLockAndFlush(self):
        """ Releases the session lock for this object
        Please use only if the object is expected to be used by other sessions"""
        r = self._getRoot()
        reg = r._registry
        if reg is not None:
            reg._release_session_lock_and_flush(r)

    def _loadObject(self):
        """If there's an attached registry then ask it to load this object"""
        if self._should_load and self._registry is not None:
            self._registry._load(self)
            self._should_load = False

    # define when the object is read-only (for example a job is read-only in
    # the states other than new)
    def _readonly(self):
        """
        Returns a 1 or 0 depending on if this object is read-only
        TODO: make this True/False
        """
        r = self._getRoot()
        # is object a root for itself? check needed otherwise infinite
        # recursion
        if r is None or r is self:
            return 0
        else:
            return r._readonly()

    def _setRegistry(self, registry):
        """
        Set the Registry of the GangaObject which will manage it. This can only
        set the registry *to* or *from* None and never from one registry to
        another.
        Args:
            registry (GangaRegistry): This is a Ganga Registry object
        """
        assert self._getParent() is None, 'Can only set the registry of a root object'
        if registry is None or self._registry is None:
            self._registry = registry
        elif registry is not self._registry:
            raise RuntimeError('Cannot set registry of {0} to {1} if one is already set ({2}).'.format(type(self), registry, self._registry))

    # get the registry for the object by getting the registry associated with
    # the root object (if any)
    def _getRegistry(self):
        """
        Get the registry which is managing this GangaObject
        The registry is only managing a root object so it gets this first
        """
        return self._getRoot()._registry

    def _getRegistryID(self):
        """
        Get the ID of self within a Registry
        This is normally the .id of an object itself but there is no need for it to be implemented this way
        """
        try:
            return self._registry.find(self)
        except AttributeError as err:
            logger.debug("_getRegistryID Exception: %s" % err)
            return None

    def _setFlushed(self, auto_load_deps=True):
        """
        Un-Set the dirty flag all of the way down the schema.
        Args:
            auto_load_deps (bool): Should we attempt to get objects which may be unloaded and load them via the schema?
        """
        if self._schema and auto_load_deps:
            for k in self._schema.allItemNames():
                this_attr = getattr(self, k)
                if isinstance(this_attr, Node):
                    if not this_attr._dirty:
                        continue
                else:
                    continue
                ## Avoid attributes the likes of job.master which crawl back up the tree
                k_props = self._schema[k].getProperties()
                if not k_props['visitable'] or k_props['transient']:
                    continue
                if isinstance(this_attr, Node) and this_attr._dirty:
                    this_attr._setFlushed()
        super(GangaObject, self)._setFlushed()

    # post __init__ hook automatically called by GPI Proxy __init__
    def _auto__init__(self):
        """
        This is called when an object is constructed from infront of the Proxy automatically, or manually when mimicing the behavior of the IPython prompt
        default behavior is to do nothing
        """
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
        """
        Return the parent Job which manages this object or throw an AssertionError is non exists
        Unknown: Why is this a GangaObject method and not a Node method?
        """
        from GangaCore.GPIDev.Lib.Job import Job
        if self._getParent() is not None:
            r = self._getRoot(cond=lambda o: isinstance(o, Job))
            if not isinstance(r, Job):
                raise AssertionError('No Job associated with object instead root=\'%s\' for \'%s\'' % (repr(r), type(r)))
            return r
        elif isinstance(self, Job):
            return self
        raise AssertionError('No Parent associated with object \'%s\'' % repr(self))

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
    """
    Filter which allows for "obj.x = "Y"   <=> obj.x = Y()"
    TODO evaluate removing this and the architecture behind it
    """
    if isinstance(val, type('')):
        if item is None:
            raise GangaValueError('cannot apply default string conversion, probably you are trying to use it in the constructor')
        from GangaCore.Utility.Plugin import allPlugins, PluginManagerError
        try:
            obj = allPlugins.find(item['category'], val)()
            obj._auto__init__()
            return obj
        except PluginManagerError as err:
            logger.debug("string_type_shortcut_filter Exception: %s" % err)
            raise GangaValueError('Cannot assign string to object, are you sure this is correct?\n%s' % err)
    return None

# FIXME: change into classmethod (do they inherit?) and then change stripComponentObject to use class instead of
# FIXME: object (object model clearly fails with sequence of Files)
# FIXME: test: ../bin/ganga -c local_lhcb.ini run.py
# TestNativeSpecific.testFileSequence


from .Filters import allComponentFilters
allComponentFilters.setDefault(string_type_shortcut_filter)

from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaList

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
