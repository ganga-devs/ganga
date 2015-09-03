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

import Ganga.Utility.logging

import types
from copy import deepcopy

import Ganga.GPIDev.Schema as Schema

from Ganga.GPIDev.Base.Proxy import GPIProxyClassFactory, ProxyDataDescriptor, ProxyMethodDescriptor, GangaAttributeError, TypeMismatchError, isType, stripProxy, GPIProxyObjectFactory
from Ganga.Core.exceptions import GangaValueError, GangaException

from Ganga.GPIDev.Base.VPrinter import VPrinter, VSummaryPrinter

logger = Ganga.Utility.logging.getLogger(modulename=1)


class PreparedStateError(GangaException):

    def __init__(self, txt=''):
        GangaException.__init__(self, txt)
        self.txt = txt

    def __str__(self):
        return "PreparedStateError: %s" % str(self.txt)


class Node(object):
    _parent = None
    _index_cache = None
    _ref_list = ['_parent', '_registry', '_index_cache']

    def __init__(self, parent):
        self._data = {}
        self._setParent(parent)
        super(Node, self).__init__()

    def __getstate__(self):
        d = self.__dict__.copy()
        d['_data'] = d['_data'].copy()
        for r in self._ref_list:
            d[r] = None
        return d

    def __setstate__(self, dict):
        for n, v in dict['_data'].items():
            if isType(v, Node):
                v._setParent(self)
            if hasattr(v, "__iter__") and not hasattr(v, "iteritems"):
                # set the parent of the list or dictionary (or other iterable)
                # items
                for i in v:
                    if isType(i, Node):
                        i._setParent(self)

        self.__dict__ = dict

    def __copy__(self, memo=None):
        cls = type(self)
        obj = super(cls, cls).__new__(cls)
        # FIXME: this is different than for deepcopy... is this really correct?
        d = self.__dict__.copy()
        obj.__dict__ = d
        return obj

    def __deepcopy__(self, memo=None):
        cls = type(stripProxy(self))
        obj = super(cls, cls).__new__(cls)
        d = stripProxy(self).__getstate__()
        for n in d:
            #print "%s::%s" % (str(self.__class__.__name__), str(n))
            if n not in self._ref_list:
                d[n] = deepcopy(d[n], memo)  # FIXED
            else:
                d[n] = None
        obj.__setstate__(d)
        obj._parent = None
        obj._index_cache = None
        obj._registry = stripProxy(self)._registry
        return obj

    def _getParent(self):
        return self._parent
        # if "_data" in self.__dict__ and not self._data is None:
        #    return self._data['parent']
        # return None

    def _setParent(self, parent):
        self._parent = parent
        # if not self._data is None:
        #    self._data['parent'] = parent

    # get the root of the object tree
    # if parent does not exist then the root is the 'self' object
    # cond is an optional function which may cut the search path: when it
    # returns True, then the parent is returned as root
    def _getRoot(self, cond=None):
        if self._parent is None:
            return self
        root = None
        obj = self
        while obj is not None:
            root = obj
            if cond and cond(root):
                break
            obj = obj._getParent()
        return root

    # accept a visitor pattern
    def accept(self, visitor):

        if not hasattr(self, '_schema'):
            return
        if self._schema is None:
            visitor.nodeBegin(self)
            visitor.nodeEnd(self)
            return

        visitor.nodeBegin(self)

        def getdata(name):
            try:
                return getattr(self, name)
            except AttributeError, err:
                logger.debug("accept visitor error: %s" % str(err))
                return self._data[name]

        for (name, item) in self._schema.simpleItems():
            if item['visitable']:
                visitor.simpleAttribute(self, name, getdata(name), item['sequence'])

        for (name, item) in self._schema.sharedItems():
            if item['visitable']:
                visitor.sharedAttribute(self, name, getdata(name), item['sequence'])

        for (name, item) in self._schema.componentItems():
            if item['visitable']:
                visitor.componentAttribute(self, name, getdata(name), item['sequence'])

        visitor.nodeEnd(self)

    # clone self and return a properly initialized object
    def clone(self):
        return deepcopy(self)

    # copy all the properties recursively from the srcobj
    # if schema of self and srcobj are not compatible raises a ValueError
    # ON FAILURE LEAVES SELF IN INCONSISTENT STATE
    def copyFrom(self, srcobj, _ignore_atts=[]):
        # Check if this object is derived from the source object, then the copy
        # will not throw away information
        if not isType(self, srcobj.__class__) and not isType(srcobj, self.__class__):
            raise GangaValueError("copyFrom: Cannot copy from %s to %s!" % (srcobj.__class__, self.__class__))

        if not hasattr(self, '_schema'):
            return

        if self._schema is None and srcobj._schema is None:
            return

        if srcobj._schema is None:
            self._schema = None
            return

        for name, item in self._schema.allItems():
            if name in _ignore_atts:
                continue
            #logger.debug("Copying: %s : %s" % (str(name), str(item)))
            if name is 'application' and hasattr(srcobj.application, 'is_prepared'):
                if srcobj.application.is_prepared is not None and srcobj.application.is_prepared is not True:
                    srcobj.application.incrementShareCounter(srcobj.application.is_prepared.name)
            if not self._schema.hasAttribute(name):
                #raise ValueError('copyFrom: incompatible schema: source=%s destination=%s'%(srcobj._name,self._name))
                setattr(self, name, self._schema.getDefaultValue(name))
            elif not item['copyable']:
                setattr(self, name, self._schema.getDefaultValue(name))
            else:
                c = deepcopy(getattr(srcobj, name))
                setattr(self, name, c)

    def printTree(self, f=None, sel=''):
        self.accept(VPrinter(f, sel))

    #printPrepTree is only ever run on applications, from within IPrepareApp.py
    #if you (manually) try to run printPrepTree on anything other than an application, it will not work as expected
    #see the relevant code in VPrinter to understand why
    def printPrepTree(self, f=None, sel='preparable' ):
        self.accept(VPrinter(f, sel))

    def printSummaryTree(self, level=0, verbosity_level=0, whitespace_marker='', out=None, selection=''):
        """If this method is overridden, the following should be noted:

        level: the hierachy level we are currently at in the object tree.
        verbosity_level: How verbose the print should be. Currently this is always 0.
        whitespace_marker: If printing on multiple lines, this allows the default indentation to be replicated.
                           The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
        out: An output stream to print to. The last line of output should be printed without a newline.'
        selection: See VPrinter for an explaintion of this.
        """
        self.accept(VSummaryPrinter(level, verbosity_level, whitespace_marker, out, selection))

    def __eq__(self, node):

        if self is node:
            return 1
        if not node:  # or not self._schema.isEqual(node._schema):
            return 0

        if not isType(node, type(self)):
            return 0

        if (hasattr(self, '_schema') and self._schema is None) and (hasattr(node, '_schema') and node._schema is None):
            return 1
        elif (hasattr(self, '_schema') and self._schema is not None) and (hasattr(node, '_schema') and node._schema is None):
            return 0
        elif (hasattr(self, '_schema') and self._schema is None) and (hasattr(node, '_schema') and node._schema is not None):
            return 0
        else:
            if (hasattr(self, '_schema') and hasattr(node, '_schema')) and (not self._schema.isEqual(node._schema)):
                return 0

        ## logging code useful for debugging
        for (name, item) in self._schema.allItems():
            if item['comparable'] == True:
                #logger.info("testing: %s::%s" % (str(self.__class__.__name__), str(name)))
                if getattr(self, name) != getattr(node, name):
                    #logger.info( "diff: %s::%s" % (str(self.__class__.__name__), str(name)))
                    return 0

        return 1

    def __ne__(self, node):
        return not self == node

##########################################################################


class Descriptor(object):

    def __init__(self, name, item):
        self._name = name
        self._item = item
        self._getter_name = None
        self._checkset_name = None
        self._filter_name = None


        if not hasattr( item, '_meta'):
            return

        if 'getter' in item._meta:
            self._getter_name = item['getter']

        if 'checkset' in item._meta:
            self._checkset_name = item['checkset']

        if 'filter' in item._meta:
            self._filter_name = item['filter']


    def _bind_method(self, obj, name):
        if name is None:
            return None
        return getattr(obj, name)

    def _check_getter(self):
        if self._getter_name:
            raise AttributeError('cannot modify or delete "%s" property (declared as "getter")' % self._name)

    def __get__(self, obj, cls):
        if obj is None:
            return cls._schema[self._name]
        else:
            result = None
            g = self._bind_method(obj, self._getter_name)
            if g:
                result = g()
            else:
                # LAZYLOADING
                lookup_result = None

                try:
                    if obj._index_cache:
                        if self._name in obj._index_cache.keys():
                            lookup_result = obj._index_cache[self._name]
                except Exception, err:
                    logger.debug("Lazy Loading Exception: %s" % str(err))
                    lookup_result = None
                    pass

                if hasattr(obj, '_data'):
                    if (obj._data is None) and (not obj._index_cache is None) and (lookup_result is not None):
                        result = lookup_result
                    else:
                        obj._getReadAccess()
                        if self._name in obj._data:
                            result = obj._data[self._name]
                        else:
                            from Ganga.GPIDev.Base.Proxy import isProxy
                            if isProxy(obj._data):
                                if self._name in stripProxy(self._data):
                                    result = stripProxy(obj._data)[self._name]
                                else:
                                    logger.debug("Error, cannot find %s parameter in %s" % (self._name, obj._name))
                                    GangaException("Error, cannot find %s parameter in %s" % (self._name, obj._name))
                                    result = obj._data[self._name]
                            else:
                                logger.debug("Error, cannot find %s parameter in %s" % (self._name, obj._name))
                                GangaException("Error, cannot find %s parameter in %s" % (self._name, obj._name))
                                result = obj._data[self._name]
                else:
                    err = GangaException("Error finding parameter %s in object %s" % (str(self._name, obj._name)))
                    raise err

            return result

    def __set__(self, obj, val):

        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaList

        cs = self._bind_method(obj, self._checkset_name)
        if cs:
            cs(val)
        filter = self._bind_method(obj, self._filter_name)
        if filter:
            val = filter(val)

        # LOCKING
        obj._getWriteAccess()

        # self._check_getter()

        def cloneVal(v):
            if v is None:
                assert(item['optional'])
                return None
            else:
                assert(isType(v, Node))
                if isType(v, GangaList):
                    catagories = v.getCategory()
                    len_cat = len(catagories)
                    # we pass on empty lists, as the catagory is yet to be
                    # defined
                    if (len_cat > 1) or ((len_cat == 1) and (catagories[0] != item['category'])):
                        raise GangaAttributeError('%s: attempt to assign a list containing incompatible objects %s to the property in category "%s"' % (
                            self._name, v, item['category']))
                else:
                    if stripProxy(v)._category != item['category']:
                        raise GangaAttributeError('%s: attempt to assign an incompatible object %s to the property in category "%s"' % (self._name, v, item['category']))
                v = stripProxy(v).clone()
                v._setParent(obj)
                return v

        item = obj._schema[self._name]

        if item.isA(Schema.ComponentItem):
            if item['sequence']:
                if item['preparable']:
                    val = makeGangaList(val, cloneVal, parent=obj, preparable=True)
                else:
                    val = makeGangaList(val, cloneVal, parent=obj)
            else:
                val = cloneVal(val)

        else:
            if item['sequence']:
                if item['preparable']:
                    val = makeGangaList(val, parent=obj, preparable=True)
                else:
                    val = makeGangaList(val, parent=obj)

        obj._data[self._name] = val

        obj._setDirty()

    def __delete__(self, obj):
        # self._check_getter()
        del obj._data[self._name]


def export(method):
    """
    Decorate a GangaObject method to be exported to the GPI
    """
    method.exported_method = True
    return method


class ObjectMetaclass(type):
    _descriptor = Descriptor

    def __init__(cls, name, bases, dict):
        super(ObjectMetaclass, cls).__init__(name, bases, dict)

        # ignore the 'abstract' base class
        # FIXME: this mechanism should be based on explicit cls._name or alike
        if name == 'GangaObject':
            return

        logger.debug("Metaclass.__init__: class %s name %s bases %s", cls, name, bases)

        # all Ganga classes must have (even empty) schema
        if not hasattr(cls, '_schema') or cls._schema is None:
            cls._schema = Schema.Schema(None, None)

        # Add all class members of type `Schema.Item` to the _schema object
        # TODO: We _could_ add base class's Items here by going through `bases` as well.
        for member_name, member in dict.items():
            if isinstance(member, Schema.Item):
                cls._schema.datadict[member_name] = member

        # produce a GPI class (proxy)
        proxyClass = GPIProxyClassFactory(name, cls)

        if not hasattr(cls, '_exportmethods'):
            cls._exportmethods = []

        # export public methods of this class and also of all the bases
        # this class is scanned last to extract the most up-to-date docstring
        dicts = (b.__dict__ for b in reversed(cls.__mro__))
        for d in dicts:
            for k in d:
                if k in cls._exportmethods or getattr(d[k], 'exported_method', False):

                    internal_name = "_export_" + k
                    if internal_name not in d.keys():
                        internal_name = k
                    try:
                        method = d[internal_name]
                    except Exception, err:
                        logger.debug("ObjectMetaClass Error internal_name: %s,\t d: %s" % (str(internal_name), str(d)))
                        logger.debug("ObjectMetaClass Error: %s" % str(err))

                    if not isinstance(method, types.FunctionType):
                        continue
                    f = ProxyMethodDescriptor(k, internal_name)
                    f.__doc__ = method.__doc__
                    setattr(proxyClass, k, f)

        # sanity checks for schema...
        if '_schema' not in dict.keys():
            s = "Class %s must _schema (it cannot be silently inherited)" % (name,)
            logger.error(s)
            raise ValueError(s)

        # If a class has not specified a '_name' then default to using the class '__name__'
        if not cls.__dict__.get('_name'):
            cls._name = name

        if cls._schema._pluginclass is not None:
            logger.warning('Possible schema clash in class %s between %s and %s',
                           name, cls._name, cls._schema._pluginclass._name)

        # export visible properties... do not export hidden properties
        for attr, item in cls._schema.allItems():
            setattr(cls, attr, cls._descriptor(attr, item))
            if not item['hidden']:
                setattr(proxyClass, attr, ProxyDataDescriptor(attr))

        # additional check of type
        # bugfix #40220: Ensure that default values satisfy the declared types
        # in the schema
        for attr, item in cls._schema.simpleItems():
            if not item['getter']:
                item._check_type(
                    item['defvalue'], '.'.join([name, attr]), enableGangaList=False)

        # create reference in schema to the pluginclass
        cls._schema._pluginclass = cls

        # store generated proxy class
        cls._proxyClass = proxyClass

        # register plugin class
        if hasattr(cls, '_declared_property'):
            # if we've not even declared this we don't want to use it!
            if not cls._declared_property('hidden') or cls._declared_property('enable_plugin'):
                from Ganga.Utility.Plugin import allPlugins
                allPlugins.add(cls, cls._category, cls._name)

            # create a configuration unit for default values of object properties
            if not cls._declared_property('hidden') or cls._declared_property('enable_config'):
                cls._schema.createDefaultConfig()


class GangaObject(Node):
    __metaclass__ = ObjectMetaclass
    _schema = None  # obligatory, specified in the derived classes
    _proxyClass = None  # created automatically
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

    _lock_count = {}

    # additional properties that may be set in derived classes which were declared as _hidden:
    #   _enable_plugin = 1 -> allow registration of _hidden classes in the allPlugins dictionary
    # _enable_config = 1 -> allow generation of [default_X] configuration
    # section with schema properties

    # the constructor is directly used by the GPI proxy so the GangaObject
    # must be fully initialized
    def __init__(self):
        # IMPORTANT: if you add instance attributes like in the line below
        # make sure to update the __getstate__ method as well
        # use cache to help preserve proxy objects identity in GPI
        self._proxyObject = None
        # dirty flag is true if the object has been modified locally and its
        # contents is out-of-sync with its repository
        self._dirty = False

        super(GangaObject, self).__init__(None)

        if self._schema is not None and hasattr(self._schema, 'allItems'):
            for attr, item in self._schema.allItems():
                setattr(self, attr, self._schema.getDefaultValue(attr))

        self._lock_count = {}
        # Overwrite default values with any config values specified
        # self.setPropertiesFromConfig()

    # construct an object of this type from the arguments. Defaults to copy
    # constructor.
    def __construct__(self, args):
        self._lock_count = {}
        # act as a copy constructor applying the object conversion at the same
        # time (if applicable)
        if len(args) == 0:
            return
        elif len(args) == 1:
            self.copyFrom(args[0])
        else:
            raise TypeMismatchError("Constructor expected one or zero non-keyword arguments, got %i" % len(args))

    def __getstate__(self):
        # IMPORTANT: keep this in sync with the __init__
        self._getReadAccess()
        dict = super(GangaObject, self).__getstate__()
        dict['_proxyObject'] = None
        dict['_dirty'] = False
        return dict

    def __setstate__(self, dict):
        self._getWriteAccess()
        super(GangaObject, self).__setstate__(dict)
        self._setParent(None)
        self._proxyObject = None
        self._dirty = False

    # on the deepcopy reset all non-copyable properties as defined in the
    # schema
    def __deepcopy__(self, memo=None):
        self._getReadAccess()
        c = super(GangaObject, self).__deepcopy__(memo)
        if self._schema is not None:
            for name, item in self._schema.allItems():
                if not item['copyable']:
                    setattr(c, name, self._schema.getDefaultValue(name))
                if item.isA(Schema.SharedItem):
                    shared_dir = getattr(c, name)
                    try:
                        from Ganga.Core.GangaRepository import getRegistry
                        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
                        logger.debug("Increasing shareref")
                        shareref.increase(shared_dir.name)
                    except AttributeError, err:
                        logger.debug("__deepcopy__ Exception: %s" % str(err))
                        pass
        c.lock_count = {}
        return c

    def accept(self, visitor):
        self._getReadAccess()
        super(GangaObject, self).accept(visitor)

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
                try:
                    reg._write_access(root)
                    _haveLocked = True
                except (RegistryLockError, RegistryAccessError) as x:
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
        try:
            return r._registry
        except AttributeError, err:
            logger.debug("_getRegistry Exception: %s" % str(err))
            return None

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
        if self._registry is not None:
            self._registry._dirty(self)

    def _setFlushed(self):
        self._dirty = False

    # post __init__ hook automatically called by GPI Proxy __init__
    def _auto__init__(self):
        pass

    # return True if _name attribute was explicitly defined in the class
    # this means that implicit (inherited) _name attribute has no effect in the derived class
    # example: cls._declared_property('hidden') => True if there is class
    # attribute _hidden explicitly declared
    def _declared_property(self, name):
        return '_' + name in self.__dict__

    _declared_property = classmethod(_declared_property)

    # get the job object associated with self or raise an assertion error
    # the FIRST PARENT Job is returned...
    # this method is for convenience and may well be moved to some subclass
    def getJobObject(self):
        from Ganga.GPIDev.Lib.Job import Job
        r = self._getRoot(cond=lambda o: isType(o, Job))
        if not isType(r, Job):
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
        if (hasattr(v, '_on_attribute__set__')):
            return v._on_attribute__set__(self, name)
        return v

# define the default component object filter:
# obj.x = "Y"   <=> obj.x = Y()


def string_type_shortcut_filter(val, item):
    if isType(val, type('')):
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
