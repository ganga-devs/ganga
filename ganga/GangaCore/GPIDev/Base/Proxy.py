
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Proxy.py,v 1.2.4.3 2009-07-10 11:29:27 ebke Exp $
##########################################################################

import GangaCore.Utility.logging
from GangaCore.Utility.Config import getConfig

from GangaCore.GPIDev.Schema import ComponentItem

from GangaCore.Core.exceptions import GangaException, GangaValueError
from GangaCore.GPIDev.Base.Objects import Node, GangaObject, ObjectMetaclass, _getName
from GangaCore.Core.exceptions import GangaAttributeError, ProtectedAttributeError, ReadOnlyObjectError, TypeMismatchError

import collections
import functools
import os

from inspect import isclass, getfullargspec

import types

from copy import deepcopy

implRef = '_impl'
proxyClass = '_proxyClass'
proxyObject = '_proxyObject'

prepconfig = getConfig('Preparable')

logger = GangaCore.Utility.logging.getLogger()

# some proxy related convieniance methods

_knownLists = None

_stored_Interface = None

_eval_cache = {}

def setProxyInterface(my_interface):
    """ Set the proxy interface, not strictly needed for GangaCore.GPI but good practice as we move to 'ganga' """
    global _stored_Interface
    _stored_Interface = my_interface

def getProxyInterface():
    """ Get the proxy interface  GangaCore.GPI by default, 'ganga' if it's be set to this """
    if not _stored_Interface:
        import GangaCore.GPI
        setProxyInterface(GangaCore.GPI)
    return _stored_Interface

def getRuntimeGPIObject(obj_name, silent=False, evalClass=True):
    """ Get, or attempt to get an object from the GPI, if it exists then return a new instance if a class or an object if it's not
       If it doesn't exist attempt to evaluate the obj_name as a string like a standard python object
       If it's none of the above then return 'None' rather than the object string which was input
       Args:
           obj_name(str): This is the object we want to get from the GPI
           silent(bool): Should we be silent about errors?
           evalClass(bool): Should we create an instance of a class object when it's returned"""
    interface = getProxyInterface()
    if obj_name in interface.__dict__:
        this_obj = interface.__dict__[obj_name]
        if evalClass and isclass(this_obj):
            return this_obj()
        else:
            return this_obj
    else:
        returnable = raw_eval(obj_name)
        if returnable == obj_name:
            if silent is False:
                logger.error("Cannot find Object: '%s' in GPI. Returning None." % obj_name)
            return None
        return returnable

def runtimeEvalString(this_obj, attr_name, val):
    """
     Return the evaluated value of the 'val' after checking the schema and attributes associated with this_obj and attr_name
     If the attribute or the Schema are or allow for string objects then val is not evaluated but if it does allow for non string objects and isn't then an eval is performed
     This is ugly and is a direct consequence of allowing j.backend = 'Dirac' which in this authors (rcurrie) opinion is going to hurt us later
    """

    ## Don't check or try to auto-eval non-string objects
    if not isinstance(val, str):
        return val

    raw_obj = stripProxy(this_obj)
    shouldEval = None

    ## Lets check the Schema to see if a string object is allowed here
    ## If this is a ComponentItem we know in advance that we need to try and evaluate this
    ## If the object is NOT a ComponentItem but is still in the schema, check to see if the object is allowed to be a string or not
    ## If an object is NOT allowed to be a string then it should be 'eval'-ed
    if hasattr(raw_obj, '_schema'):

        if not raw_obj._schema.hasAttribute(attr_name):
            shouldEval = True
        else:
            this_attr = raw_obj._schema.getItem(attr_name)
            if isType(this_attr, ComponentItem):
                ## This is a component Item and isn't equivalent to a string
                shouldEval = True
            else:
                allowedTypes = this_attr.getProperties()['typelist']
                for this_type in allowedTypes:
                    if this_type == str:
                        ## This type is a string and shouldn't be evaluated
                        shouldEval = False
                        break
                    else:
                        ## This type is written as a string so need to work out what it is
                        if type(this_type) == str:
                            try:
                                interface = getProxyInterface() 
                                eval_type = eval(this_type, interface.__dict__)
                                if eval_type == str:
                                    ## This type was written as "str" ... slightly annoying but OK...
                                    shouldEval = False
                                    break
                                else:
                                    ## This type is NOT a string so based on this we should Eval
                                    shouldEval = True
                            except Exception as err:
                                logger.debug("Failed to evalute type: %s" % this_type)
                                logger.debug("Err: %s" % err)
                                ## We can't eval in this case. It may just be the type which is broken
                                shouldEval = True
                        else:
                            ## This type isn't in a string format so we don't need to work out what it is
                            shouldEval = True

    ## If the attribute is not in the Schema lets see if this class instance knows about this object or not
    ## If the attribute is NOT a string but is in this instane then we should try and eval
    ## But if it's known by the instance and is a string, we should just use the value
    elif hasattr(raw_obj, attr_name):

        if type(getattr(raw_obj, attr_name)) == str:
            shouldEval = False
        else:
            shouldEval = True

    ## If the object is not in the schema then try and eval the object anyway as we a-priori don't know any better
    ## THIS IS POTENTIALLY DANGEROUS AND IF A LOT OF USERS COMPLAIN THIS SHOULD BE REVERSED TO FALSE!!!
    else:
        shouldEval = True

    assert(shouldEval is not None)

    if shouldEval is True:
        new_val = raw_eval(val)
    else:
        new_val = val

    return new_val



def raw_eval(val):
    """
     Attempts to evaluate the val object and return the object it evaluates to if it is a Python object
     Makes use of basic caching as we don't expect that things at this level should change.
     Args:
        val(str): This is the string which we're looking to evaluate from 'in front' of the proxy
     """

    if val in _eval_cache:
        return deepcopy(_eval_cache[val])

    try:
        interface = getProxyInterface() 
        temp_val = eval(val, interface.__dict__)
        if isclass(temp_val):
            new_val = temp_val()
        else:
            new_val = temp_val
    except Exception as err:
        ## Useful for debugging these
        ## import traceback; traceback.print_stack()
        logger.debug("Proxy Cannot evaluate v=: '%s'" % val)
        logger.debug("Using raw value instead")
        new_val = val

    raw_val = stripProxy(new_val)
    if hasattr(raw_val, '_auto__init__'):
        raw_val._auto__init__()

    _eval_cache[val] = new_val

    return deepcopy(new_val)

def getKnownLists():
    """ Returns the list of iterable objects, tuple, list and maybe GangaList which we can use here due to import """
    global _knownLists
    if _knownLists is None:
        try:
            from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
        except ImportError:
            _knownLists = None
            return (tuple, list)
        _knownLists = (tuple, list, GangaList)
    return _knownLists

def isProxy(obj):
    """Checks if an object is a proxy
    Args:
        obj (object): This may be an instance or a class
    """
#    return isinstance(obj.__class__, GPIProxyObject)
    # Alex changed for below as doesn't check class objects, only instances
    # e.g. isProxy(DiracFile) fails at the Ganga prompt
    if isclass(obj):
        return issubclass(obj, GPIProxyObject) or hasattr(obj, implRef)
    else:
        obj_class = obj.__class__
        return issubclass(obj_class, GPIProxyObject) or hasattr(obj_class, implRef)

def isType(_obj, type_or_seq):
    """Checks whether on object is of the specified type, stripping proxies as needed.
    Args:
        obj (object): This may be an instance or a class
        type_or_seq (type, list, tuple, GangaList): This may be an individual type or an iterable list of types
    """

    obj = stripProxy(_obj)

    bare_type_or_seq = stripProxy(type_or_seq)

    ## Here to avoid circular GangaObject dependencies
    ## is type_or_seq iterable?
    if isinstance(type_or_seq, getKnownLists()):
        clean_list = []
        for type_obj in type_or_seq:
            if type_obj != str and type(type_obj) != type(str) and (not isclass(type_obj)):
                clean_list.append(type(stripProxy(type_obj)))
            elif isclass(type_obj):
                clean_list.append(type_obj)
            else:
                clean_list.append(type_obj)

        return isinstance(obj, tuple(clean_list))

    else:
        return isinstance(obj, bare_type_or_seq)

def getName(_obj):
    """Strip any proxy and then return an objects name
    Args:
        _obj (object): This may be an instance or a class"""
    obj = stripProxy(_obj)
    returnable = _getName(obj)
    return returnable

def is_namedtuple_instance(obj):
    if len(type(obj).__bases__) != 1 or not isinstance(obj, tuple):
        return False
    fields = getattr(obj, '_fields', None)
    if not isinstance(fields, tuple):
        return False
    return all(isinstance(n, str) for n in fields)

def stripProxy(obj):
    """Removes the proxy if there is one
    Args:
        obj (object): This may be an instance or a class
    """
    if is_namedtuple_instance(obj):
        return type(obj)(*[stripProxy(_) for _ in obj])
    elif isinstance(obj, (list, tuple)):
        return type(obj)(stripProxy(_) for _ in obj)
    elif isinstance(obj, dict):
        return dict((k, stripProxy(v)) for k, v in obj.items())
    elif hasattr(obj, implRef):
        return getattr(obj, implRef)
    else:
        return obj


def addProxy(obj):
    """Adds a proxy to a GangaObject instance or class
    Args:
        obj (GangaObject): This may be a Ganga object which you're wanting to add a proxy to
    """
    if isType(obj, GangaObject):
        if not isProxy(obj):
            if hasattr(obj, proxyObject):
                return getattr(obj, proxyObject)
            else:
                return GPIProxyObjectFactory(obj)
    elif isclass(obj) and issubclass(obj, GangaObject):
        return getProxyClass(obj)
    elif is_namedtuple_instance(obj):
        return type(obj)(*[addProxy(_) for _ in obj])
    elif isinstance(obj, (list, tuple)):
        return type(obj)(addProxy(_) for _ in obj)
    elif isinstance(obj, dict):
        return dict((k, addProxy(v)) for k, v in obj.items())
    return obj


def getProxyAttr(obj, attr_name):
    """Gets an attribute from a proxied object"""
    return getattr(stripProxy(obj), attr_name)


def runProxyMethod(obj, method_name, *args):
    """Calls a method on the object, removing the proxy if needed"""
    fp = getProxyAttr(obj, method_name)
    return fp(*args)


def export(method):
    """
    Decorate a GangaObject method to be exported to the GPI
    """
    method.exported = True
    return method

# apply object conversion or if it fails, strip the proxy and extract the
# object implementation


def stripComponentObject(v, cfilter, item):

    def getImpl(v):
        if v is None:
            if not item['optional']:
                raise TypeMismatchError(None, 'component(%s) is mandatory and None may not be used' % getName(item))
                return v
            else:
                return None
        if isType(v, GangaObject):
            return v
        if not isinstance(v, GPIProxyObject):
            raise TypeMismatchError("cannot assign value '%s', expected a '%s' object " % (repr(v), item['category']))
        return stripProxy(v)

    vv = cfilter(v, item)
    if vv is None:
        return getImpl(v)
    else:
        return vv

from GangaCore.GPIDev.TypeCheck import _valueTypeAllowed
valueTypeAllowed = lambda val, valTypeList: _valueTypeAllowed(val, valTypeList, logger)


class ProxyDataDescriptor(object):

    __slots__ = ('_name', )

    def __init__(self, name):
        """
        Descriptor which sits in fromnt  of raw unproxied objects
        Args:
            name (str): Name of the attribute which we're looking after here
        """
        self._name = name

    # apply object conversion or if it failes, make the wrapper proxy
    def disguiseComponentObject(self, v):
        # get the proxy for implementation object
        def getProxy(v):
            if not isType(v, GangaObject):
                raise GangaAttributeError("invalid type: cannot assign '%s' to attribute '%s'" % (repr(v), getName(self)))
            return GPIProxyObjectFactory(v)

        # convert implementation object to GPI value according to the
        # static method defined in the implementation object
        def object2value(v):
            return None

        vv = object2value(v)
        if vv is None:
            # allow None to be a legal value for optional component items
            if v is None:
                return None
            else:
                return getProxy(v)

        else:
            return vv

    # apply attribute conversion
    def disguiseAttribute(self, v):
        if isType(v, GangaObject):
            return GPIProxyObjectFactory(v)
        return v

    def __get__(self, obj, cls):

        # at class level return a helper object (for textual description)
        if obj is None:
            # return Schema.make_helper(getattr(getattr(cls, implRef), getName(self)))
            return getattr(stripProxy(cls), getName(self))

        raw_obj = stripProxy(obj)
        try:
            val = getattr(raw_obj, getName(self))
        except Exception as err:
            if getName(self) in raw_obj.__dict__:
                val = raw_obj.__dict__[getName(self)]
            else:
                val = getattr(raw_obj, getName(self))

        # wrap proxy
        item = raw_obj._schema[getName(self)]

        if item['proxy_get']:
            return getattr(raw_obj, item['proxy_get'])()

        if isType(item, ComponentItem):
            disguiser = self.disguiseComponentObject
        else:
            disguiser = self.disguiseAttribute

        ## FIXME Add GangaList?
        if item['sequence'] and isType(val, list):
            from GangaCore.GPIDev.Lib.GangaList.GangaList import makeGangaList
            val = makeGangaList(val, disguiser)

        returnable = disguiser(val)
        

        if isType(returnable, GangaObject):
            return addProxy(returnable)
        else:
            return returnable

    @staticmethod
    def _check_type(obj, val, attr_name):
        item = stripProxy(obj)._schema[attr_name]
        return item._check_type(val, attr_name)

    # apply attribute conversion
    @staticmethod
    def _stripAttribute(obj, v, name):
        # just warn
        # print '**** checking',v,v.__class__,
        # isinstance(val,GPIProxyObject)
        new_v = None
        if isinstance(v, list):
            from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList
            v_new = GangaList()
            for elem in v:
                v_new.append(elem)
        if isinstance(v, GPIProxyObject) or hasattr(v, implRef):
            new_v = stripProxy(v)
            logger.debug('%s property: assigned a component object (%s used)' % (name, implRef))

        if new_v is None:
            new_v = v
        return obj._attribute_filter__set__(name, new_v)

    @staticmethod
    def __app_set__(obj, val):

        if not hasattr(obj, 'application') or obj.application is None:
            return

        if hasattr(obj.application, '_is_prepared'):

            #a=Job(); a.prepare(); a.application=Executable()
            if obj.application.is_prepared not in [None, True] and\
                 hasattr(val, 'is_prepared') and val.is_prepared is None:
                 logger.debug('Overwriting a prepared application with one that is unprepared')
                 obj.application.unprepare()

            #a=Job(); b=Executable(); b.prepare(); a.application=b
        elif obj.application.is_prepared is not True:
            if hasattr(val, 'is_prepared'):
                if val.is_prepared not in [None, True]:
                    from GangaCore.Core.GangaRepository import getRegistry
                    shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
                    logger.debug('Overwriting application with a prepared one')
                    raw_app = stripProxy(obj.application)
                    if raw_app != val:
                        raw_app.unprepare()
                        shareref.increase(val.is_prepared)

            # check that the shared directory actually exists before
            # assigning the (prepared) application to a job
            if hasattr(val, 'is_prepared'):
                if val.is_prepared not in [None, True]:
                    if hasattr(val.is_prepared, 'name'):
                        from GangaCore.Utility.files import expandfilename
                        Config_conf = getConfig('Configuration')
                        shared_path = os.path.join(expandfilename(Config_conf['gangadir']), 'shared', Config_conf['user'])
                        if not os.path.isdir(os.path.join(shared_path, val.is_prepared.name)):
                            logger.error('ShareDir directory not found: %s' % val.is_prepared.name)

    @staticmethod
    def __prep_set__(obj, val):

        # if we set is_prepared to None in the GPI, that should effectively
        # unprepare the application
        if val is None:
            if obj.is_prepared is not None:
                logger.info('Unpreparing application.')
                obj.unprepare()

        # Replace is_prepared on an application for another ShareDir object
        if hasattr(obj, '_getRegistry'):
            from GangaCore.GPIDev.Lib.File import ShareDir
            if obj._getRegistry() is not None and isType(val, ShareDir):
                logger.debug('Overwriting is_prepared attribute with a ShareDir object')
                # it's safe to unprepare 'not-prepared' applications.
                obj.unprepare()
                from GangaCore.Core.GangaRepository import getRegistry
                shareref = getRegistry("prep").getShareRef()
                shareref.increase(val)

        if isinstance(val, str):
            logger.error("Setting string type to 'is_prepared'")
            #import traceback
            #traceback.print_stack()

    @staticmethod
    def __sequence_set__(stripper, obj, val, name):
        item = obj._schema[name]
        # we need to explicitly check for the list type, because simple
        # values (such as strings) may be iterable
        new_v = None
        from GangaCore.GPIDev.Lib.GangaList.GangaList import makeGangaList
        if isType(val, getKnownLists()):

            # create GangaList
            if stripper is not None:
                new_v = makeGangaList(val, stripper)
            else:
                temp_v = ProxyDataDescriptor._stripAttribute(obj, val, name)
                new_v = makeGangaList(temp_v)
        else:
            # val is not iterable
            if item['strict_sequence']:
                raise GangaAttributeError('cannot assign a simple value %s to a strict sequence attribute %s.%s (a list is expected instead)' % (repr(val), getName(obj), name))
            if stripper is not None:
                new_v = makeGangaList(stripper(val))
            else:
                temp_v = ProxyDataDescriptor._stripAttribute(obj, val, name)
                new_v = makeGangaList(temp_v)

        if new_v is None:
            new_v = val
        return new_v

    @staticmethod
    def __preparable_set__(obj, val, name):
        if obj.is_prepared is not None:
            if obj.is_prepared is not True:
                raise ProtectedAttributeError('AttributeError: "%s" attribute belongs to a prepared application and so cannot be modified.\
                                                unprepare() the application or copy the job/application (using j.copy(unprepare=True)) and modify that new instance.' % (name,))

    ## Inspect this given item to determine if it has editable attributes if it has been set as read-only
    ## Curently Unused although may be useful to keep
    @staticmethod
    def __subitems_read_only(obj):
        can_be_modified = []
        for name, item in obj._schema.allItems():
            ## This object inherits from Node therefore likely has a schema too.
            obj_attr = getattr(obj, name)
            if isType(obj_attr, Node):
                can_be_modified.append( ProxyDataDescriptor.__subitems_read_only(obj_attr) )
            else:

                ## This object doesn't inherit from Node and therefore needs to be evaluated
                if item.getProperties()['changable_at_resubmit']:
                    can_be_modified.append( True )
                else:
                    can_be_modified.append( False )
        
        can_modify = False
        for i in can_be_modified:
            can_modify = can_modify or i

        return can_modify

    @staticmethod
    def __recursive_strip(_val):
        ## Strip the proxies recursively for things like nested lists
        raw_val = stripProxy(_val)
        if isinstance(_val, collections.abc.Sequence) and not isinstance(_val, str):
            val = raw_val.__class__()
            if isinstance(val, dict):
                for _key, elem in _val.items():
                    if isType(_key, GangaObject):
                        key = stripProxy(_key)
                    else:
                        key = _key
                    if isType(elem, GangaObject):
                        val[key] = ProxyDataDescriptor.__recursive_strip(stripProxy(elem))
                    else:
                        val[key] = elem
            else:
                for elem in _val:
                    if isType(elem, GangaObject):
                        val.append(ProxyDataDescriptor.__recursive_strip(stripProxy(elem)))
                    else:
                        val.append(elem)
        else:
            val = raw_val
        return val

    @staticmethod
    def _process_set_value(raw_obj, _val, attr_name, check_read_only=True):
        """
        Process an incoming attribute value.

        Args:fdef _init
            raw_obj: the object the value is being assigned to
            _val: the value being assigned
            attr_name: the name of the attribute being assigned
            check_read_only: enforce the checks of read-only objects. This makes sense to be disabled during object construction.

        Returns:
            The processed value
        """

        val = ProxyDataDescriptor.__recursive_strip(_val)

        new_val = None

        if not raw_obj._schema.hasAttribute(attr_name):
            raise GangaAttributeError("Cannot assign %s, as it is NOT an attribute in the schema for class: %s" % (attr_name, getName(obj))) 

        #logger.debug("__set__")
        item = raw_obj._schema[attr_name]
        if item['protected']:
            raise ProtectedAttributeError('"%s" attribute is protected and cannot be modified' % (attr_name,))
        if raw_obj._readonly():

            if not item.getProperties()['changable_at_resubmit']:
                raise ReadOnlyObjectError('object %s is read-only and attribute "%s" cannot be modified now' % (repr(addProxy(raw_obj)), attr_name))

        if check_read_only:
            # mechanism for locking of preparable attributes
            if item['preparable']:
                ## Does not modify val
                ProxyDataDescriptor.__preparable_set__(raw_obj, val, attr_name)

        # if we set is_prepared to None in the GPI, that should effectively
        # unprepare the application
        if attr_name == 'is_prepared':
            # Replace is_prepared on an application for another ShareDir object
            ## Does not modify val
            ProxyDataDescriptor.__prep_set__(raw_obj, val)

        # catch assignment of 'something'  to a preparable application
        if attr_name == 'application':
            ## Does not modify val
            ProxyDataDescriptor.__app_set__(raw_obj, val)

        # unwrap proxy
        if item.isA(ComponentItem):
            from .Filters import allComponentFilters
            cfilter = allComponentFilters[item['category']]
            stripper = lambda v: stripComponentObject(v, cfilter, item)
        else:
            stripper = None

        if item['sequence']:
            ## Does not modify val
            new_val = ProxyDataDescriptor.__sequence_set__(stripper, raw_obj, val, attr_name)
        else:
            if stripper is not None:
                ## Shouldn't modify val
                new_val = stripper(val)
            else:
                ## Does not modify val
                new_val = ProxyDataDescriptor._stripAttribute(raw_obj, val, attr_name)

        if new_val is None and val is not None:
            new_val = val

        final_val = None
        # apply attribute filter to component items
        if item.isA(ComponentItem):
            ## Does not modify val
            final_val = ProxyDataDescriptor._stripAttribute(raw_obj, new_val, attr_name)
        else:
            final_val = new_val

        if final_val is None and val is not None:
            final_val = val

        ## Does not modify val?
        ProxyDataDescriptor._check_type(raw_obj, final_val, attr_name)

        return final_val

    def __set__(self, obj, _val):
        # self is the attribute we're about to change
        # obj is the object we're about to make the change in
        # val is the value we're setting the attribute to.
        # item is the schema entry of the attribute we're about to change

        attr_name = getName(self)

        raw_obj = stripProxy(obj)

        final_val = ProxyDataDescriptor._process_set_value(raw_obj, _val, attr_name)

        setattr(raw_obj, attr_name, final_val)


def proxy_wrap(f):
    # type: (Callable) -> Callable
    """
    A decorator to strip the proxy from all incoming arguments
    (including ``self`` if it's a method) and add one to the return
    value.
    """

    @functools.wraps(f)
    def proxy_wrapped(*args, **kwargs):
        s_args = [stripProxy(a) for a in args]
        s_kwargs = dict((name, stripProxy(a)) for name, a in kwargs.items())
        try:
            r = f(*s_args, **s_kwargs)
        except KeyboardInterrupt:
            logger.error("Command was interrupted by a Ctrl+C event!")
            logger.error("This can lead to inconsistencies")
            return
        return addProxy(r)

    return proxy_wrapped


class ProxyMethodDescriptor(object):

    __slots__ = ('_name', '_internal_name', '__doc__')

    def __init__(self, name, internal_name):
        self._name = name
        self._internal_name = internal_name

    def __get__(self, obj, cls):
        try:
            if obj is None:
                method = getattr(stripProxy(cls), self._internal_name)
            else:
                method = getattr(stripProxy(obj), self._internal_name)
            return proxy_wrap(method)
        except Exception as err:
            logger.error("%s" % err)
            raise

##########################################################################

# helper to create a wrapper for an existing ganga object

_proxyClassDict={}

def addProxyClass(some_class):
    ## CANNOT USE THE ._name (hence getName) HERE DUE TO REQUIREMENTS OF THE OBJECT IN GPI BEING SANE!!!
    class_name = some_class.__name__
    if class_name not in _proxyClassDict:
        _proxyClassDict[class_name] = GPIProxyClassFactory(class_name, some_class)    
    setattr(some_class, proxyClass, _proxyClassDict[class_name])

def getProxyClass(some_class):
    class_name = some_class.__name__
    if not isclass(some_class):
        raise GangaException("Cannot perform getProxyClass on a non-class object: %s:: %s" % (class_name, some_class))
    if not issubclass(some_class, GangaObject):
        raise GangaException("Cannot perform getProxyClass on class which is not a subclass of GangaObject: %s:: %s" % (class_name, some_class))
    proxy_class = getattr(some_class, proxyClass, None)
    ## It's possible we ourselves have added a proxy to the base class which we're now inheriting here.
    ## To avoid giving a proxy from Dataset to LHCbDataset and equivalent we'll check against our list of already-found class names.
    if proxy_class is None or class_name not in _proxyClassDict:
        addProxyClass(some_class)
        proxy_class = getattr(some_class, proxyClass)
    return proxy_class

def GPIProxyObjectFactory(_obj):
    # type: (GangaObject) -> GPIProxyObject
    """
    This function _must_ be passed a raw GangaObject. Use :function:`addProxy` for a safe version

    Args:
        _obj (GangaObject): the object to wrap

    Returns:
        a proxy object
    """
    if hasattr(_obj, proxyObject):
        return getattr(_obj, proxyObject)
    if not isType(_obj, GangaObject):
        raise GangaException("%s is NOT a Proxyable object" % type(_obj))

    obj_class = _obj.__class__

    proxy_class = getProxyClass(obj_class)

    proxy_object = proxy_class(_proxy_impl_obj_to_wrap=_obj)

    setattr(_obj, proxyObject, proxy_object)

    return proxy_object

# this class serves only as a 'tag' for all generated GPI proxy classes
# so we can test with isinstance rather then relying on more generic but
# less user friendly checking of attribute x._impl


class GPIProxyObject(object):
    __slots__ = list()
    pass

# create a new GPI class for a given ganga (plugin) class


def GPIProxyClassFactory(name, pluginclass):
    # type: (str, type(GangaObject)) -> type(GPIProxyObject)
    """
    Args:
        name: the name of the proxy class
        pluginclass: the ``GangaObject`` subclass to wrap

    Returns:
        a new type which wraps ``pluginclass``
    """

    def helptext(f, s):
        if name == '' or name is None:
            _name = ' '
        else:
            _name = name
        f.__doc__ = s % {'classname': _name, 'objname': _name.lower(), 'shortvarname': _name[0].lower()}

    # construct the class on-the-fly using the functions below as methods for
    # the new class

    def _init(self, *args, **kwds):

        ## Zero-th fully initialize self before moving on
        GPIProxyObject.__init__(self)

        ## THE ORDER IN HOW AN OBJECT IS INITIALIZED IS IMPORTANT AND HAS BEEN DOUBLE CHECKED - rcurrie


        ## If we're only constructing a raw Proxy to wrap an existing object lets wrap that and return
        proxy_obj_str = '_proxy_impl_obj_to_wrap'

        if proxy_obj_str in kwds:
            instance = kwds[proxy_obj_str]
            ## Even if we're wrapping something such as here make sure we set all of the proxy related attributes correctly.
            ## Setting of these attributes shold be done here within this class and should probably be properly be done on proxy construction. aka. here
        else:
            ## FIRST INITALIZE A RAW OBJECT INSTANCE CORRESPONDING TO 'pluginclass'
            ## Object was not passed by construction so need to construct new object for internal use
            # Case 1 j = Job(myExistingJob)            # We want to perform a deepcopy
            arg_len = len(args)
            if arg_len == 1 and isinstance(args[0], pluginclass):
                instance = deepcopy(stripProxy(args[0]))
            # Case 2 file_ = LocalFile('myFile.txt')   # We need to pass the (stripped) arguments to the constructor only if the 
            # Remember self = 1
            # For the moment we're warning the user until it's clear this is a safe thing to do, aka once all classes are deemed safe
            # The args will simply be passed through regardless
            elif arg_len == 0:
                instance = pluginclass.getNew(should_init=True)
            elif arg_len < len(getfullargspec(pluginclass.__init__)[0]):
                clean_args = (stripProxy(arg) for arg in args)
                instance = pluginclass(*clean_args)
            else:
                # In the future we will just pass the args to the classes directly and throw excepions, but for now we're trying to maintain old behavior
                logger.warning("Cannot use arguments: '%s' for constructing class type '%s'. Ignoring." % (args, getName(pluginclass)))
                logger.warning("Please contact the Ganga developers if you believe this is an error!")
                instance = pluginclass()

        ## Avoid intercepting any of the setter method associated with the implRef as they could trigger loading from disk
        ## These are protected objects in the setter and it will throw an exception if they're altered
        setattr(self, implRef, instance)
        instance.__dict__[proxyObject] = self

        ## Need to avoid any setter methods for GangaObjects
        ## Would be very nice to remove this entirely as I'm not sure a GangaObject should worry about it's proxy (if any)


        ## SECOND WE NEED TO MAKE SURE THAT OBJECT ID IS CORRECT AND THIS DOES THINGS LIKE REGISTER A JOB WITH THE REPO

        if proxy_obj_str in kwds:
            #instance._auto__init__()
            # wrapping not constructing so can exit after determining that the proxy attributes are setup correctly
            return

        from GangaCore.GPIDev.Base.Objects import do_not_copy
        ## All objects with an _auto__init__ method need to have that method called and we set the various node attributes here based upon the schema
        for key, _val in instance._schema.allItems():
            if not _val['getter'] and key not in instance._data:
                val = instance._schema.getDefaultValue(key)
                if isinstance(val, GangaObject):
                    val._auto__init__()
                instance.setSchemaAttribute(key, instance._attribute_filter__set__(key, val))

        instance._auto__init__()

        ## THIRD ALLOW FOR APPLICATION AND IS_PREPARED etc TO TRIGGER RELAVENT CODE AND SET THE KEYWORDS FROM THE SCHEMA AGAIN
        ## THIS IS MAINLY FOR THE FIRST EXAMPLE ABOVE

        ## THIS CORRECTLY APPLIES A PROXY TO ALL OBJECT ATTRIBUTES OF AN OBJECT CREATED WITHIN THE GPI

        # initialize all properties from keywords of the constructor
        for k in kwds:
            if instance._schema.hasAttribute(k):
                # This calls the same logic when assigning a named attribute as when we're assigning it to the object
                # There is logic here which we 'could' duplicate but it is over 100 lines of code which then is duplicating funtionality written elsewhere
                try:
                    val = ProxyDataDescriptor._process_set_value(instance, kwds[k], k, False)
                except Exception as err:
                    logger.warning('Error assigning following value to attribute: \'%s\'' % k)
                    logger.warning('value: \'%s\'' % str(kwds[k]))
                    logger.warning('Error: \'%s\'' % str(err))
                    raise GangaValueError('Error constructing object of type: \'%s\'' % getName(instance))
                if isinstance(val, GangaObject):
                    val._auto__init__()
                setattr(instance, k, val)
            else:
                logger.warning('keyword argument in the %s constructor ignored: %s=%s (not defined in the schema)', name, k, kwds[k])

        ## end of _init
        return

    from GangaCore.Utility.strings import ItemizedTextParagraph

    itbuf = ItemizedTextParagraph('Properties:', 80, ' ', '')

    for n, item in pluginclass._schema.allItems():
        if not item['hidden']:
            itbuf.addLine(n, item.describe())

    if not pluginclass.__doc__:
        pluginclass.__doc__ = 'Documentation missing.'

    pluginclass.__doc__.strip()
    pluginclass.__doc__ += "\n\n"

    publicdoc = pluginclass.__doc__ + itbuf.getString()

    helptext(_init, """GPI %(classname)s object constructor:
    %(classname)s() : create %(objname)s with default settings;
    %(classname)s(%(shortvarname)s) : make a copy of %(shortvarname)s;
    %(classname)s(%(shortvarname)s,x=a,...): make a copy of %(shortvarname)s and set property 'x' to a, etc..
    """)

    def _str(self, interactive=False):
        import io
        sio = io.StringIO()
        stripProxy(self).printSummaryTree(0, 0, '', out=sio, interactive=interactive)
        returnable = str(sio.getvalue()).rstrip()
        return returnable
    helptext(_str, """Return a printable string representing %(classname)s object as a tree of properties.""")

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('proxy object...')
            return

        p_text = ""
        try:

            if hasattr(self, implRef):
                raw_self = stripProxy(self)
                if hasattr(raw_self, '_repr_pretty_'):
                    raw_self._repr_pretty_(p, cycle)
                elif hasattr(raw_self, '_display'):
                    p_text = raw_self._display()
                else:
                    p_text = self.__str__(True)
            else:
                p_text = self.__str__(True)
        except Exception as err:
            p_text = "Error Representing object: %s\nErr:\n%s" % (type(self), err)

        p.text(p_text)

    helptext(_repr_pretty_, """Return a nice string to be printed in the IPython termial""")

    def _repr(self):
        try:
            has_proxy = hasattr(self, implRef)
            if has_proxy:
                raw_proxy = stripProxy(self)
            else:
                raw_proxy = None
            if has_proxy and hasattr(raw_proxy, '_repr'):
                return raw_proxy._repr()
            else:
                return '<' + repr(stripProxy(self)) + ' PROXY at ' + hex(abs(id(self))) + '>'
        except Exception as err:
            return "Error Representing object: %s\nErr:\n" % (type(self), err)

    helptext(_repr, "Return an short representation of %(classname)s object.")

    def _eq(self, x):
        result = False
        if isType(x, GPIProxyObject) or hasattr(x, implRef):
            result = stripProxy(self).__eq__(stripProxy(x))
        else:
            result = stripProxy(self).__eq__(x)
        return result
    helptext(_eq, "Equality operator (==), compare the %(classname)s properties which are declared as [comparable].")

    def _ne(self, x):
        result = True
        if isType(x, GPIProxyObject) or hasattr(x, implRef):
            result = stripProxy(self).__ne__(stripProxy(x))
        else:
            result = stripProxy(self).__ne__(x)
        return result
    helptext(_ne, "Non-equality operator (!=).")

    def _copy(self, unprepare=None):
        logger.debug('unprepare is %s', unprepare)
        if unprepare is None:
            if prepconfig['unprepare_on_copy'] is True:
                if hasattr(self, 'is_prepared') or hasattr(self, 'application'):
                    unprepare = True

        def _getSharedPath():
            Config_conf = getConfig('Configuration')
            return os.path.join(expandfilename(Config_conf['gangadir']), 'shared', Config_conf['user'])

        if hasattr(self, 'application'):
            if hasattr(self.application, 'is_prepared'):
                from GangaCore.Utility.files import expandfilename
                if self.application.is_prepared not in [None, True]:
                    if hasattr(self.application.is_prepared, 'name'):
                        shared_path = _getSharedPath()
                        if os.path.isdir(os.path.join(shared_path, self.application.is_prepared.name)):
                            from GangaCore.Core.GangaRepository import getRegistry
                            shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
                            logger.debug('increasing counter from proxy.py')
                            shareref.increase(self.application.is_prepared)
                            logger.debug('Found ShareDir directory: %s' % self.application.is_prepared.name)
                elif self.application.is_prepared not in [None, True]:
                    shared_path = _getSharedPath()
                    if not os.path.isdir(os.path.join(shared_path, self.application.is_prepared.name)):
                        logger.error('ShareDir directory not found: %s' % self.application.is_prepared.name)
                        logger.error('Unpreparing Job #%s' % self.id)
                        from GangaCore.Core.GangaRepository import getRegistry
                        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
                        shareref.increase(self.application.is_prepared)
                        self.unprepare()

        if unprepare is True:
            if hasattr(self, 'is_prepared'):
                from GangaCore.Utility.files import expandfilename
                if self.is_prepared not in [None, True]:
                    if hasattr(self.is_prepared, 'name'):
                        shared_path = _getSharedPath()
                        if not os.path.isdir(os.path.join(shared_path, self.is_prepared.name)):
                            logger.error('ShareDir directory not found: %s' % self.is_prepared.name)
                            logger.error('Unpreparing %s application' % getName(self))
                            self.unprepare()

            c = stripProxy(self).clone()
            if hasattr(c, 'is_prepared') and c._getRegistry() is None:
                from GangaCore.Core.GangaRepository import getRegistry
                shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
                shareref.increase(self.is_prepared)
            stripProxy(c)._auto__init__(unprepare=True)
        else:
            c = stripProxy(self).clone()
            stripProxy(c)._auto__init__()

        return addProxy(c)

    helptext(_copy, "Make an identical copy of self.")

    def _setattr(self, x, v):
        'This is the setter method in the Proxied Objects'
        #logger.debug("_setattr")
        # need to know about the types that require metadata attribute checking
        # this allows derived types to get same behaviour for free.
        raw_self = stripProxy(self)
        p_Ref = raw_self
        if p_Ref is not None:
            if not isclass(p_Ref):
                class_type = type(p_Ref)
            else:
                class_type = p_Ref
        else:
            class_type = p_Ref

        if x == implRef and not isinstance(v, class_type):
            raise AttributeError("Internal implementation object '%s' cannot be reassigned" % implRef)

        elif not raw_self._schema.hasAttribute(x):
            from GangaCore.GPIDev.Lib.Job.MetadataDict import MetadataDict
            if hasattr(raw_self, 'metadata') and isType(raw_self.metadata, MetadataDict):
                if x in raw_self.metadata.data:
                    raise GangaAttributeError("Metadata item '%s' cannot be modified" % x)

            if x != implRef:
                raise GangaAttributeError("Can't assign '%s' as it does NOT appear in the object schema for class '%s'" % (x, getName(self)))

        new_v = stripProxy(runtimeEvalString(self, x, v))
        GPIProxyObject.__setattr__(self, x, new_v)


    helptext(_setattr, """Set a property of %(classname)s with consistency and safety checks.
Setting a [protected] or a unexisting property raises AttributeError.""")

    #    def _getattr(self, name):
#        if name == '_impl': return self._impl
#        if '_attribute_filter__get__' in dir(self._impl):
#            return self._impl._attribute_filter__get__(name)
#        return self.name
#        ## need to know about the types that require metadata attribute checking
#        ## this allows derived types to get same behaviour for free.
#        from GangaCore.GPIDev.Lib.Job.Job import Job
#        from GangaCore.GPIDev.Lib.Tasks.Task import Task
#        from GangaCore.GPIDev.Lib.Tasks.Transform import Transform
#        metadata_objects=[Job]
#        if True in (isType(self,t) for t in metadata_objects):
#            try:
#                return self.metadata[name]
#            except:
#                return object.__getattribute__(self,name)
#        return object.__getattribute__(self,name)

    def _getattribute(self, name):

        #logger.debug("_getattribute: %s" % name)

        if name.startswith('__') or name == implRef:
            return GPIProxyObject.__getattribute__(self, name)
        else:
            implInstance = stripProxy(self)

            if '_attribute_filter__get__' in dir(implInstance) and \
                    not isType(implInstance, ObjectMetaclass) and \
                    implInstance._schema.hasItem(name) and \
                    not implInstance._schema.getItem(name)['hidden']:
                        returnable = addProxy(implInstance._attribute_filter__get__(name))
            else:
                try:
                    returnable = GPIProxyObject.__getattribute__(self, name)
                except AttributeError:
                    raise GangaAttributeError("Object '%s' does not have attribute: '%s'" % (getName(self), name))

        if isType(returnable, GangaObject):
            return addProxy(returnable)
        else:
            return returnable

    # but at the class level _impl is a ganga plugin class
    d = {implRef: pluginclass,
            '__init__': _init,
            '__str__': _str,
            '__repr__': _repr,
            '_repr_pretty_': _repr_pretty_,
            '__eq__': _eq,
            '__ne__': _ne,
            'copy': _copy,
            '__doc__': publicdoc,
            '__setattr__': _setattr,
         #          '__getattr__': _getattr,
            '__getattribute__': _getattribute,
         }

    if not hasattr(pluginclass, '_exportmethods'):
        pluginclass._exportmethods = []

    exported_methods = pluginclass._exportmethods

    # export public methods of this class and also of all the bases
    # this class is scanned last to extract the most up-to-date docstring
    exported_dicts = (b.__dict__ for b in reversed(pluginclass.__mro__))

    for subclass_dict in exported_dicts:
        for k in subclass_dict:
            if getattr(subclass_dict[k], 'exported', False):
                exported_methods.append(k)  # Add all @export'd methods
            if k in exported_methods:

                # Should we expose the object/method directly or a custom '_export_' wrapper?
                internal_name = "_export_" + k
                if internal_name not in subclass_dict:
                    internal_name = k

                try:
                    method = subclass_dict[internal_name]
                except KeyError as err:
                    logger.debug("ObjectMetaClass Error internal_name: %s,\t d: %s" % (internal_name, d))
                    logger.debug("ObjectMetaClass Error: %s" % err)
                    raise

                # If this is a method make sure we wrap the method so it's not exposed to GPI objects
                if not isinstance(method, types.FunctionType):
                    continue

                # Wrap the method so that arguments are stripped before the method runs and a proxied object is returned
                f = ProxyMethodDescriptor(k, internal_name)
                f.__doc__ = method.__doc__
                d[k] = f

    # export visible properties... do not export hidden properties
    for attr, item in pluginclass._schema.allItems():
        if not item['hidden']:
            d[attr] = ProxyDataDescriptor(attr)

    return type(name, (GPIProxyObject,), d)


#
#
# $Log: not supported by cvs2svn $
# Revision 1.2.4.2  2009/07/08 15:39:15  ebke
# removed object_filter__get__
#
# Revision 1.2.4.1  2009/07/08 11:18:21  ebke
# Initial commit of all - mostly small - modifications due to the new GangaRepository.
# No interface visible to the user is changed
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
# Revision 1.32.4.15  2008/07/02 13:19:35  moscicki
# TODO comment
#
# Revision 1.32.4.14  2008/06/04 10:34:54  moscicki
# fixed typecheck function naming
#
# Revision 1.32.4.13  2008/06/02 11:42:12  moscicki
# fixed typo...
#
# Revision 1.32.4.12  2008/05/27 18:02:50  kuba
# fixed bug #36836, .ganga.log in afs home directory
#
# Revision 1.32.4.11  2008/02/29 09:16:17  moscicki
# fix from Will Reece
#
# Revision 1.32.4.10  2008/02/28 15:45:30  moscicki
# fixed GangaList typechecking problem
#
# Revision 1.32.4.9  2008/02/06 16:26:30  moscicki
# typelist == None => disable type checking
# extra warnings for incomplete typesystem information
#
# Revision 1.32.4.8  2008/02/06 09:48:44  wreece
# Allows the proxy stripper a NoOp if the object has already been stripped.
#
# Revision 1.32.4.7  2008/02/06 09:28:48  wreece
# First pass at a cleanup of the gangalist stuff. I've made changes so the diffs with the 4.4 series are more transparent. Currently still test failures.
#
# Revision 1.32.4.6  2007/12/18 16:40:39  moscicki
# bugfix
#
# Revision 1.32.4.5  2007/12/18 09:05:03  moscicki
# integrated typesystem from Alvin and made more uniform error reporting
#
# Revision 1.32.4.4  2007/11/20 14:29:49  wreece
# Corrects typo in TFile, and removes extra methods from Proxy. Typo was causing tests to fail.
#
# Revision 1.32.4.3  2007/11/14 13:03:54  wreece
# Changes to make shortcuts work correctly with gangalists. all but one tests should now pass.
#
# Revision 1.32.4.2  2007/11/07 17:02:10  moscicki
# merged against Ganga-4-4-0-dev-branch-kuba-slices with a lot of manual merging
#
# Revision 1.32.4.1  2007/11/07 15:10:02  moscicki
# merged in pretty print and GangaList support from ganga-5-dev-branch-4-4-1-will-print branch
#
# Revision 1.32.8.2  2007/10/30 14:30:23  wreece
# Non-working update. Adds in Kuba's exported methods dodge. It is now possible to define a _export_ version of a method for external use and a undecorated method for internal use.
#
# Revision 1.32.8.1  2007/10/30 12:12:08  wreece
# First version of the new print_summary functionality. Lots of changes, but some known limitations. Will address in next version.
#
# Revision 1.32  2007/07/10 13:08:29  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.31  2007/07/10 07:50:52  moscicki
# fixed #27541
#
# Revision 1.30.8.1  2007/06/18 10:16:34  moscicki
# slices prototype
#
# Revision 1.30  2007/03/07 09:24:34  moscicki
# AGAIN: fixed a problem of assigning iterable plain value (such as string) to a non-strict sequence (so x.files = "abc" was yelding 3 files "a","b","c"
#
# added GangaAttributeError exception
#
# Revision 1.29  2007/03/06 16:38:24  moscicki
# fixed a problem of assigning iterable plain value (such as string) to a non-strict sequence (so x.files = "abc" was yelding 3 files "a","b","c"
#
# Revision 1.28  2007/03/05 12:03:01  moscicki
# explicit switch for strict_sequence (default is True), if the sequence is non-strict then a single value v will be converted to [v] on assignment, for example non-strict File sequence yields obj.x = 'a' <=> obj.x = [File('a')]  <=> obj.x = File('a')
#
# Revision 1.27  2007/02/28 18:20:49  moscicki
# moved GangaException to GangaCore.Core
#
# Revision 1.26  2006/10/23 11:00:42  moscicki
# minor logger fixed (level changed)
#
# Revision 1.25  2006/09/15 14:19:44  moscicki
# Fixed bug #12229 overview: FutureWarning from Python
#
# Revision 1.24  2006/08/11 13:13:05  adim
# Added: GangaException as a markup base class for all exception that need to be printed in a usable way in IPython shell
#
# Revision 1.23  2005/12/02 15:28:46  moscicki
# customizable _repr() method on GPI objects
#
# Revision 1.22  2005/11/14 13:58:27  moscicki
# fixed a getter filter bug
#
# Revision 1.21  2005/11/14 10:30:37  moscicki
# enabled getter attribute filter
#
# Revision 1.20  2005/09/21 09:09:14  moscicki
# "better" repr text for proxies
#
# Revision 1.19  2005/08/26 13:21:47  moscicki
# do not document hidden properties
#
# Revision 1.18  2005/08/26 10:21:31  karl
# KH: Minor correction: linesep -> separator
#
# Revision 1.17  2005/08/26 09:54:55  moscicki
# minor changes
#
# Revision 1.16  2005/08/24 15:41:19  moscicki
# automatically generated help for properties, disabled the SchemaHelper and few other improvements to the help system
#
# Revision 1.15  2005/08/23 17:15:06  moscicki
# *** empty log message ***
#
#
#
