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
import inspect

import Ganga.GPIDev.Schema as Schema

from Ganga.GPIDev.Base.Proxy import isType, stripProxy, getName, addProxy, runtimeEvalString
from Ganga.Core.exceptions import GangaValueError, GangaException

from Ganga.Utility.Plugin import allPlugins


logger = Ganga.Utility.logging.getLogger(modulename=1)

class PreparedStateError(GangaException):

    def __init__(self, txt=''):
        GangaException.__init__(self, txt)
        self.txt = txt

    def __str__(self):
        return "PreparedStateError: %s" % str(self.txt)


class Node(object):
    _ref_list = ['_parent', '_registry', '_index_cache', '_proxyObject']

    def __init__(self, parent):
        self._data = {}
        self._parent = parent
        self._index_cache = None
        self._proxyObject = None
        super(Node, self).__init__()
        #logger.info("Node __init__")

    def __construct__(self, args):
        ## Don't obliterate the data stored in the node here
        ## Objects are initialized then '__construct__'-ed in the Proxy
        if not hasattr(self, '_data'):
            self._data = {}
        if not hasattr(self, '_parent'):
            self._setParent(None)
        if not hasattr(self, '_proxyObject'):
            self._proxyObject = None
        if not hasattr(self, '_index_cache'):
            self._index_cache = None

    def __getstate__(self):
        d = self.__dict__
        d['_data'] = d['_data'].copy()
        for r in self._ref_list:
            d[r] = None
        return d

    def __setstate__(self, this_dict):
        for key, val in this_dict['_data'].iteritems():
            if isType(val, Node) and key not in self._ref_list:
                val._setParent(self)

        for attr in self._ref_list:
            if not hasattr(self, attr):
                setattr(self, attr, None)

        for key, val in this_dict.iteritems():
            setattr(self, key, val)

    def __copy__(self, memo=None):
        cls = type(stripProxy(self))
        obj = super(cls, cls).__new__(cls)
        obj.__init__()
        # FIXME: this is different than for deepcopy... is this really correct?
        this_dict = self.__dict__.copy()
        for elem in this_dict.keys():
            if elem not in self._ref_list:
                this_dict[elem] = deepcopy(stripProxy(this_dict[elem]), memo)
            else:
                this_dict[elem] = None
        #obj.__setstate__(this_dict)
        obj.getParent(self._getParent())
        setattr(obj, '_index_cache', None)
        setattr(obj, '_registry', self._registry)
        return obj

    def __deepcopy__(self, memo=None):
        cls = type(self)
        obj = super(cls, cls).__new__(cls)
        obj.__init__()
        this_dict = stripProxy(self).__getstate__()
        for elem in this_dict.keys():
            if elem not in self._ref_list:
                this_dict[elem] = deepcopy(stripProxy(this_dict[elem]), memo)  # FIXED
            else:
                this_dict[elem] = None
        #obj.__setstate__(this_dict)

        if self._getParent() is not None:
            obj._setParent(self._getParent())
        setattr(obj, '_index_cache', None)
        setattr(obj, '_registry', self._registry)
        return obj

    def _getParent(self):
        if not hasattr(self, '_parent'):
            self._setParent(None)
        return self._parent

    def _setParent(self, parent):
        #if parent is None:
        #    import traceback
        #    traceback.print_stack()
        #    logger.error("Setting NONE Parent!!!")
        setattr(self, '_parent', parent)

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
            obj = stripProxy(obj)._getParent()
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

    def __copy__(self):
        copied_obj = self.clone()

    @staticmethod
    def __resetParent(class_obj):
        ## Fix some objects losing parent knowledge
        src_dict = class_obj.__dict__
        for key, val in src_dict.iteritems():
            this_attr = getattr(class_obj, key)
            if isType(this_attr, Node) and key not in Node._ref_list:
                #logger.debug("k: %s  Parent: %s" % (str(key), (stripProxy(srcobj))))
                stripProxy(this_attr)._setParent(stripProxy(class_obj))

    # clone self and return a properly initialized object
    def clone(self):
        new_obj = deepcopy(self)
        #new_obj.__setstate__(self.__getstate__())

        self.__resetParent(new_obj)
        self.__resetParent(self)

        return new_obj

    # copy all the properties recursively from the srcobj
    # if schema of self and srcobj are not compatible raises a ValueError
    # ON FAILURE LEAVES SELF IN INCONSISTENT STATE
    def copyFrom(self, srcobj, _ignore_atts=None):

        if _ignore_atts is None:
            _ignore_atts = []
        _srcobj = stripProxy(srcobj)
        # Check if this object is derived from the source object, then the copy
        # will not throw away information

        if not hasattr(_srcobj, '__class__') and not inspect.isclass(_srcobj.__class__):
            raise GangaValueError("Can't copyFrom a non-class object: %s isclass: %s" % (str(_srcobj), str(inspect.isclass(_srcobj))))

        if not isType(self, _srcobj.__class__) and not isType(_srcobj, self.__class__):
            raise GangaValueError("copyFrom: Cannot copy from %s to %s!" % (getName(_srcobj), getName(self)))

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
            if isType(this_attr, Node) and key not in Node._ref_list:
                #logger.debug("k: %s  Parent: %s" % (str(key), (stripProxy(srcobj))))
                stripProxy(this_attr)._setParent(stripProxy(srcobj))

    def _actually_copyFrom(self, _srcobj, _ignore_atts):

        for name, item in stripProxy(self)._schema.allItems():
            if name in _ignore_atts:
                continue

            #logger.debug("Copying: %s : %s" % (str(name), str(item)))
            if name == 'application' and hasattr(_srcobj.application, 'is_prepared'):
                _app = _srcobj.application
                if _app.is_prepared not in [None, True]:
                    _app.incrementShareCounter(_app.is_prepared.name)

            if not self._schema.hasAttribute(name):
                #raise ValueError('copyFrom: incompatible schema: source=%s destination=%s'%(getName(_srcobj),getName(self)))
                setattr(self, name, self._schema.getDefaultValue(name))
                this_attr = getattr(self, name)
                if isType(this_attr, Node) and name not in Node._ref_list:
                    this_attr._setParent(self)
            elif not item['copyable']: ## Default of '1' instead of True...
                setattr(self, name, self._schema.getDefaultValue(name))
                this_attr = getattr(self, name)
                if isType(this_attr, Node) and name not in Node._ref_list:
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

    def printSummaryTree(self, level=0, verbosity_level=0, whitespace_marker='', out=None, selection=''):
        """If this method is overridden, the following should be noted:

        level: the hierachy level we are currently at in the object tree.
        verbosity_level: How verbose the print should be. Currently this is always 0.
        whitespace_marker: If printing on multiple lines, this allows the default indentation to be replicated.
                           The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
        out: An output stream to print to. The last line of output should be printed without a newline.'
        selection: See VPrinter for an explaintion of this.
        """
        from Ganga.GPIDev.Base.VPrinter import VSummaryPrinter
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
        return not self.__eq__(node)

    def getNodeData(self):
        if not hasattr(self, '_data') or self._data is None:
            setattr(self, '_data', {})
        return self._data

    def setNodeData(self, new_data):
        self._data = new_data

    def getNodeAttribute(self, attrib_name):
        return self.getNodeData()[attrib_name]

    def setNodeAttribute(self, attrib_name, attrib_value):
        self.getNodeData()[attrib_name] = attrib_value

    def removeNodeAttribute(self, attrib_name):
        if attrib_name in self._data:
            del self._data[attrib_name]

    def setNodeIndexCache(self, new_index_cache):
        setattr(self, '_index_cache', new_index_cache)

    def getNodeIndexCache(self):
        if hasattr(self, '_index_cache'):
            return self._index_cache
        else:
            #logger.debug("Assigning dummy '_index_cache'!")
            self.setNodeIndexCache({})
            return self._index_cache

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

    @staticmethod
    def _bind_method(obj, name):
        if name is None:
            return None
        return getattr(obj, name)

    def _check_getter(self):
        if self._getter_name:
            raise AttributeError('cannot modify or delete "%s" property (declared as "getter")' % getName(self))

    def __get__(self, obj, cls):
        if obj is None:
            return cls._schema[getName(self)]
        else:
            result = None
            getter = self._bind_method(obj, self._getter_name)
            if getter:
                result = getter()
            else:

                # LAZYLOADING
                lookup_result = None

                lookup_exception = None

                try:
                    if stripProxy(obj).getNodeIndexCache() is not None:
                        obj_index = stripProxy(obj).getNodeIndexCache()
                        if getName(self) in obj_index.keys():
                            lookup_result = obj_index[getName(self)]
                except Exception as err:
                    #import traceback
                    #traceback.print_stack()
                    logger.debug("Lazy Loading Exception: %s" % str(err))
                    lookup_exception = err
                    #raise err

                ## ._data takes priority ALWAYS over ._index_cache
                try:
                    if stripProxy(obj).getNodeData() is not None:
                        obj_data = stripProxy(obj).getNodeData()
                        if getName(self) in obj_data.keys():
                            lookup_result = obj_data[getName(self)]
                except Exception as err:
                    logger.debug("Object Data Exception: %s" % str(err))
                    lookup_exception = err


                if stripProxy(obj).getNodeData() or stripProxy(obj).getNodeIndexCache():
                    _obj = stripProxy(obj)
                    if ((_obj.getNodeData() is not None) or (_obj.getNodeIndexCache() is not None)) and (lookup_exception is not None):
                        result = lookup_result
                    else:
                        if getName(self) in _obj.getNodeData().keys():
                            result = _obj.getNodeAttribute(getName(self))
                        else:
                            from Ganga.GPIDev.Base.Proxy import isProxy
                            if isProxy(_obj.getNodeData()):
                                if getName(self) in stripProxy(self.getData()):
                                    result = stripProxy(_obj.getData())[getName(self)]
                                else:
                                    ##THIS TRIGGERS THE LOADING OF THE JOB FROM DISK!!!
                                    _obj._getReadAccess()
                                    logger.debug("1) Error, cannot find '%s' parameter in: %s" % (getName(self), getName(obj)))
                                    GangaException("Error, cannot find '%s' parameter in: %s" % (getName(self), getName(obj)))
                                    result = _obj.getNodeAttribute(getName(self))
                            else:
                                ##THIS TRIGGERS THE LOADING OF THE JOB FROM DISK!!!
                                _obj._getReadAccess()
                                logger.debug("2) Error, cannot find '%s' parameter in: %s" % (getName(self), getName(obj)))
                                GangaException("Error, cannot find '%s' parameter in: %s" % (getName(self), getName(obj)))
                                result = _obj.getNodeAttribute(getName(self))
                else:
                    if lookup_exception is not None:
                        err = lookup_exception
                    else:
                        err = GangaException("Error finding parameter '%s' in object: %s" % (getName(self), getName(obj)))
                    raise err

            return result

    def __cloneVal(self, v, obj):

        item = obj._schema[getName(self)]

        if v is None:
            if item.hasProperty('category'):
                assertion = item['optional'] and (item['category'] != 'internal')
            else:
                assertion = item['optional']
            #assert(assertion)
            if assertion is False:
                logger.warning("Item: '%s'. of class type: '%s'. Has a Default value of 'None' but is NOT optional!!!" % (getName(self), type(obj)))
                logger.warning("Please contact the developers and make sure this is updated!")
            return None
        elif isinstance(v, str):
            return str(v)
        elif isinstance(v, int):
            return int(v)
        elif isinstance(v, dict):
            new_dict = {}
            for key, item in new_dict.iteritems():
                new_dict[key] = self.__cloneVal(val, obj)
            return new_dict
        else:
            if not isType(v, Node) and isType(v, (list, tuple)):
                try:
                    from Ganga.GPI import GangaList
                    new_v = GangaList()
                except ImportError:
                    new_v = []
                for elem in v:
                    new_v.append(self.__cloneVal(elem, obj))
                #return new_v
            elif not isType(v, Node):
                if inspect.isclass(v):
                    new_v = v()
                if not isType(new_v, Node):
                    logger.error("v: %s" % str(v))
                    raise GangaException("Error: found Object: %s of type: %s expected an object inheriting from Node!" % (str(v), str(type(v))))
                else:
                    new_v = self.__copyNodeObject(new_v, obj)
            else:
                new_v = self.__copyNodeObject(v, obj)

            if isType(new_v, Node):
                #logger.debug("Seeting Parent: %s" % stripProxy(obj))
                stripProxy(new_v)._setParent(stripProxy(obj))
            return new_v

    def __copyNodeObject(self, v, obj):

        item = obj._schema[getName(self)]

        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
        if isType(v, GangaList):
            categories = v.getCategory()
            len_cat = len(categories)
            if (len_cat > 1) or ((len_cat == 1) and (categories[0] != item['category'])) and item['category'] != 'internal':
                # we pass on empty lists, as the catagory is yet to be defined
                from Ganga.GPIDev.Base.Proxy import GangaAttributeError
                raise GangaAttributeError('%s: attempt to assign a list containing incompatible objects %s to the property in category "%s"' % (getName(self), v, item['category']))
        else:
            if stripProxy(v)._category != item['category'] and item['category'] != 'internal':
                from Ganga.GPIDev.Base.Proxy import GangaAttributeError
                raise GangaAttributeError('%s: attempt to assign an incompatible object %s to the property in category "%s"' % (getName(self), v, item['category']))

 
        v_copy = stripProxy(v).clone()

        #logger.info("Cloned Object Parent: %s" % v_copy._getParent())
        #logger.info("Original: %s" % v_copy._getParent())

        return v_copy

    def __set__(self, _obj, _val):
        ## self: attribute being changed or Ganga.GPIDev.Base.Objects.Descriptor in which case getName(self) gives the name of the attribute being changed
        ## _obj: parent class which 'owns' the attribute
        ## _val: value of the attribute which we're about to set

        if getName(self) in ['_parent', '_proxyObject', '_impl', '_proxyClass']:
            object.__setattr__(_obj, getName(self), _val)
            return

        self_reg = None
        self_prevState = None
        if hasattr(stripProxy(self), '_getRegistry'):
            self_reg = stripProxy(stripProxy(self)._getRegistry())
            if self_reg is not None and hasattr(self_reg, 'isAutoFlushEnabled'):
                self_prevState = self_reg.isAutoFlushEnabled()
                if self_prevState is True and hasattr(self_reg, 'turnOffAutoFlushing'):
                    self_reg.turnOffAutoFlushing()

        obj_reg = None
        obj_prevState = None
        if isType(stripProxy(_obj), GangaObject) and hasattr(stripProxy(_obj), '_getRegistry'):
            obj_reg = stripProxy(stripProxy(_obj)._getRegistry())
            if obj_reg is not None and hasattr(obj_reg, 'isAutoFlushEnabled'):
                obj_prevState = obj_reg.isAutoFlushEnabled()
                if obj_prevState is True and hasattr(obj_reg, 'turnOffAutoFlushing'):
                    obj_reg.turnOffAutoFlushing()

        val_reg = None
        val_prevState = None
        if isType(stripProxy(_val), GangaObject) and hasattr(stripProxy(_val), '_getRegistry'):
            val_reg = stripProxy(stripProxy(_val)._getRegistry())
            if val_reg is not None and hasattr(val_reg, 'isAutoFlushEnabled'):
                val_prevState = val_reg.isAutoFlushEnabled()
                if val_prevState is True and hasattr(val_reg, 'turnOffAutoFlushing'):
                    val_reg.turnOffAutoFlushing()

        new_val = runtimeEvalString(_obj, getName(self), _val)

        self.__atomic_set__(_obj, new_val)

        set_obj = getattr(stripProxy(_obj), getName(self))

        if isType(set_obj, Node):
            stripProxy(set_obj)._setParent(stripProxy(_obj))
            stripProxy(set_obj)._setDirty()
        if isType(new_val, Node):
            stripProxy(_val)._setDirty()

        if val_reg is not None:
            if val_prevState is True and hasattr(val_reg, 'turnOnAutoFlushing'):
                val_reg.turnOnAutoFlushing()

        if obj_reg is not None:
            if obj_prevState is True and hasattr(obj_reg, 'turnOnAutoFlushing'):
                obj_reg.turnOnAutoFlushing()

        if self_reg is not None:
            if self_prevState is True and hasattr(self_ref, 'turnOnAutoFlushing'):
                self_reg.turnOnAutoFlushing()

    def __atomic_set__(self, _obj, _val):
        ## self: attribute being changed or Ganga.GPIDev.Base.Objects.Descriptor in which case getName(self) gives the name of the attribute being changed
        ## _obj: parent class which 'owns' the attribute
        ## _val: value of the attribute which we're about to set

        #if hasattr(_obj, getName(self)):
        #    if not isType(getattr(_obj, getName(self)), GangaObject):
        #        if type( getattr(_obj, getName(self)) ) == type(_val):
        #            object.__setattr__(_obj, getName(self), deepcopy(_val))
        #            return
#
#        if not isType(_obj, GangaObject) and type(_obj) == type(_val):
#            _obj = deepcopy(_val)
#            return

        obj = stripProxy(_obj)
        temp_val = stripProxy(_val)

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

        item = stripProxy(obj._schema[getName(self)])

        def cloneVal(v):
            from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
            if isType(v, (list, tuple, GangaList)):
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
                from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
                new_val = GangaList()
            else:
                if isType(item, Schema.ComponentItem):
                    new_val = makeGangaList(val, cloneVal, parent=obj, preparable=_preparable)
                else:
                    new_val = makeGangaList(val, parent=obj, preparable=_preparable)
        else:
            ## Else we need to work out what we've got.
            if isType(item, Schema.ComponentItem):
                from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
                if isType(val, (list, tuple, GangaList)):
                    ## Can't have a GangaList inside a GangaList easily so lets not
                    if isType(_obj, GangaList):
                        newListObj = []
                    else:
                        newListObj = GangaList()

                    self.__createNewList(newListObj, val, cloneVal)
                    #for elem in val:
                    #    newListObj.append(cloneVal(elem))
                    new_val = newListObj
                else:
                    new_val = cloneVal(val)
            else:
                new_val = val
                pass
                #val = deepcopy(val)

        if isType(new_val, Node):
            new_val._setParent(obj)

        stripProxy(obj).setNodeAttribute(getName(self), new_val)
        obj.__dict__[getName(self)] = new_val

        obj._setDirty()

    def __delete__(self, obj):
        obj.removeNodeAttribute(getName(self))

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

        try:
            from Ganga.GPI import queues
            linearize = False
        except ImportError:
            linearize = True


        try:
            import threading
        except ImportError:
            linearize = True

        if linearize is True or len(input_elements) < 20 or\
                not isinstance(threading.current_thread(), threading._MainThread):
            addToList(input_elements, final_list, action)
            return

        import math
        tenth = math.ceil(float(len(input_elements))/10.)

        for i in range(10):
            these_elements = input_elements[int(i*tenth):int((i+1)*tenth)]
            queues._monitoring_threadpool.add_function(addToList, (these_elements, final_list, action))

        while(len(final_list) != len(input_elements)):
            import time
            time.sleep(0.5)

        return

def export(method):
    """
    Decorate a GangaObject method to be exported to the GPI
    """
    method.exported_method = True
    return method


class ObjectMetaclass(type):
    _descriptor = Descriptor

    def __init__(cls, name, bases, this_dict):

        from Ganga.GPIDev.Base.Proxy import GPIProxyClassFactory, ProxyDataDescriptor, ProxyMethodDescriptor

        super(ObjectMetaclass, cls).__init__(name, bases, this_dict)

        # ignore the 'abstract' base class
        # FIXME: this mechanism should be based on explicit getName(cls) or alike
        #if name == 'GangaObject':
        #    return

        #logger.debug("Metaclass.__init__: class %s name %s bases %s", cls, name, bases)

        # all Ganga classes must have (even empty) schema
        if not hasattr(cls, '_schema') or cls._schema is None:
            cls._schema = Schema.Schema(None, None)

        this_schema = cls._schema

        # Add all class members of type `Schema.Item` to the _schema object
        # TODO: We _could_ add base class's Items here by going through `bases` as well.
        for member_name, member in this_dict.items():
            if isinstance(member, Schema.Item):
                this_schema.datadict[member_name] = member

        # produce a GPI class (proxy)
        proxyClass = GPIProxyClassFactory(name, cls)

        if not hasattr(cls, '_exportmethods'):
            cls._exportmethods = []

        this_export = cls._exportmethods

        # export public methods of this class and also of all the bases
        # this class is scanned last to extract the most up-to-date docstring
        dicts = (b.__dict__ for b in reversed(cls.__mro__))
        for d in dicts:
            for k in d:
                if k in this_export or getattr(d[k], 'exported_mes_thod', False):

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
        if '_schema' not in this_dict.keys():
            s = "Class %s must _schema (it cannot be silently inherited)" % (name,)
            logger.error(s)
            raise ValueError(s)

        # If a class has not specified a '_name' then default to using the class '__name__'
        if not cls.__dict__.get('_name'):
            cls._name = name

        if this_schema._pluginclass is not None:
            logger.warning('Possible schema clash in class %s between %s and %s', name, getName(cls), getName(this_schema._pluginclass))

        # export visible properties... do not export hidden properties
        for attr, item in this_schema.allItems():
            setattr(cls, attr, cls._descriptor(attr, item))
            if not item['hidden']:
                setattr(proxyClass, attr, ProxyDataDescriptor(attr))

        # additional check of type
        # bugfix #40220: Ensure that default values satisfy the declared types
        # in the schema
        for attr, item in this_schema.simpleItems():
            if not item['getter']:
                item._check_type(item['defvalue'], '.'.join([name, attr]), enableGangaList=False)

        # create reference in schema to the pluginclass
        this_schema._pluginclass = cls

        # store generated proxy class
        cls._proxyClass = proxyClass

        # register plugin class
        if hasattr(cls, '_declared_property'):
            # if we've not even declared this we don't want to use it!
            if not cls._declared_property('hidden') or cls._declared_property('enable_plugin'):
                allPlugins.add(cls, cls._category, getName(cls))

            # create a configuration unit for default values of object properties
            if not cls._declared_property('hidden') or cls._declared_property('enable_config'):
                this_schema.createDefaultConfig()



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
        #Node.__init__(self, None)

        if self._schema is not None and hasattr(self._schema, 'allItems'):
            for attr, item in self._schema.allItems():
                ## If an object is hidden behind a getter method we can't assign a parent or defvalue so don't bother - rcurrie
                if item.getProperties()['getter'] is None:
                    defVal = self._schema.getDefaultValue(attr)
                    setattr(self, attr, defVal)
                    new_attr = getattr(self, attr)
                    if isType(new_attr, Node):
                        new_attr._setParent(self)

        # Overwrite default values with any config values specified
        # self.setPropertiesFromConfig()

    # construct an object of this type from the arguments. Defaults to copy
    # constructor.
    def __construct__(self, args):
        # act as a copy constructor applying the object conversion at the same
        # time (if applicable)

        super(GangaObject, self).__construct__(args)

        if len(args) == 0:
            return
        elif len(args) == 1:
            self.copyFrom(args[0])
        else:
            from Ganga.GPIDev.Base.Proxy import TypeMismatchError
            raise TypeMismatchError("Constructor expected one or zero non-keyword arguments, got %i" % len(args))

    def __getstate__(self):
        # IMPORTANT: keep this in sync with the __init__
        #self._getReadAccess()
        this_dict = super(GangaObject, self).__getstate__()
        #this_dict['_proxyObject'] = None
        #this_dict['_dirty'] = False
        return this_dict

    def __setstate__(self, this_dict):
        #self._getWriteAccess()
        super(GangaObject, self).__setstate__(this_dict)
        #if '_parent' in this_dict:
        #    self._setParent(this_dict['_parent'])
        #self._setParent(None)
        #self._proxyObject = None
        self._dirty = False

    # on the deepcopy reset all non-copyable properties as defined in the
    # schema
    def __deepcopy__(self, memo=None):
        true_parent = self._getParent()
        self = stripProxy(self)
        ## This triggers a read of the job from disk
        self._getReadAccess()
        self_copy = super(GangaObject, self).__deepcopy__(memo)

        if self._schema is not None:
            for name, item in self._schema.allItems():
                if not item['copyable']:
                    setattr(self_copy, name, self._schema.getDefaultValue(name))
                    this_attr = getattr(self_copy, name)
                    if isType(this_attr, Node):
                        this_attr._setParent(self_copy)
                else:
                    setattr(self_copy, name, deepcopy(getattr(self, name)))


                if item.isA(Schema.SharedItem):

                    shared_dir = getattr(self_copy, name)

                    if hasattr(shared_dir, 'name'):

                        from Ganga.Core.GangaRepository import getRegistry
                        from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
                        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())

                        logger.debug("Increasing shareref")
                        shareref.increase(shared_dir.name)
        if true_parent is not None:
            self._setParent(true_parent)
            self_copy._setParent(true_parent)
        return self_copy

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
        root = stripProxy(self)._getRoot()
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
        root = stripProxy(self)._getRoot()
        reg = root._getRegistry()
        if reg is not None:
            logger.debug("Releasing: %s" % (reg.name))
            reg._release_lock(root)

    def _getReadAccess(self):
        """ makes sure the objects _data is there and the object itself has a recent state.
        Raise RepositoryError"""
        root = stripProxy(self)._getRoot()
        reg = root._getRegistry()
        if reg is not None:
            reg._read_access(root, self)

    # define when the object is read-only (for example a job is read-only in
    # the states other than new)
    def _readonly(self):
        r = stripProxy(self)._getRoot()
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
        r = stripProxy(self)._getRoot()
        if hasattr(r, '_registry'):
            return r._registry
        else:
            logger.debug("_getRegistry Exception: '_registry not found for object: %s" % str(r))
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
        r = stripProxy(self)._getRoot(cond=lambda o: isType(o, Job))
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
        if hasattr(v, '_on_attribute__set__'):
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
