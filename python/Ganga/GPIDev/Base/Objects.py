################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Objects.py,v 1.5.2.10 2009-07-24 13:35:53 ebke Exp $
################################################################################
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
logger = Ganga.Utility.logging.getLogger(modulename=1)

from Ganga.Utility.Plugin import allPlugins, PluginManagerError
from Ganga.Utility.Config import Config

import types
import copy

import Ganga.GPIDev.Schema as Schema

from Proxy import GPIProxyClassFactory, ProxyDataDescriptor, ProxyMethodDescriptor, GangaAttributeError, isType

from Ganga.Utility.logic import implies
from Ganga.Utility.util import canLoopOver, isStringLike

    
class Node(object):
    _parent = None
    _index_cache = None

    def __init__(self, parent):
        self._data= {}
        self._setParent(parent)
            
    def __getstate__(self):
        dict = self.__dict__.copy()
        dict['_data'] = dict['_data'].copy()
        dict['_parent'] = None
        dict['_registry'] = None
        dict['_index_cache'] = None
        return dict

    def __setstate__(self, dict):
        for n, v in dict['_data'].items():
            if isinstance(v,Node):
                v._setParent(self)
            if hasattr(v,"__iter__") and not hasattr(v,"iteritems"):
                # set the parent of the list or dictionary (or other iterable) items
                for i in v:
                    if isinstance(i,Node):
                        i._setParent(self)

        self.__dict__ = dict

    def __copy__(self, memo = None):
        cls = type(self)
        obj = super(cls, cls).__new__(cls)
        dict = self.__dict__.copy() #FIXME: this is different than for deepcopy... is this really correct?
        obj.__dict__ = dict
        return obj

    def __deepcopy__(self, memo = None):
        cls = type(self)
        obj = super(cls, cls).__new__(cls)
        dict = self.__getstate__()
        for n in dict:
            dict[n] = copy.deepcopy(dict[n],memo) # FIXED
        obj.__setstate__(dict)
        return obj

    def _getParent(self):
        return self._parent
        #if "_data" in self.__dict__ and not self._data is None:
        #    return self._data['parent']
        #return None

    def _setParent(self, parent):
        self._parent = parent
        #if not self._data is None:
        #    self._data['parent'] = parent

    # get the root of the object tree
    # if parent does not exist then the root is the 'self' object
    # cond is an optional function which may cut the search path: when it returns True, then the parent is returned as root
    def _getRoot(self,cond=None):
        if self._parent is None:
            return self
        root = None
        obj  = self
        while not obj is None:
            root = obj
            if cond and cond(root):
                break
            obj = obj._getParent()
        return root

    # accept a visitor pattern 
    def accept(self,visitor):
        visitor.nodeBegin(self)

        def getdata(name):
            try:
                return getattr(self,name)
            except AttributeError:
                return self._data[name]
            
        for (name,item) in self._schema.simpleItems():
            if item['visitable']:            
                visitor.simpleAttribute(self,name,getdata(name),item['sequence'])

        for (name,item) in self._schema.componentItems():
            if item['visitable']:
                visitor.componentAttribute(self,name,getdata(name),item['sequence'])
                                       
        visitor.nodeEnd(self)

    # clone self and return a properly initialized object
    def clone(self):
        return copy.deepcopy(self)

    # copy all the properties recursively from the srcobj
    # if schema of self and srcobj are not compatible raises a ValueError
    # ON FAILURE LEAVES SELF IN INCONSISTENT STATE
    def copyFrom(self,srcobj):
        for name,item in self._schema.allItems():
            if not srcobj._schema.hasAttribute(name):
                raise ValueError('copyFrom: incompatible schema: source=%s destination=%s'%(srcobj._name,self._name))
            if not item['copyable']:
                setattr(self,name,self._schema.getDefaultValue(name))
            else:
                c = copy.deepcopy(getattr(srcobj,name))
                setattr(self,name,c)
        
    def printTree(self,f=None, sel='' ):
        from VPrinter import VPrinter
        self.accept(VPrinter(f,sel))

    def printSummaryTree(self,level = 0, verbosity_level = 0, whitespace_marker = '', out = None, selection = ''):
        """If this method is overridden, the following should be noted:

        level: the hierachy level we are currently at in the object tree.
        verbosity_level: How verbose the print should be. Currently this is always 0.
        whitespace_marker: If printing on multiple lines, this allows the default indentation to be replicated.
                           The first line should never use this, as the substitution is 'name = %s' % printSummaryTree()
        out: An output stream to print to. The last line of output should be printed without a newline.'
        selection: See VPrinter for an explaintion of this.
        """
        from VPrinter import VSummaryPrinter
        self.accept(VSummaryPrinter(level, verbosity_level, whitespace_marker, out, selection))

    def __eq__(self,node):
        
        if self is node:
            return 1
        
        if not node or not self._schema.isEqual(node._schema):
            return 0

        for (name,item) in self._schema.allItems():
            if item['comparable']:
                if getattr(self,name) != getattr(node,name):
                    return 0
        return 1

    def __ne__(self,node):
        return not self == node
            
################################################################################   
class Descriptor(object):
    def __init__(self, name, item):
            self._name  = name
            self._item = item
            self._getter_name = None
            self._checkset_name = None
            self._filter_name = None
            
            try:
                self._getter_name = item['getter']
            except KeyError:
                pass

            try:
                self._checkset_name = item['checkset']
            except KeyError:
                pass
            
            try:
                self._filter_name = item['filter']
            except KeyError:
                pass

    def _bind_method(self,obj,name):
        if name is None:
            return None
        return getattr(obj,name)

    def _check_getter(self):
        if self._getter_name:
            raise AttributeError('cannot modify or delete "%s" property (declared as "getter")'%self._name)                    

    def __get__(self, obj, cls):
        if obj is None:
            return cls._schema[self._name]
        else:
            result = None
            g = self._bind_method(obj,self._getter_name)
            if g:
                result = g()
            else:
                #LAZYLOADING
                if obj._data is None and not obj._index_cache is None and self._name in obj._index_cache:
                    result = obj._index_cache[self._name]
                else:
                    obj._getReadAccess()
                    result = obj._data[self._name]
            
            return result

    def __set__(self, obj, val):

        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaList

        cs = self._bind_method(obj,self._checkset_name)
        if cs:
            cs(val)
        filter = self._bind_method(obj, self._filter_name)
        if filter:
            val = filter(val)

        #LOCKING
        obj._getWriteAccess()
        
        #self._check_getter()
            
        def cloneVal(v):
            #print 'cloneVal:',self._name,v,item['optional'],item['load_default'], item['defvalue']
            if v is None:
                assert(item['optional'])
                return None
            else:    
                assert(isinstance(v, Node))
                if isinstance(v, GangaList):
                    catagories = v.getCategory()
                    len_cat = len(catagories)
                    #we pass on empty lists, as the catagory is yet to be defined
                    if (len_cat > 1) or ((len_cat == 1) and (catagories[0] != item['category'])):
                        raise GangaAttributeError('%s: attempt to assign a list containing incompatible objects %s to the property in category "%s"' %(self._name, v,item['category']))  
                else:                                                         
                        if v._category != item['category']:
                            raise GangaAttributeError('%s: attempt to assign an incompatible object %s to the property in category "%s"' %(self._name, v,item['category']))  
                v = v.clone()
                v._setParent(obj)
                return v

        item = obj._schema[self._name]

        if item.isA(Schema.ComponentItem):
            if item['sequence']:
##                 checklist=filter(lambda x: not implies(x is None,item['optional']) or  x._category != item['category'],val)
##                 if len(checklist) > 0:
##                     raise AttributeError('%s: attempt to assign incompatible objects %s to the property in category "%s"'%(self._name, str(checklist),item['category']))
                val = makeGangaList(val,cloneVal, parent = obj)
            else:
                val = cloneVal(val)

##                 if val is None:
##                     assert(item['optional'])
##                 else:    
##                     assert(isinstance(val, Node))
##                     if val._category != item['category']:
##                         raise AttributeError('%s: attempt to assign an incompatible object %s to the property in category "%s"' %(self._name, val,item['category']))
##                     val = cloneVal(val)
        else:
            if item['sequence']:
                val = makeGangaList(val, parent = obj)

        obj._data[self._name] = val

        obj._setDirty()

            
    def __delete__(self, obj):
        #self._check_getter()
        del obj._data[self._name]


class ObjectMetaclass(type):
    _descriptor = Descriptor
    def __init__(cls, name, bases, dict):
        super(ObjectMetaclass, cls).__init__(name, bases, dict)

        # ignore the 'abstract' base class
        # FIXME: this mechanism should be based on explicit cls._name or alike
        if name == 'GangaObject':
            return 

        logger.debug("Metaclass.__init__: class %s name %s bases %s",cls,name,bases)
        
        # all Ganga classes must have (even empty) schema
        assert(not cls._schema is None)
        
        # produce a GPI class (proxy)
        proxyClass = GPIProxyClassFactory(name,cls)
        
        # export public methods of this class and also of all the bases
        # this class is scanned last to extract the most up-to-date docstring
        dictlist = [b.__dict__ for b in cls.__mro__]
        for di in range(0, len(dictlist)):
            d = dictlist[len(dictlist)-1-di]
            for k in d:
                if k in cls._exportmethods:
                    try:
                        internal_name = "_export_"+k
                        method = d[internal_name]
                    except KeyError:
                         internal_name = k
                         method = d[k]
                    if not (type(method) == types.FunctionType):
                       continue
                    f = ProxyMethodDescriptor(k,internal_name)
                    f.__doc__ = method.__doc__
                    setattr(proxyClass, k, f)

        # sanity checks for schema...
        if not '_schema' in dict.keys():
            s = "Class %s must _schema (it cannot be silently inherited)" % (name,)
            logger.error(s)
            raise ValueError(s)

        if not cls._schema._pluginclass is None:
            logger.warning('Possible schema clash in class %s between %s and %s',name,cls._name,cls._schema._pluginclass._name)

        # export visible properties... do not export hidden properties
        for attr, item in cls._schema.allItems():            
            setattr(cls, attr, cls._descriptor(attr, item))
            if not item['hidden']:
                setattr(proxyClass, attr, ProxyDataDescriptor(attr))

        # additional check of type
        # bugfix #40220: Ensure that default values satisfy the declared types in the schema
        for attr, item in cls._schema.simpleItems():
            if not item['getter']:
                item._check_type(item['defvalue'],'.'.join([name,attr]),enableGangaList=False)

        # create reference in schema to the pluginclass
        cls._schema._pluginclass = cls

        # store generated proxy class
        cls._proxyClass = proxyClass
        
        # register plugin class
        if not cls._declared_property('hidden') or cls._declared_property('enable_plugin'):
            allPlugins.add(cls,cls._category,cls._name)

        # create a configuration unit for default values of object properties
        if not cls._declared_property('hidden') or cls._declared_property('enable_config'):
            cls._schema.createDefaultConfig()

    
class GangaObject(Node):
    __metaclass__ = ObjectMetaclass
    _schema       = None # obligatory, specified in the derived classes
    _proxyClass   = None # created automatically
    _registry     = None # automatically set for Root objects
    _exportmethods= [] # optional, specified in the derived classes

    # by default classes are not hidden, config generation and plugin registration is enabled
    _hidden       = 1  # optional, specify in the class if you do not want to export it publicly in GPI,
                       # the class will not be registered as a plugin unless _enable_plugin is defined
                       # the test if the class is hidden is performed by x._declared_property('hidden')
                       # which makes sure that _hidden must be *explicitly* declared, not inherited

    # additional properties that may be set in derived classes which were declared as _hidden:
    #   _enable_plugin = 1 -> allow registration of _hidden classes in the allPlugins dictionary
    #   _enable_config = 1 -> allow generation of [default_X] configuration section with schema properties
    
    # the constructor is directly used by the GPI proxy so the GangaObject must be fully initialized
    def __init__(self):
        # IMPORTANT: if you add instance attributes like in the line below
        # make sure to update the __getstate__ method as well
        self._proxyObject  = None # use cache to help preserve proxy objects identity in GPI
        self._dirty = 0 # dirty flag is true if the object has been modified locally and its contents is out-of-sync with its repository
        
        super(GangaObject, self).__init__(None)
        for attr, item in self._schema.allItems():
            setattr(self, attr, self._schema.getDefaultValue(attr))
            
        # Overwrite default values with any config values specified
        #self.setPropertiesFromConfig()                

    def __getstate__(self):
        # IMPORTANT: keep this in sync with the __init__
        self._getReadAccess()
        dict = super(GangaObject, self).__getstate__()
        dict['_proxyObject'] = None
        dict['_dirty'] = 0
        return dict

    def __setstate__(self, dict):
        self._getWriteAccess()
        super(GangaObject, self).__setstate__(dict)
        self._setParent(None)
        self._proxyObject = None
        self._dirty = 0

    # on the deepcopy reset all non-copyable properties as defined in the schema
    def __deepcopy__(self, memo = None):
        self._getReadAccess()
        c = super(GangaObject,self).__deepcopy__(memo)
        for name,item in self._schema.allItems():
            if not item['copyable']:
                setattr(c,name,self._schema.getDefaultValue(name))
        return c

    def accept(self, visitor):
        self._getReadAccess()
        super(GangaObject, self).accept(visitor)

    def _getWriteAccess(self):
        """ tries to get write access to the object.
        Raise LockingError (or so) on fail """
        root = self._getRoot()
        reg = root._getRegistry()
        if reg is not None:
            reg._write_access(root)

    def _getReadAccess(self):
        """ makes sure the objects _data is there and the object itself has a recent state.
        Raise RepositoryError"""
        root = self._getRoot()
        reg = root._getRegistry()
        if reg is not None:
            reg._read_access(root,self)
            #print "excepting because of access to ", self._name
            #import traceback
            #traceback.print_stack()
            #raise Exception(self._name)

    # define when the object is read-only (for example a job is read-only in the states other than new)
    def _readonly(self):
        r = self._getRoot()
        # is object a root for itself? check needed otherwise infinite recursion
        if r is None or r is self: return 0
        else:
            return r._readonly()

    # set the registry for this object (assumes this object is a root object)
    def _setRegistry(self, registry):
        assert self._getParent() is None
        self._registry = registry

    # get the registry for the object by getting the registry associated with the root object (if any)
    def _getRegistry(self):
        r = self._getRoot()
        try:
            return r._registry
        except AttributeError:
            return None

    def _getRegistryID(self):
        try:
            return self._registry.find(self)
        except AttributeError:
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

    # post __init__ hook automatically called by GPI Proxy __init__
    def _auto__init__(self):
        pass

    # return True if _name attribute was explicitly defined in the class
    # this means that implicit (inherited) _name attribute has no effect in the derived class
    # example: cls._declared_property('hidden') => True if there is class attribute _hidden explicitly declared 
    def _declared_property(self,name):
        return '_'+name in self.__dict__

    _declared_property = classmethod(_declared_property)
            
    # get the job object associated with self or raise an assertion error
    # the FIRST PARENT Job is returned...
    # this method is for convenience and may well be moved to some subclass
    def getJobObject(self):
        from Ganga.GPIDev.Lib.Job import Job
        r = self._getRoot(cond=lambda o: isinstance(o,Job))
        if not isinstance(r,Job):
            raise AssertionError('no job associated with object '+repr(self))
        return r

    # Customization of the GPI attribute assignment: Attribute Filters
    #
    # Example of usage:
    # if some properties depend on the value of other properties in a complex way such as:
    # changing platform of Gaudi should change the version if it is not supported... etc.
    #
    # Semantics:
    #  gpi_proxy.x = v        --> gpi_proxy._impl._attribute_filter__set__('x',v)
    #  gpi_proxy.y = [v1,v2]  --> gpi_proxy._impl._attribute_filter__set__('x',[v1,v2])
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
    def _attribute_filter__set__(self,name,v):
        return v

# define the default component object filter:
# obj.x = "Y"   <=> obj.x = Y()
 
def string_type_shortcut_filter(val,item):
    if type(val) is type(''):
        if item is None:
            raise ValueError('cannot apply default string conversion, probably you are trying to use it in the constructor')
        from Ganga.Utility.Plugin import allPlugins, PluginManagerError
        try:
            obj = allPlugins.find(item['category'],val)()
            obj._auto__init__()
            return obj
        except PluginManagerError,x:
            raise ValueError(x)
    return None

# FIXME: change into classmethod (do they inherit?) and then change stripComponentObject to use class instead of
# FIXME: object (object model clearly fails with sequence of Files)
# FIXME: test: ../bin/ganga -c local_lhcb.ini run.py TestNativeSpecific.testFileSequence


from Filters import allComponentFilters
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
