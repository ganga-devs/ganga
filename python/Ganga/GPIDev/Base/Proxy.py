################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Proxy.py,v 1.2.4.3 2009-07-10 11:29:27 ebke Exp $
################################################################################

import Ganga.Utility.logging
from Ganga.Utility.Config import getConfig
logger = Ganga.Utility.logging.getLogger(modulename=1)

import Ganga.GPIDev.Schema as Schema

from Ganga.Core import GangaException,GangaAttributeError,ProtectedAttributeError,ReadOnlyObjectError,TypeMismatchError,SchemaError

from Ganga.Utility.util import importName
prepconfig = getConfig('Preparable')

import os
from Ganga.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

#some proxy related convieniance methods
def isProxy(obj):
    """Checks if an object is a proxy"""
    return isinstance(obj, GPIProxyObject)

def isType(obj, type_or_seq):
    """Checks whether on object is of the specified type, stripping proxies as needed."""
    return isinstance(stripProxy(obj), type_or_seq)

def stripProxy(obj):
    """Removes the proxy if there is one"""
    result = obj
    if isProxy(obj):
        result = obj._impl
    return result

def addProxy(obj):
    """Adds a proxy to a GangaObject"""
    from Ganga.GPIDev.Base.Objects import GangaObject
    if isinstance(obj, GangaObject):
            return GPIProxyObjectFactory(obj)
    return obj

def getProxyAttr(obj, attr_name):
    """Gets an attribute from a proxied object"""
    return getattr(stripProxy(obj), attr_name)

def runProxyMethod(obj, method_name, *args):
    """Calls a method on the object, removing the proxy if needed"""
    fp = getProxyAttr(obj,method_name)
    return fp(*args)

# apply object conversion or if it fails, strip the proxy and extract the object implementation
def stripComponentObject(v,cfilter,item):

    from Ganga.GPIDev.Base import GangaObject

    def getImpl(v):
        if v is None:
            if not item['optional']:
                raise TypeMismatchError(None,'component is mandatory and None may not be used')
            return v
        if isinstance(v, GangaObject):
            return v
        if not isinstance(v, GPIProxyObject):
            raise TypeMismatchError("cannot assign value '%s', expected a '%s' object "%(repr(v),item['category']))
        return v._impl

    vv = cfilter(v,item)
    if vv is None:
        return getImpl(v)
    else:
        return vv                

from Ganga.GPIDev.TypeCheck import _valueTypeAllowed
valueTypeAllowed = lambda val,valTypeList: _valueTypeAllowed(val,valTypeList,logger)

class ProxyDataDescriptor(object):
    def __init__(self, name):
        self._name = name

    def __get__(self, obj, cls):
        # at class level return a helper object (for textual description)
        if obj is None:
            #return Schema.make_helper(getattr(cls._impl,self._name))
            return getattr(cls._impl,self._name)
        
        val = getattr(obj._impl, self._name)

        # apply object conversion or if it failes, make the wrapper proxy
        def disguiseComponentObject(v):
            # get the proxy for implementation object
            def getProxy(v):
                from Ganga.GPIDev.Base import GangaObject
                if not isinstance(v, GangaObject):
                    raise GangaAttributeError("invalid type: cannot assign '%s' to attribute '%s'"%(repr(v),self._name))
                return GPIProxyObjectFactory(v)
            
            # convert implementation object to GPI value according to the static method defined in the implementation object
            def object2value(v):
                return None
            
            vv = object2value(v)
            if vv is None:
                # allow None to be a legal value for optional component items
                if v is None: return None
                else:
                    return getProxy(v)
            else:
                return vv                

        # apply attribute conversion
        def disguiseAttribute(v):
            #FIXME: this is obsoleted method
            from Ganga.GPIDev.Base import GangaObject
            if isinstance(v,GangaObject):
                return GPIProxyObjectFactory(v)
            return v
            
        # wrap proxy
        item = obj._impl._schema[self._name]

        if item['proxy_get']:
            return getattr(obj._impl,item['proxy_get'])()

        if item.isA(Schema.ComponentItem):
            disguiser = disguiseComponentObject
        else:
            disguiser = disguiseAttribute
        
        from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList
        if item['sequence'] and isinstance(val,list):
            val = makeGangaList(val,disguiser)

        return disguiser(val)

    def _check_type(self, obj, val):
        item = obj._impl._schema[self._name]
        return item._check_type(val, self._name)
        
    def __set__(self, obj, val):
        #self is the attribute we're about to change (?)
        #obj is the object we're about to make the change in
        #val is the value we're setting the attribute to.
        #item is the schema entry of the attribute we're about to change
        item = obj._impl._schema[self._name]
        if item['protected']:
            raise ProtectedAttributeError('"%s" attribute is protected and cannot be modified'%(self._name,))
        if obj._impl._readonly():
            raise ReadOnlyObjectError('object %s is read-only and attribute "%s" cannot be modified now'%(repr(obj),self._name))

        #mechanism to provide for locking of preparable attributes
        #we tried this with the 'protected' meta attribute, but this is static, so cannot be set on a per-instance basis.
        if item['preparable']:
            #then we must have an application here.
            if hasattr(obj,'is_prepared'):
                if obj.is_prepared is not None and obj.is_prepared is not True:
                    #then we must have a preparble application that has been prepared
                    raise ProtectedAttributeError('AttributeError: "%s" attribute belongs to a prepared application and so cannot be modified. unprepare() the application if you want to modify this value, or copy the job/application (using j.copy(unprepare=True)) and modify that instance.'%(self._name,))

        #if we set is_prepared to None in the GPI, that should effectively unprepare the application
        if self._name == 'is_prepared' and val is None and obj.is_prepared is not None:
            logger.info('Unpreparing application.')
            obj.unprepare()

        #if we set is_prepared to another ShareDir in the GPI, that should unprepare the original application
        #and reference the new one, remembering to increase the shareref counter accordingly
        #if self._name == 'is_prepared' and isType(val,ShareDir) and obj.is_prepared is not None:
        if hasattr(obj,'is_prepared') and hasattr(val,'_root_shared_path'):
            #if self._name == 'is_prepared' and obj.is_prepared is not True and obj.is_prepared is not None:
            if self._name == 'is_prepared' and obj.is_prepared is not True:
                logger.info('Overwriting is_prepared attribute with a ShareDir object')
                #we don't need to check to see whether the receiving object is prepared or not before unpreparing it
                obj.unprepare()
                from Ganga.Core.GangaRepository import getRegistry
                shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef()) 
                s=shareref._impl.name
                shareref.increase(val.name)

        #here we catch assignment of something  to a preparable application    
        if self._name == 'application' and hasattr(obj.application,'is_prepared'):
            #catch assignment of an unprepared app over the top of an existing prepared one e.g.
            #a=Job(); a.prepare(); a.application=Executable()
            if obj.application.is_prepared is not None and obj.application.is_prepared is not True and val.is_prepared is None:
                logger.info('Overwriting a prepared application with one that is unprepared')
                obj.application.unprepare()
            #catch assignment of a prepared application over the top of an unprepared one e.g.
            #a=Job(); b=Executable(); b.prepare(); a.application=b
            elif obj.application.is_prepared is not True and hasattr(val,'is_prepared') and val.is_prepared is not None and val.is_prepared is not True:
                from Ganga.Core.GangaRepository import getRegistry
                shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef()) 
                s=shareref._impl.name
                logger.info('Overwriting application with a prepared one')
                #note that it doesn't matter whether we are overwriting a prepared or unprepared application, because  
                #unpreparing an already unprepared app will not break anything
                obj.application.unprepare()
                shareref.increase(val.is_prepared.name)
        #check that the shared directory actually exists before assigning the (prepared) application to a job
        if hasattr(val, 'is_prepared'):
            if val.is_prepared is not None and val.is_prepared is not True:
                if not os.path.isdir(os.path.join(shared_path,val.is_prepared.name)):
                    logger.error('ShareDir directory not found: %s' % val.is_prepared.name)
                    logger.error('Unpreparing %s application' % val._impl._name)
                    val.unprepare()


        # apply attribute conversion
        def stripAttribute(v):
            # just warn
            #print '**** checking',v,v.__class__, isinstance(val,GPIProxyObject)
            if isinstance(v,GPIProxyObject):
                v = v._impl
                logger.debug('%s property: assigned a component object (_impl used)',self._name)            
            return obj._impl._attribute_filter__set__(self._name,v)

        # unwrap proxy
        if item.isA(Schema.ComponentItem):
            from Filters import allComponentFilters
            item = obj._impl._schema.getItem(self._name)
            cfilter = allComponentFilters[item['category']]
            stripper = lambda v: stripComponentObject(v,cfilter,item)
        else:
            stripper = stripAttribute

        from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaList
        if item['sequence']:
            # we need to explicitly check for the list type, because simple values (such as strings) may be iterable
            if isType(val, (GangaList,list)):
                #create GangaList
                val = makeGangaList(val,stripper)
            else:
                # val is not iterable
                if item['strict_sequence']:
                    raise GangaAttributeError('cannot assign a simple value %s to a strict sequence attribute %s.%s (a list is expected instead)'%(repr(val),obj._impl._schema.name,self._name))
                val = makeGangaList(stripper(val))
        else:
            val = stripper(val)

        # apply attribute filter to component items
        if item.isA(Schema.ComponentItem):
            val = stripAttribute(val)

        self._check_type(obj,val)
        setattr(obj._impl, self._name, val)


class ProxyMethodDescriptor(object):
    def __init__(self, name, internal_name):
        self._name = name
        self._internal_name = internal_name
    def __get__(self, obj, cls):
        if obj is None:
            return getattr(cls._impl,self._internal_name)
        return getattr(obj._impl, self._internal_name)

################################################################################

# helper to create a wrapper for an existing ganga object
def GPIProxyObjectFactory(obj):
    if obj._proxyObject is None:
        cls = obj._proxyClass
        proxy = super(cls, cls).__new__(cls)
        proxy.__dict__['_impl'] = obj #FIXME: NEW STYLE CLASS CAN DO __DICT__??
        obj._proxyObject = proxy
        logger.debug('generated the proxy '+repr(proxy))
    else:
        logger.debug('reusing the proxy '+repr(obj._proxyObject))
    return obj._proxyObject # FIXED

# this class serves only as a 'tag' for all generated GPI proxy classes
# so we can test with isinstance rather then relying on more generic but less user friendly checking of attribute x._impl
class GPIProxyObject(object): pass

# create a new GPI class for a given ganga (plugin) class
def GPIProxyClassFactory(name, pluginclass):

    def helptext(f,s):
        f.__doc__ = s % {'classname':name,'objname':name.lower(),'shortvarname':name[0].lower()}
    
    # construct the class on-the-fly using the functions below as methods for the new class

    def _init(self,*args,**kwds):
        #if len(args) > 1:
        #    logger.warning('extra arguments in the %s constructor ignored: %s',name,args[1:])

        # at the object level _impl is a ganga plugin object
        self.__dict__['_impl'] = pluginclass()
        self._impl.__construct__(map(stripProxy,args))

        # initialize all properties from keywords of the constructor
        for k in kwds:
            if self._impl._schema.hasAttribute(k):
                setattr(self,k,kwds[k])
            else:
                logger.warning('keyword argument in the %s constructur ignored: %s=%s (not defined in the schema)',name,k,kwds[k])
        
        self._impl._proxyObject = self
        self._impl._auto__init__()

    from Ganga.Utility.strings import ItemizedTextParagraph

    itbuf = ItemizedTextParagraph('Properties:',linesep='')

    for n,item in pluginclass._schema.allItems():
        if not item['hidden']:
            itbuf.addLine(n,item.describe())

    if not pluginclass.__doc__:
        pluginclass.__doc__ = 'Documentation missing.'

    pluginclass.__doc__.strip()
    pluginclass.__doc__ +=  "\n\n" 
        
    publicdoc = pluginclass.__doc__ + itbuf.getString()
    helptext(pluginclass,'This is a Ganga.GPI.%(classname)s implementation class. Refer to Ganga.GPI.%(classname)s.__doc__ for documentation.')
    
    helptext(_init, """GPI %(classname)s object constructor:
    %(classname)s() : create %(objname)s with default settings;
    %(classname)s(%(shortvarname)s) : make a copy of %(shortvarname)s;
    %(classname)s(%(shortvarname)s,x=a,...): make a copy of %(shortvarname)s and set property 'x' to a, etc..
    """)
        
    def _str(self):
        import StringIO
        sio = StringIO.StringIO()
        self._impl.printSummaryTree(0,0,'',out = sio)
        return sio.getvalue()
    helptext(_str,"""Return a printable string representing %(classname)s object as a tree of properties.""")

    def _repr(self):
        try:
            return self._impl._repr()
        except AttributeError:
            return '<'+repr(self._impl)+' PROXY at '+hex(abs(id(self)))+'>'
    helptext(_repr,"Return an short representation of %(classname)s object.")

    def _eq(self,x):
        result = False
        if isinstance(x, GPIProxyObject): result = self._impl.__eq__(x._impl)
        else: result = self._impl.__eq__(x)
        return result
    helptext(_eq,"Equality operator (==), compare the %(classname)s properties which are declared as [comparable].")

    def _ne(self,x):
        return self._impl.__ne__(x._impl)
    helptext(_ne,"Non-equality operator (!=).")

    def _copy(self, unprepare=None):
        logger.debug('unprepare is %s', str(unprepare))
        if unprepare is None:
            if prepconfig['unprepare_on_copy'] is True:
                if hasattr(self,'is_prepared') or hasattr(self,'application'):
                    unprepare = True
#        if hasattr(self,'is_prepared'):
#            if self.is_prepared is not None and self.is_prepared is not True:
#                from Ganga.Core.GangaRepository import getRegistry
#                shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef()) 
#                shareref.increase(self.is_prepared.name)

#        if unprepare is not True:

        if hasattr(self,'application'):
            if hasattr(self.application,'is_prepared'):
                if self.application.is_prepared is not None and self.application.is_prepared \
                           is not True and os.path.isdir(os.path.join(shared_path,self.application.is_prepared.name)):
                    from Ganga.Core.GangaRepository import getRegistry
                    shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef()) 
                    logger.debug('increasing counter from proxy.py')
                    shareref.increase(self.application.is_prepared.name)
                    logger.debug('Found ShareDir directory: %s' % self.application.is_prepared.name)
                elif self.application.is_prepared is not None and self.application.is_prepared \
                           is not True and not os.path.isdir(os.path.join(shared_path,self.application.is_prepared.name)):
                    logger.error('ShareDir directory not found: %s' % self.application.is_prepared.name)
                    logger.error('Unpreparing Job #%s' % self.id)
                    from Ganga.Core.GangaRepository import getRegistry
                    shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef()) 
                    shareref.increase(self.application.is_prepared.name)
                    self.unprepare()
    
#The following has the effect of increaseing shareref when we copy a prepared, but isolated application
#that's a bug.
#        if hasattr(self,'is_prepared') and self.is_prepared is not None and self.is_prepared \
#                           is not True and os.path.isdir(self.is_prepared.name):
#            from Ganga.Core.GangaRepository import getRegistry
#            shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef()) 
#            shareref.increase(self.is_prepared.name)
#            logger.debug('Found ShareDir directory: %s' % self.is_prepared.name)
        if hasattr(self,'is_prepared') and self.is_prepared is not None and self.is_prepared \
                           is not True and not os.path.isdir(os.path.join(shared_path,self.is_prepared.name)):
            logger.error('ShareDir directory not found: %s' % self.is_prepared.name)
            logger.error('Unpreparing %s application' % self._impl._name)
            self.unprepare()

            
        if unprepare is True:
            c = self._impl.clone()
            c._auto__init__(unprepare=True)
        else:
            c = self._impl.clone()
            c._auto__init__()
        return GPIProxyObjectFactory(c)

    helptext(_copy,"Make an identical copy of self.")
    
    def _setattr(self,x,v):
        'something'
        if x == '_impl':
            raise AttributeError("Internal implementation object '_impl' cannot be reassigned")

        if not self._impl._schema.hasAttribute(x):
            raise GangaAttributeError("'%s' has no attribute '%s'" % (self._impl._name,x))

        object.__setattr__(self,x,v)
    helptext(_setattr,"""Set a property of %(classname)s with consistency and safety checks.
Setting a [protected] or a unexisting property raises AttributeError.""")

    

    # but at the class level _impl is a ganga plugin class
    d = { '_impl' : pluginclass,
          '__init__' : _init,
          '__str__' : _str,
          '__repr__': _repr,
          '__eq__': _eq,
          '__ne__': _ne,
          'copy' : _copy,
          '__doc__' : publicdoc,
          '__setattr__': _setattr
         }

    ## TODO: this makes GangaList inherit from the list
    ## this is not tested and specifically the TestGangaList/testAllListMethodsExported should be verified 
    ##if name == "GangaList":
    ##    return type(name, (GPIProxyObject,list), d)
    
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
# moved GangaException to Ganga.Core
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
