from __future__ import absolute_import
###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Schema.py,v 1.3 2009-05-20 13:40:22 moscicki Exp $
##########################################################################

import Ganga.Utility.logging

import copy

from Ganga.Utility.logic import implies

import Ganga.Utility.Config

from Ganga.Core.exceptions import GangaAttributeError, TypeMismatchError, SchemaError

from Ganga.Utility.Plugin import allPlugins

import types

from Ganga.GPIDev.TypeCheck import _valueTypeAllowed

logger = Ganga.Utility.logging.getLogger()

# Dictionary for storing data from the Config system which takes a while to lookup
_found_components = {}
_found_configs = {}
_found_attrs = {}
_stored_defaults = {}

#
# Ganga Public Interface Schema
#

# Version of the schema.


class Version(object):

    def __init__(self, major, minor):
        self.major = major
        self.minor = minor

    def __eq__(self, v):
        return self.major == v.major and self.minor == v.minor

    def __ne__(self, v):
        return not self == v

    def isCompatible(self, v):
        return v.major == self.major and v.minor <= self.minor


def defaultConfigSectionName(name):
    global _stored_defaults
    if name not in _stored_defaults:
       _stored_defaults[name] = 'defaults_' + name  # _Properties
    return _stored_defaults[name]

# Schema defines the logical model of the Ganga Public Interface (GPI)
# for  jobs and  pluggable  job components  such  as applications  and
# submission backends.

# Schema contains the data description (properties) which is the basis
# of the efficient management of the persistent data in a generic way.
# Any Ganga component is  a container of properties (a property
# may be a component as well).  Properties may be protected (read-only
# by  the  users).  Persistent  properties  are  stored  with the  job
# whereas transient are not.  This additional information is contained
# in  metaproperties. Large fractions of Ganga GPI may be generated
# automatically based on the metaproperties definition in a schema.

# Each  component   belongs  to   a  certain  category   (for  example
# 'application'). There  may be many different  types of applications,
# each one defined by a separate component.

# Schema  of each  component  provides a  version  information (it  is
# important for persistent storage) and compatibility of the scripts.

# Internal Note:  Schema object must be  additionally initialized with
# the _pluginclass object  which is used as the  source of information
# about name and category of  the schema deepcopy of Schema objects is
# possible  however   the  _pluginclass  objects   are  shared  unless
# overriden explicitly.

class Schema(object):
    # Schema constructor is used by Ganga plugin developers.
    # Ganga will automatically set a reference to the plugin class which corresponds to this schema, hence
    # defining the schema's name and category.
    #
    # datadict: dictionary of properties (schema items) Defaults to '{}'
    # version: the version information

    def __init__(self, version, datadict=None):
        self.datadict = datadict or {}
        self.version = version
        self._pluginclass = None

    def __getitem__(self, name):
        if name in self.datadict:
            return self.datadict[name]
        else:
            err_str = "Ganga Cannot find: %s in Object: %s" % (name, self.name)
            logger.error(err_str)
            raise GangaAttributeError(err_str)

    @property
    def category(self):
        return self._pluginclass._category

    @property
    def name(self):
        from Ganga.GPIDev.Base.Proxy import getName
        return getName(self._pluginclass)

    def allItemNames(self):
        return self.datadict.keys()

    def allItems(self):
        if self.datadict is None: return zip()
        return zip(self.datadict.keys(), self.datadict.values())

    def simpleItems(self):
        return self._filter(SimpleItem)

    def componentItems(self):
        return self._filter(ComponentItem)

    def sharedItems(self):
        return self._filter(SharedItem)

    def hasAttribute(self, name):
        return name in self.datadict

    def getItem(self, name):
        return self.__getitem__(name)

    def hasItem(self, name):
        return name in self.datadict

    def _filter(self, klass):
        if self.datadict is None:
            return []

        r = []
        for n, c in zip(self.datadict.keys(), self.datadict.values()):
            if issubclass(c.__class__, klass):
                r.append((n, c))
        return r

    def isEqual(self, schema):
        return self.name == schema.name and self.category == schema.category

    # make a schema copy for a derived class, does not copy the pluginclass
    def inherit_copy(self):
        new_dict = {}
        for key, val in self.datadict.iteritems():
            new_dict[key] = copy.deepcopy(val)
        return Schema(version=copy.deepcopy(self.version), datadict=new_dict)

    def createDefaultConfig(self):
        # create a configuration unit for default values of object properties
        # take the defaults from schema defaults
        config = Ganga.Utility.Config.makeConfig(defaultConfigSectionName(self.name),
                                                 "default attribute values for %s objects" % self.name)

        for name, item in self.allItems():
            # and not item['sequence']: #FIXME: do we need it or not??
            if not item['protected'] and not item['hidden']:
                if item.hasProperty('typelist'):
                    types = item['typelist']
                    if types == []:
                        types = None
                else:
                    types = None

                if item['sequence']:
                    if not types is None:
                        # bugfix 36398: allow to assign a list in the
                        # configuration
                        types.append('list')
                if isinstance(item['defvalue'], dict):
                    if not types is None:
                        types.append('dict')
                config.addOption(
                    name, item['defvalue'], item['doc'], override=False, typelist=types)

        def prehook(name, x):
            errmsg = "Cannot set %s=%s in [%s]: " % (name, repr(x), config.name)

            try:
                item = self.getItem(name)
            #except KeyError as x:
            #    raise Ganga.Utility.Config.ConfigError(errmsg + "attribute not defined in the schema")
            except Exception as x:
                raise Ganga.Utility.Config.ConfigError(errmsg + str(x))

            if item.isA(ComponentItem):
                if not isinstance(x, str) and not x is None:
                    raise Ganga.Utility.Config.ConfigError(errmsg + "only strings and None allowed as a default value of Component Item.")
                try:
                    self._getDefaultValueInternal(name, x, check=True)
                except Exception as err:
                    logger.info("Unexpected error: %s", err)
                    raise
                    #Ganga.Utility.logging.log_unknown_exception()
                    #raise Ganga.Utility.Config.ConfigError(errmsg + str(x))

            if item['protected'] or item['hidden']:
                raise Ganga.Utility.Config.ConfigError(errmsg + "protected or hidden property")

            # FIXME: File() == 'x' triggers AttributeError
            # try:
            #    if x == '': x = None
            # except AttributeError:
            #    pass

            return x

        config.attachUserHandler(prehook, None)
        config.attachSessionHandler(prehook, None)


    def getDefaultValue(self, attr):
        """ Get the default value of a schema item, both simple and component.
        """
        return self._getDefaultValueInternal(attr)

    def _getDefaultValueInternal(self, attr, val=None, check=False):
        """ Get the default value of a schema item, both simple and component.
        If check is True then val is used instead of default value: this is used to check if the val may be used as a default value (e.g. if it is OK to use it as a value in the config file)
        """
        def_name = defaultConfigSectionName(self.name)

        item = self.getItem(attr)

        stored_attr_key = def_name + ':' + str(attr)

        from Ganga.Utility.Config import Config
        is_finalized = Config._after_bootstrap

        useDefVal = False

        try:
            # Attempt to get the relevant config section
            config = Config.getConfig(def_name, create=False)

            if is_finalized and stored_attr_key in _found_attrs and not config.hasModified():
                defvalue = _found_attrs[stored_attr_key]
            else:
                if attr in config.getEffectiveOptions():
                    defvalue = config[attr]
                    from Ganga.GPIDev.Base.Proxy import isProxy
                    if isProxy(defvalue):
                        raise GangaException("(1)Proxy found where it shouldn't be in the Config: %s" % stored_attr_key)
                    ## Just in case a developer puts the proxied object into the default value!
                    _found_attrs[stored_attr_key] = defvalue
                else:
                    useDefVal = True
        except (KeyError, Config.ConfigError):
            useDefVal = True

        if useDefVal:
            # hidden, protected and sequence values are not represented in config
            defvalue = item['defvalue']
            from Ganga.GPIDev.Base.Proxy import isProxy
            if isProxy(defvalue):
                raise GangaException("(2)Proxy found where is shouldn't be in the Config" % stored_attr_key)
            ## Just in case a developer puts the proxied object into the default value!
            _found_attrs[stored_attr_key] = defvalue

        # in the checking mode, use the provided value instead
        if check is True:
            defvalue = val

        if isinstance(item, ComponentItem):

            # FIXME: limited support for initializing non-empty sequences (i.e.
            # apps => ['DaVinci','Executable'] is NOT correctly initialized)

            if not item['sequence']:
                if defvalue is None:
                    if not item['load_default']:
                        assert(item['optional'])
                        return None

                # if a defvalue of a component item is an object (not string) just process it as for SimpleItems (useful for FileItems)
                # otherwise do a lookup via plugin registry

                category = item['category']

                if isinstance(defvalue, str) or defvalue is None:
                    try:
                        config = Config.getConfig(def_name, create=False)
                        has_modified = config.hasModified()
                    except KeyError:
                        has_modified = False

                    if category not in _found_components or has_modified:
                        _found_components[category] = allPlugins.find(category, defvalue)
                    return _found_components[category]()

        # make a copy of the default value (to avoid strange effects if the
        # original modified)
        try:
            from Ganga.GPIDev.Base.Proxy import isType, getRuntimeGPIObject, stripProxy, getName
            from Ganga.GPIDev.Base.Objects import Node
            if isinstance(defvalue, Node):
                return stripProxy(getRuntimeGPIObject(getName(defvalue)))
            else:
                return copy.deepcopy(defvalue)
        except ImportError:
            return copy.deepcopy(defvalue)


# Items in schema may be either Components,Simples, Files or BindingItems.
#
# Component, Simple, File are data items and have implicit storage.
#
# Metaproperties of all Items:
#
# transient : never stored on persistent media
# protected : not modifiable via GPI
# hidden    : not visible in the GPI
# comparable: taken into account for equality checks
# sequence:   an item is a sequence (algorithms traversing the component tree dive into sequences as well)
# strict_sequence: if not strict sequence then assignment of a single item will be automatically converted to a 1-element sequence, i.e. obj.x = v => obj.x = [v]
# defvalue  : default value, if item is a sequence the defvalue must be a list
#             for component this must be either None or a string with a name of the component in the same category
# copyable  : if 0 then the property value will not be copied and the default value from schema will be set in the destination
#             implied initialization rule (unless the "copyable" metaproperty is explicitly set)
#             protected == 1 => copyable == 0
#             protected == 0 => copyable == 1
# doc       : a docstring
# checkset  : a bound checkset method, restrict write access at the object level (for example job.status
#             may not be modified directly in backend handlers, instead updateStatus() method should be used)
#
# filter    : a bound method can be used to change the values of an attribute. The attribute will be set to the return value
#
# visitable : if false then all algorithms based on the visitor patter will not accept this item [true]
#             this is needed in certain cases (such as job.master) to avoid infinite recursion (and loops)
#
# summary_print: An bound method name (string). Will be passed an attribute value and a verbosity_level. Should
#                return a (Python parsable) string summarising the state of the value.
#
# summary_sequence_maxlen: An integer longer than which a sequence will be summerised when doing a summary
#                          print. If the value is -1, the sequence will never be summerised.
#
# changable_at_resubmit: if true than the value may be changed at job resubmission, see Job.resubmit()
#
#
# Metaproperties of SimpleItems
#
# typelist  : a list of type names (strings) indicating allowed types of the property (e.g. ["str","int","Ganga.GPIDev.Lib.File.File.File"]), see: http://twiki.cern.ch/twiki/bin/view/ArdaGrid/GangaTypes
#     please consider not using strings in future, we can support the type objects correctly
#
#
# Metaproperties of ComponentItems:
#
# category : category of the component ('applications','backends',...)
# optional : if true then None may be used as a legal value of the item, [false]
# load_default: if true and defvalue is None then load default plugin, [true]
#
# defvalue is None load_default==true   => load default plugin
# defvalue is None load_default==false  => use None (optional MUST be true, otherwise error)
#
# defvalue is 'x'  => load 'x'
#
# getter : a bound getter method, this implies that the component does not have associated storage and cannot be neither set nor deleted [None]
#          getter implies: transient=1, protected=1, sequence=0, defvalue=None, load_default=0, optional=1, copyable=0
# proxy_get: a bound getter method for proxy decoration, allows to customize completely the creation of the proxy
#
#
# Metaproperties of FileItems
#
# FIXME: under development
# in/out    : direction wrt to the job submission operation
# ref/copy  : reference vs copy semantics for file contents
#

# BindingItems enable to hook arbitrary getter methods and do not have any implicit storage. Therefore
# the BindingItems are always transient, cannot be copied, cannot be
# sequences and have None default value.

class Item(object):
    # default values of common metaproperties
    _metaproperties = {'transient': 0, 'protected': 0, 'hidden': 0, 'comparable': 1, 'sequence': 0, 'defvalue': None, 'copyable': 1,
                        'doc': '', 'visitable': 1, 'checkset': None, 'filter': None, 'strict_sequence': 1, 'summary_print': None,
                        'summary_sequence_maxlen': 5, 'proxy_get': None, 'getter': None, 'changable_at_resubmit': 0, 'preparable': 0,
                        'optional': 0, 'category': 'internal', 'typelist': None, 'load_default': 1}

    def __init__(self):
        super(Item, self).__init__()
        self._meta = Item._metaproperties.copy()

    def __construct(self, args):
        self._meta = Item._metaproperties.copy()

    def __getitem__(self, key):
        return self._meta[key]

    def hasProperty(self, key):
        return key in self._meta

    def getProperties(self):
        return self._meta

    def __len__(self):
        return len(self._meta)

    # compare the kind of item:
    # all calls are equivalent:
    # item.isA(SimpleItem)
    def isA(self, _what):

        from Ganga.GPIDev.Base.Proxy import stripProxy

        what = stripProxy(_what)

        if isinstance(what, types.InstanceType):
            what = what.__class__

        return issubclass(self.__class__, what)

    def _update(self, kwds, forced=None):
        """ Add new metaproperties/override old values. To be used by derived contructors only.
        'forced' is an (optional) dictionary containing all values which cannot be modified by
        the user of the derived contructor.
        """
        if forced:
            # find intersection
            forbidden = [k for k in forced if k in kwds]
            if len(forbidden) > 0:
                raise TypeError('%s received forbidden (forced) keyword arguments %s' % (self.__class__, forbidden))
            self._meta.update(forced)

        self._meta.update(kwds)

        # conditional initial value logic...
        # unless 'copyable' explicitly set (or forced), protected==1 =>
        # copyable==0
        if 'copyable' not in kwds and (not forced or 'copyable' not in forced):
            if self._meta['protected']:
                #logger.debug('applied implicit conditional rule: protected==1 => copyable==0')
                self._meta['copyable'] = 0

    # return a description of the property including a docstring
    def describe(self):

        specdoc = '(' + self._describe() + ')'

        if self['doc']:
            s = "%s. %s" % (self['doc'], specdoc)
        else:
            s = specdoc

        return s

    # return a meta description of the property
    def _describe(self):
        first = ','
        #txt = ' ['
        txt = ''
        for m in ['transient', 'protected', 'comparable', 'optional']:
            if m in self._meta:
                txt += '%s%s' % (first, m)
            # try:
            #    if self[m]:
            #        txt += '%s%s'%(first,m)
            #        first = ','
            # except KeyError:
            #    pass
        #txt += ']'
        txt = " default=" + repr(self['defvalue']) + txt
        if self['sequence']:
            txt = " list," + txt
        return txt

    @staticmethod
    def __check(isAllowedType, name, validTypes, input_val):
        if not isAllowedType:
            #import traceback
            #traceback.print_stack()
            raise TypeMismatchError('Attribute "%s" expects a value of the following types: %s\nfound: "%s" of type: %s' % (name, validTypes, input_val, type(input_val)))

    def _check_type(self, val, name, enableGangaList=True):

        if enableGangaList:
            from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
        else:
            GangaList = list

        # type checking does not make too much sense for Component Items because component items are
        # always checked at the object (_impl) level for category compatibility.

        if self.isA(ComponentItem):
            return

        validTypes = self._meta['typelist']

        # setting typelist explicitly to None results in disabling the type
        # checking completely
        if validTypes is None:
            return

        if self._meta['sequence']:
            from Ganga.GPIDev.Base.Proxy import isType
            if not isType(self._meta['defvalue'], (list, tuple, GangaList)):
                raise SchemaError('Attribute "%s" defined as a sequence but defvalue is not a list.' % name)

            if not isType(val, (GangaList, tuple, list)):
                raise TypeMismatchError('Attribute "%s" expects a list.' % name)

        if validTypes:
            if self._meta['sequence']:

                for valItem in val:
                    if not valueTypeAllowed(valItem, validTypes):
                        raise TypeMismatchError('List entry %s for %s is invalid. Valid types: %s' % (valItem, name, validTypes))

                return
            else:  # Non-sequence
                if isinstance(self._meta['defvalue'], dict):
                    if isinstance(val, dict):
                        for dKey, dVal in val.iteritems():
                            if not valueTypeAllowed(dKey, validTypes) or not valueTypeAllowed(dVal, validTypes):
                                raise TypeMismatchError('Dictionary entry %s:%s for attribute %s is invalid. Valid types for key/value pairs: %s' % (dKey, dVal, name, validTypes))
                    else:  # new value is not a dict
                        raise TypeMismatchError('Attribute "%s" expects a dictionary.' % name)
                    return
                else:  # a 'simple' (i.e. non-dictionary) non-sequence value
                    self.__check(valueTypeAllowed(val, validTypes), name, validTypes, val)
                    return

        # typelist is not defined, use the type of the default value
        if self._meta['defvalue'] is None:
            logger.warning('type-checking disabled: type information not provided for %s, contact plugin developer', name)
        else:
            if self._meta['sequence']:
                logger.warning('type-checking is incomplete: type information not provided for a sequence %s, contact plugin developer', name)
            else:

                logger.debug("valType: %s defValueType: %s name: %s" % (type(val), type(self._meta['defvalue']), name))
                self.__check(self.__actualCheck(val, self._meta['defvalue']), name, type(self._meta['defvalue']), val)


    @staticmethod
    def __actualCheck(val, defVal):

        try:
            from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
            knownLists = (list, tuple, GangaList)
        except Exception as err:
            knownLists = (list, tuple)
        from Ganga.GPIDev.Base.Proxy import isType
        if isType(defVal, knownLists) and isType(val, knownLists):
                return True
        else:
            if type(defVal) == type:
                return isType(val, type)
            else:
                return isType(val, type(defVal))


class ComponentItem(Item):
    # schema user of a ComponentItem cannot change the forced values below
    _forced = {}

    def __init__(self, category, optional=0, load_default=1, **kwds):
        super(ComponentItem, self).__init__()
        kwds['category'] = category
        kwds['optional'] = optional
        kwds['load_default'] = load_default
        #kwds['getter'] = getter
        self._update(kwds, forced=ComponentItem._forced)
        assert(implies(self['defvalue'] is None and not self['load_default'], self['optional']))

        assert(implies(self['getter'], self['transient'] and self['defvalue'] is None and self['protected'] and not self['sequence'] and not self['copyable']))

    def _describe(self):
        return "'" + self['category'] + "' object," + Item._describe(self)


valueTypeAllowed = lambda val, valTypeList: _valueTypeAllowed(val, valTypeList, logger)

defaultValue='_NOT_A_VALUE_'


class SimpleItem(Item):

    def __init__(self, defvalue, typelist=defaultValue, **kwds):
        super(SimpleItem, self).__init__()
        if typelist == defaultValue:
            if type(defvalue) == dict:
                typelist = []
            else:
                typelist = [type(defvalue)]
        kwds['defvalue'] = defvalue
        kwds['typelist'] = typelist
        self._update(kwds)

    def _describe(self):
        return 'simple property,' + Item._describe(self)


class SharedItem(Item):

    def __init__(self, defvalue, typelist=defaultValue, **kwds):
        super(SharedItem, self).__init__()
        if typelist == defaultValue:
            if type(defvalue) == dict:
                typelist = []
            else:
                typelist = [type(defvalue)]
        kwds['defvalue'] = defvalue
        kwds['typelist'] = typelist
        self._update(kwds)

    def _describe(self):
        return 'shared property,' + Item._describe(self)


# Files are important and common enough to merit a special support for
# defining their metaproperties
class FileItem(ComponentItem):

    def __init__(self, **kwds):
        super(FileItem, self).__init__('files')
        self._update(kwds)

    def _describe(self):
        return "'files' object," + Item._describe(self)


class GangaFileItem(ComponentItem):

    def __init__(self, **kwds):
        super(GangaFileItem, self).__init__('gangafiles')
        self._update(kwds)

    def _describe(self):
        return "'gangafiles' object," + Item._describe(self)


if __name__ == '__main__':

    # a simple test

    dd = {
        'application': ComponentItem(category='applications'),
        'backend':     ComponentItem(category='backends'),
        'name':        SimpleItem('', comparable=0),
        'workdir':     SimpleItem(defvalue=None, type='string', transient=1, protected=1, comparable=0),
        'status':      SimpleItem(defvalue='new', protected=1, comparable=0),
        'id':           SimpleItem(defvalue=None, type='string', protected=1, comparable=0),
        'inputbox':     FileItem(defvalue=[], sequence=1),
        'outputbox':    FileItem(defvalue=[], sequence=1),
        'overriden_copyable': SimpleItem(defvalue=None, protected=1, copyable=1),
        'plain_copyable': SimpleItem(defvalue=None, copyable=0)
    }

    schema = Schema(Version(1, 0), dd)

    # NOT a public interface: emulate the Ganga Plugin object for test purposes
    # Note that pclass MUST be a new-style class in order to support deepcopy
    class pclass(object):
        _category = 'jobs'
        _name = 'Job'
    schema._pluginclass = pclass
    # end of emulating code
    # allSchemas.add(schema)

    assert(schema.name == 'Job')
    assert(schema.category == 'jobs')

    assert(schema.allItems() == dd.items())

    cc = (schema.componentItems() + schema.simpleItems()).sort()
    cc2 = dd.items().sort()
    assert(cc == cc2)

    for i in schema.allItems():
        assert(schema[i[0]] == schema.getItem(i[0]))

    assert(schema['id'].isA(SimpleItem))
    assert(schema['application'].isA(ComponentItem))
    assert(schema['inputbox'].isA(ComponentItem))
    assert(schema['inputbox'].isA(FileItem))

    assert(schema['id']['protected'])
    assert(not schema['id']['comparable'])
    assert(schema['id']['type'] == 'string')

    logger.info(schema['application']['category'] + ' ' + schema['application']['defvalue'])

    schema2 = copy.deepcopy(schema)

    assert(schema2 is not schema)
    assert(schema.datadict is not schema2.datadict)
    assert(schema._pluginclass is schema2._pluginclass)

    for i in schema.allItems():
        assert(schema.getItem(i[0]) is not schema2.getItem(i[0]))

    # check the implied rules

    assert(schema['overriden_copyable']['copyable'] == 1)
    assert(schema['plain_copyable']['copyable'] == 0)
    assert(schema['id']['copyable'] == 0)
    assert(schema['application']['copyable'] == 1)

    logger.info('Schema tested OK.')


#
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2008/09/09 14:37:16  moscicki
# bugfix #40220: Ensure that default values satisfy the declared types in the schema
#
# factored out type checking into schema module, fixed a number of wrongly declared schema items in the core
#
# Revision 1.1  2008/07/17 16:40:55  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.15.26.8  2008/05/15 06:31:38  moscicki
# # bugfix 36398: allow to assign a list in the configuration
#
# Revision 1.15.26.7  2008/04/18 13:42:18  moscicki
# remove obsolete printout
#
# Revision 1.15.26.6  2008/04/18 10:52:13  moscicki
# 1) typechecking fix
# 2) Ganga/test/GPI/ConfigSetEmptyVOMSString.gpi fix
#
# Revision 1.15.26.5  2008/04/18 08:14:38  moscicki
# bugfix 18272 (reintroduced in Ganga 5): add typelist information to the configuration option
#
# Revision 1.15.26.4  2007/12/18 09:07:06  moscicki
# integrated typesystem from Alvin
#
# Revision 1.15.26.3  2007/11/07 17:02:13  moscicki
# merged against Ganga-4-4-0-dev-branch-kuba-slices with a lot of manual merging
#
# Revision 1.15.26.2  2007/11/07 15:10:03  moscicki
# merged in pretty print and GangaList support from ganga-5-dev-branch-4-4-1-will-print branch
#
#
# Revision 1.15.26.1  2007/10/12 13:56:24  moscicki
# merged with the new configuration subsystem
#
# Revision 1.15.28.1  2007/09/25 09:45:11  moscicki
# merged from old config branch
#
# Revision 1.15.6.1  2007/06/18 07:44:55  moscicki
# config prototype
#
# Revision 1.15.30.1  2007/10/30 12:12:08  wreece
# First version of the new print_summary functionality. Lots of changes, but some known limitations. Will address in next version.
#
# Revision 1.15.8.1  2007/06/18 10:16:36  moscicki
# slices prototype
#
# Revision 1.15  2007/03/05 12:04:18  moscicki
# explicit switch for strict_sequence (default is True), if the sequence is non-strict then a single value v will be converted to [v] on assignment, for example non-strict File sequence yields obj.x = 'a' <=> obj.x = [File('a')]  <=> obj.x = File('a')
#
# Revision 1.14  2006/10/02 13:11:18  moscicki
# added extra check when setting default values: try to apply the value and raise ConfigError if it fails (for example unknown plugin)
#
# Revision 1.13  2006/07/28 12:53:36  moscicki
# fixed default value for ComponentItem (was broken)
#
# Revision 1.12  2006/07/28 08:26:23  moscicki
# allow defvalue to be an object for component items (not only a string or None)
#
# Revision 1.11  2006/07/27 20:15:35  moscicki
# createDefaultConfig()
# getDefaultValue()
# "checkset" metaproperty
#
# Revision 1.10  2005/12/02 15:35:06  moscicki
# visitable and getter metaproperties
#
# Revision 1.9  2005/08/24 15:42:21  moscicki
# automatically generated help for properties, disabled the SchemaHelper and few other improvements to the help system
#
#
#
