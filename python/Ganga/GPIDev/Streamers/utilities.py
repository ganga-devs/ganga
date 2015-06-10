################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: utilities.py,v 1.1 2008-07-17 16:40:56 moscicki Exp $
################################################################################

from Ganga.Utility.Plugin import PluginManagerError, allPlugins
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
from Ganga.GPIDev.Lib.GangaList.GangaList import stripGangaList
from Ganga.GPIDev.Lib.GangaList.GangaList import makeGangaList

import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

from Ganga.Utility.external.ordereddict import oDict
allConverters = oDict()

################################################################################
# helper to create a dictionary of simpleattributes
# according to the schema from a ganga object from
def serialize(obj):
    """returns a (nested) dictionary of simple job attributes"""
    schema = obj._schema
    attrDict = {}
    attrDict['name']     = schema.name
    attrDict['category'] = schema.category
    attrDict['version']  = (schema.version.major, schema.version.minor)
    attrDict['simple']   = 0
    attrDict['data']     = {}
    def mapper(val):
        if isinstance(val, GangaObject):
            return serialize(val)
        else:
            for c in allConverters:
                logger.debug('serialize: trying to apply %s converter',c)
                vc = allConverters[c].serialize(val)
                if vc:
                    val = vc
                    break

            simpleDict = {}
            simpleDict['simple'] = 1
            simpleDict['data']   = val
            return simpleDict
    for attr, item in schema.allItems():
        if not item['transient']:
            val = getattr(obj, attr)
            if item['sequence']:
                val = map(mapper, stripGangaList(val))
            else:
                val = mapper(val)
            attrDict['data'][attr] = val
    return attrDict
        
################################################################################
# Exception raised by the GangaObjectFactory
class GangaObjectFactoryError(Exception):
    """
    Exception raised by the GangaObjectFactory
    """
    def __init__(self, e = None, msg = None):
        if msg == None:
            msg = "GangaObjectFactoryError: see self.e for more info"
        Exception.__init__(self, msg)    
        self.e = e 


################################################################################
# Empty Ganga Object
class EmptyGangaObject(GangaObject):
    """Empty Ganga Object. Is used to construct incomplete jobs"""
    _schema = Schema(Version(0,0), {})
    _name   = "Unknown"
    _category = "unknownObjects"
    _hidden   = 1

################################################################################
# helper to create a ganga object from dictionary of attributes
# according to the schema
def gangaObjectFactory(attrDict, migration_class = None):
    ## gangaObjectFactory(...) --> (object, migrated, [<list of errors>])
    migrated = [False]
    errors = []
    if attrDict['simple']:
        return (None, migrated[0], errors)
    if migration_class:
        cls = migration_class
    else:
        try:
            cls = allPlugins.find(attrDict['category'], attrDict['name'])
        except PluginManagerError as e:
            msg = "Plugin Manager Error: %s" % str(e)
            errors.append(GangaObjectFactoryError(e, msg = msg))
            return (EmptyGangaObject(), migrated[0], errors)
    
    schema = cls._schema
    major, minor = attrDict['version']
    version = Version(major, minor)

    if not schema.version.isCompatible(version):  
        v1 = '.'.join(map(str, [major, minor]))
        v2 = '.'.join(map(str, [schema.version.major, schema.version.minor]))
        msg = "Incompatible schema versions of plugin %s in the category %s. Current version %s. Repository version %s." % (attrDict['name'], attrDict['category'],v2, v1)                             
        if schema.version.major > version.major: #no forward migration
            from MigrationControl import migration
            if migration.isAllowed(attrDict['category'], attrDict['name'], attrDict['version'], msg = msg):
                # try if migration provided by the plugin class 
                try:
                    old_cls = cls.getMigrationClass(version)
                except:
                    old_cls = None
                if old_cls:
                    old_obj, old_migrated, old_errors = gangaObjectFactory(attrDict, migration_class = old_cls)
                    if old_migrated:
                        migrated[0] = old_migrated                
                    if not old_errors:
                        try:
                            obj = cls.getMigrationObject(old_obj)
                            #assert(isinstance(obj, cls))
                        except Exception as e:
                            msg += ' Error in object migration: ' + str(e) 
                        else:
                            obj.__setstate__(obj.__dict__)
                            migrated[0] = True
                            return (obj, migrated[0], errors)
                    else:
                        msg += ' Errors in object migration ' + str(map(str, old_errors))
                else:
                    msg += ' No migration class is provided.'
            else:
                msg += ' Migration was denied in the migration control object.'
        errors.append(GangaObjectFactoryError(msg = msg))
        return (EmptyGangaObject(), migrated[0], errors)
    
    obj  = super(cls, cls).__new__(cls)
    obj._data = {}

    def mapper(attrDict):
        if attrDict['simple']:
            val = attrDict['data']
        else:
            val, attr_migrated, attr_errors = gangaObjectFactory(attrDict)
            if attr_migrated:
                migrated[0] = attr_migrated
            for err in attr_errors:
                if str(err) not in map(str, errors):
                    # don't duplicate the same errors
                    errors.append(err)
        return val
    
    data = attrDict['data']
    for attr, item in schema.allItems():
        if attr in data:
            val = data[attr]
            if item['sequence']:
                val = makeGangaList(val, mapper, parent = obj)
            else:                
                val = mapper(val)
        else:
            val = schema.getDefaultValue(attr)
        obj._data[attr] = val
    obj.__setstate__(obj.__dict__)
    return (obj, migrated[0], errors)

