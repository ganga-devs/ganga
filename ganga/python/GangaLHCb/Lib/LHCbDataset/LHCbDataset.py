#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper

from GangaLHCb.Lib.Dirac import DiracShared

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def getCacheAge():

    maximum_cache_age = 10080 #a week in minutes
    try:
        config = getConfig('LHCb')
        age = int(config['maximum_cache_age'])
        if age and age >= 1:
            maximum_cache_age = age
    except ConfigError:
        pass
    except ValueError:
        logger.error('The maximum_cache_age set in the LHCb section of the ' \
                     'Ganga config is not valid')
    return maximum_cache_age

def replicaCache(names):
    # Execute LFC command in separate process as it needs special environment.
    command = """
result = dirac.getReplicas(%s)
if not result.get('OK',False): rc = -1
storeResult(result)
""" % names
    dw = diracwrapper(command)
    result = dw.getOutput()

    if dw.returnCode != 0 or result is None or \
           (result is not None and not result['OK']):
        logger.warning('The LFC query did not return cleanly. '\
                       'Some of the replica information may be missing.')
    if result is not None and result.has_key('Message'):
        logger.warning("Message from Dirac3 was '%s'" % result['Message'])
            
    if (result is not None and not result.has_key('Value')):
        result = None

    return result

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# LHCbDataset is a simple list of files (LFNs)

class LHCbDataset(Dataset):

    schema = {}
    schema['files'] = ComponentItem(category='datafiles',defvalue=[],
                                    sequence=1)
    docstr = 'The date the last full cache update was run.'
    schema['cache_date'] = SimpleItem(defvalue='', doc=docstr)
    docstr = 'True when the cache has never been updated before'
    schema['new_cache'] = SimpleItem(defvalue=True, doc=docstr , hidden=1)
    defvaluestr = """TYP='POOL_ROOTTREE' OPT='READ'"""
    docstr = 'The string that is added after the filename in the options ' + \
             'to tell Gaudi how to read the data. If reading raw data ' + \
             '(mdf files) it should be set to "SVC=\'LHCb::MDFSelector\'"'
    schema['datatype_string'] = SimpleItem(defvalue=defvaluestr, doc=docstr)

    _schema = Schema(Version(2,2), schema)
    _category = 'datasets'
    _name = "LHCbDataset"
    _exportmethods = ['updateReplicaCache','__len__','cacheOutOfDate',
                      'hasLFNs']

    def __init__(self, files=[]):
        super(LHCbDataset, self).__init__()

    def __len__(self):
        """The number of files in the dataset."""
        result = 0
        if self.files:
            result = len(self.files)
        return result

    def __nonzero__(self):
        """Returns the logical value of a dataset. This is always True, as with
        an object."""
        return True

    def cacheOutOfDate(self, maximum_cache_age=None):
        """Checks that the Dataset was updated less than maximum_cache_age
        minutes ago.

        If cache_expiry is None, then the value is taken from the LHCb section
        of the Ganga config. Returns True if the cache is invalid, and False
        otherwise. Zero or negative values of maximum_age_cache will cause the
        cache to be invalid."""

        result = True
        if not self.cache_date:
            return result

        import time
        cache_time = time.mktime(time.strptime(self.cache_date))

        time_now = time.time()

        #cache time should be in the past
        time_diff = time_now - cache_time

        if time_diff < 0:
            logger.warning('The cache_date is in the future and will be '\
                           'treated as unreliable.')
            return result

        if maximum_cache_age == None:
            #get from config
            maximum_cache_age = getCacheAge()

        if maximum_cache_age <= 0:
            msg = 'Invalid maximum_cache_age set - %d. This value must ' + \
                  'be greater than 0'
            logger.warning(msg, maximum_cache_age)
            return result
        
        if ((time_diff//60) - maximum_cache_age) < 0:
            result = False

        return result


    class MigrationLHCbDataset10(Dataset):
        '''This is a migration class for Athena Job Handler with schema version
        1.2. There is no need to implement any methods in this class, because
        they will not be used. However, the class may have
        "getMigrationClass" and "getMigrationObject" class methods, so that
        a chain of convertions can be applied.'''
        
        _schema = Schema(Version(1,0),
                         {'files':FileItem(defvalue=[],sequence=1)})
        # put this class in different category
        _category = 'application_converters' 
        _name = 'MigrationLHCbDataset10'

    def getMigrationClass(cls, version):
        '''This class method returns a (stub) class compatible with the
        schema <version>. Alternatively, it may return a (stub) class with a
        schema more recent than schema <version>, but in this case the returned
        class must have "getMigrationClass" and "getMigrationObject"
        methods implemented, so that a chain of convertions can be applied.'''
        return cls.MigrationLHCbDataset10

    getMigrationClass = classmethod(getMigrationClass)

    def getMigrationObject(cls, obj):
        '''This method takes as input an object of the class returned by the
        "getMigrationClass" method, performs object transformation and returns
        migrated object of this class (cls).'''

        # Migrator function. Migrate from old to new schema
        # check that obj has shema supported for migration
        version = obj._schema.version
        old_cls = cls.getMigrationClass(version)
        if old_cls: # obj can be converted
            converted_obj = cls()
            for attr, item in obj._schema.allItems():
                # specific convertion stuff
                if (attr, item) in obj._schema.allItems():
                    #make sure to convert from fileobject to datasetitem object
                    tmp=[]
                    for i in obj.files:
                        lhcb = LHCbDataFile()
                        lhcb.name = i.name
                        tmp.append(lhcb)
                    converted_obj.files=tmp[:]    
                else:
                    setattr(converted_obj, attr, getattr(obj, attr))
            return converted_obj
        
    getMigrationObject = classmethod(getMigrationObject)

    def isEmpty(self):
        return not bool(self.files)
    
    def updateReplicaCache(self, forceUpdate = False):
        """Updates the cache of replicas

        If forceUpdate is True then all lfns are updated,
        otherwise only those that don't contain replica info.

        If we are doing a full update then the cache_date will
        be updated.
        """

        #Savannah 40219
        if not forceUpdate and not self.cacheOutOfDate():
            return

        lfns=[]
        for f in self.files:
            if f.isLFN():
                #update only if none cached or force
                if not f.replicas or forceUpdate:
                    lfns.append(f._stripFileName())
        
        result = replicaCache(str(lfns))            
        if result is None: return #don't do anything more
        replicas = result['Value']['Successful']
        
        logger.debug('Replica information received is: ' + repr(replicas))
        for f in self.files:
            f_name = f._stripFileName()
            if replicas.has_key(f_name):
                f.replicas = replicas[f_name].keys()

        if forceUpdate or self.new_cache:
            #allows the cache to be invalidated
            import time
            self.cache_date = time.asctime()
            self.new_cache = False

    def hasLFNs(self):
        for f in self.files:
            if f.isLFN(): return True
        return False

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class LHCbDataFile(GangaObject):

    schema = {}
    schema['name'] = SimpleItem(defvalue='',
                                doc='name of the LHCb data file. A string')
    schema['replicas'] = SimpleItem(defvalue=[],sequence=1,
                                    doc='Cached replicas of the datafile',
                                    typelist= ['str'])
    
    _schema = Schema(Version(1,0),schema)

    _category='datafiles'
    _name='LHCbDataFile'
    _exportmethods = ['updateReplicaCache','isLFN']

    def __init__(self):
        super(LHCbDataFile,self).__init__()
        
    def updateReplicaCache(self):
        """Updates the cache of replicas"""

        if self.isLFN():
            result = replicaCache('\'%s\'' % self.name)
            if result is None: return # don't do anything more
            
            replicas = result['Value']['Successful']
            logger.debug('Replica information received is: ' + repr(replicas))
            name = self._stripFileName()
            if replicas.has_key(name):
                self.replicas = replicas[name].keys() 
        else:
            self.replicas = []

    def _stripFileName(self):
        lname = self.name.lower()
        if lname.startswith('lfn:'):
            return self.name[4:]
        return self.name

    def isLFN(self):
        lname = self.name.lower()
        if lname.startswith('lfn:'):
            return True
        else:
            return False

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base.Filters import allComponentFilters

def string_dataset_shortcut(v,item):
    if type(v) is type([]):
        l=[]
        
        for i in v:
            if type(i) is type(''):
               f=LHCbDataFile()
               f.name=i
               l.append(f)
        ds=LHCbDataset()
        ds.files=l[:]
        return ds       
    return None

def string_datafile_shortcut(v,item):
    if type(v) is type(''):
        f=LHCbDataFile()
        f.name=v
        return f
    return None

allComponentFilters['datafiles']=string_datafile_shortcut
allComponentFilters['datasets']=string_dataset_shortcut

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
