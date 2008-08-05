from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject
from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

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
        logger.error('The maximum_cache_age set in the LHCb section of the Ganga config is not valid')
    return maximum_cache_age

# LHCbDataset is a simple list of files (LFNs)

class LHCbDataset(Dataset):
    _schema = Schema(Version(2,1), {
        'files':ComponentItem(category='datafiles',defvalue=[],sequence=1),
        'cache_date': SimpleItem(defvalue = '', doc = 'The date the last full cache update was run.'),
        'new_cache' : SimpleItem(defvalue = True, doc = 'True when the cache has never been updated before',hidden = 1)
        })
    _category = 'datasets'
    _name = "LHCbDataset"
    _exportmethods = ['updateReplicaCache','__len__','cacheOutOfDate']

    def __init__(self,files=[]):
        super(LHCbDataset, self).__init__()

    def __len__(self):
        """The number of files in the dataset."""
        result = 0
        if self.files:
            result = len(self.files)
        return result

    def __nonzero__(self):
        """Returns the logical value of a dataset. This is always True, as with an object."""
        return True

    def cacheOutOfDate(self, maximum_cache_age = None):
        """Checks that the Dataset was updated less than maximum_cache_age minutes ago.

        If cache_expiry is None, then the value is taken from the LHCb section of the
        Ganga config. Returns True if the cache is invalid, and False otherwise. Zero or
        negative values of maximum_age_cache will cause the cache to be invalid."""

        result = True
        if not self.cache_date:
            return result

        import time
        cache_time = time.mktime(time.strptime(self.cache_date))

        time_now = time.time()

        #cache time should be in the past
        time_diff = time_now - cache_time

        if time_diff < 0:
            logger.warning('The cache_date is in the future and will be treated as unreliable.')
            return result

        if maximum_cache_age == None:
            #get from config
            maximum_cache_age = getCacheAge()

        if maximum_cache_age <= 0:
            logger.warning('Invalid maximum_cache_age set - %d. This value must be greater than 0', maximum_cache_age)
            return result
        
        if ((time_diff//60) - maximum_cache_age) < 0:
            result = False

        return result
        

    class MigrationLHCbDataset10(Dataset):
        """This is a migration class for Athena Job Handler with schema version 1.2.
        There is no need to implement any methods in this class, because they will not be used.
        However, the class may have "getMigrationClass" and "getMigrationObject" class 
        methods, so that a chain of convertions can be applied."""
        _schema = Schema(Version(1,0), {'files':FileItem(defvalue=[],sequence=1)})
        _category = 'application_converters' # put this class in different category
        _name = 'MigrationLHCbDataset10'

    def getMigrationClass(cls, version):
        """This class method returns a (stub) class compatible with the schema <version>.
        Alternatively, it may return a (stub) class with a schema more recent than schema <version>,
        but in this case the returned class must have "getMigrationClass" and "getMigrationObject"
        methods implemented, so that a chain of convertions can be applied."""
        return cls.MigrationLHCbDataset10

    getMigrationClass = classmethod(getMigrationClass)

    def getMigrationObject(cls, obj):
        """This method takes as input an object of the class returned by the "getMigrationClass" method,
        performs object transformation and returns migrated object of this class (cls)."""


        ############## Migrator function. Migrate from old to new schema
        # check that obj has shema supported for migration
        version = obj._schema.version
        old_cls = cls.getMigrationClass(version)
        if old_cls: # obj can be converted
            converted_obj = cls()
            for attr, item in obj._schema.allItems():
            #for attr, item in converted_obj._schema.allItems():
                # specific convertion stuff
                if (attr, item) in obj._schema.allItems():
                #if attr == 'files':
                    ### make sure to convert from fileobject to datasetitem object
                    tmp=[]
                    for i in obj.files:
                        lhcb = LHCbDataFile()
                        lhcb.name = i.name
                        tmp.append(lhcb)
                    converted_obj.files=tmp[:]    

                    #setattr(converted_obj, attr, [getattr(obj, attr)]) # correction: []
                else:
                    setattr(converted_obj, attr, getattr(obj, attr))
            return converted_obj
    getMigrationObject = classmethod(getMigrationObject)

    def isEmpty(self):
        return bool(self.files)
    
    def updateReplicaCache(self, forceUpdate = False):
        """Updates the cache of replicas

        If forceUpdate is True then all lfns are updated,
        otherwise only those that don't contain replica info.

        If we are doing a full update then the cache_date will
        be updated.
        """
        lfns=[]
        for f in self.files:
            if f.isLFN():
                #update only if none cached or force
                if not f.replicas or forceUpdate:
                    lfns.append(f._stripFileName())
        

        # Execute LFC command in separate process as
        # it needs special environment.
        import tempfile,os
        from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper
        (handle,fname) = tempfile.mkstemp(text=True)
        os.close(handle)
        command=\
                  'result = dirac.bulkReplicas('+repr(lfns)+')\n'+\
                  'file = open('+repr(fname)+',"w")\n'+\
                  'file.write(repr(result))\n'+\
                  'file.close()\n'

        rc=diracwrapper(command)
        file = open(fname)
        result=eval(file.readline())
        file.close()
        if rc==0: os.unlink(fname)

        if result['Status'] != 'OK':
            logger.warning('The LFC query did not return cleanly. '\
                           'Some of the replica information may be missing.')
        replicas=result['Replicas']

        
        logger.debug('Replica information received is: '+repr(replicas))
        for f in self.files:
            f_name = f._stripFileName()
            if replicas.has_key(f_name):
                f.replicas = replicas[f_name].keys()

        if forceUpdate or self.new_cache:
            #allows the cache to be invalidated
            import time
            self.cache_date = time.asctime()
            self.new_cache = False


class LHCbDataFile(GangaObject):
    _schema = Schema(Version(1,0),{'name':SimpleItem(defvalue='',doc='''name of the LHCb data file. A string'''),
                                   'replicas':SimpleItem(defvalue=[],sequence=1,doc='''Cached replicas of the datafile''', typelist= ['str'])})

    _category='datafiles'
    _name='LHCbDataFile'
    _exportmethods = ['updateReplicaCache']

    def __init__(self):
        super(LHCbDataFile,self).__init__()
        
    def updateReplicaCache(self):
        """Updates the cache of replicas"""
        if self.isLFN():

            # Execute LFC command in separate process as
            # it needs special environment.
            import tempfile,os
            from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper
            (handle,fname) = tempfile.mkstemp(text=True)
            os.close(handle)
            name=self._stripFileName()
            command=\
                      'result = dirac.lfcreplicas('+repr(name)+')\n'+\
                      'file = open('+repr(fname)+',"w")\n'+\
                      'file.write(repr(result))\n'+\
                      'file.close()\n'

            rc=diracwrapper(command)
            file = open(fname)
            result=eval(file.readline())
            file.close()
            if rc==0:
                os.unlink(fname)

            rep = result.get('lfn: '+self._stripFileName(),[])
            if rep and rep[0] != 'Replicas:  No such file or directory':
                result = []
                for r in rep:
                    result.append(r.split(' ')[0])
                rep = result
            self.replicas = rep
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
#
# $Log: not supported by cvs2svn $
# Revision 1.15.6.8  2008/05/22 16:36:40  wreece
# fixs truth of LHCbDataset
#
# Revision 1.15.6.7  2008/05/07 14:31:28  uegede
# Merged from trunk
#
# Revision 1.24  2008/05/02 10:16:35  uegede
# Fixed typo in new implementation of LHCbDatafile.updateReplicaCache()
#
# Revision 1.23  2008/05/01 15:47:46  uegede
# - Removed forced 32 bit running
# - Took away modifications to LD_LIBRARY_PATH
# - Introduced "diracwrapper"to execute DIRAC commands in separate
#   process with a different environment. Updates to LHCbDataset and
#   Dirac backend to make use of this feature.
#
# Revision 1.15.6.6  2008/03/17 11:08:28  andrew
# Merge from head
#
# Revision 1.15.6.5  2008/03/03 17:13:02  wreece
# Updates the LHCbDataset to respect the cache, and adds a test for this.
# Revision 1.21  2008/03/07 16:19:47  andrew
# Fix for bug #21546: Force use of DiracTopDir, even when LHCBPRODROOT is
#       defined
#
# Revision 1.20  2008/01/15 16:02:15  wreece
# Savannah 28228. Updates the migration code.
#
# Revision 1.19  2007/12/04 14:31:37  andrew
# Chenge the GridShell import
#
# Revision 1.18  2007/11/13 16:57:58  andrew
# fixed typo
#
# Revision 1.17  2007/11/13 14:46:42  andrew
# Merges from head
# protection if Grid UI is not installed (#30894)
# Protection if Dirac has not been imported properly
#
# Revision 1.16  2007/10/19 11:32:30  wreece
# Makes an LHCbDataset always logically true, like an object. Adds test for this.
#
# Revision 1.15  2007/07/30 13:30:24  wreece
# Adds 7 tests for the LHCbDataset, and a method on the dataset which allows
# the user to see whether the cache is out of date or not.
#
# Revision 1.14  2007/07/30 10:59:03  wreece
# Makes the LHCbDataset have a __len__ method, and updates a test to use it.
#
# Revision 1.13  2007/07/25 07:50:32  andrew
# merged with changes from Will
#
# Revision 1.12  2007/07/23 16:28:48  wreece
# Changes the LHCbDataFile's updateReplicas method to only store site info. Also fixes a few things with PFN's
#
# Revision 1.11  2007/07/23 15:53:28  wreece
# Changes the dataset to store only the site names rather than the full replica
# info. Should cut the memory footprint somewhat.
#
# Revision 1.10  2007/06/14 15:31:20  andrew
# Fixed the LHCbDataFile constructor from Mordor
#
# added a shortcut for LHCbDatasets to do ds=LHCbDataset['lfn1','lfn2'...]
#
# Revision 1.9  2007/06/11 08:43:59  andrew
# Fix for contructor in LHCbDataFile (from Will)
#
# Revision 1.8  2007/05/30 14:08:15  andrew
# Added schema migration
#
# Revision 1.7  2007/04/25 16:11:02  wreece
# Adds a check for the bulk replicas and fixes a bug in the LHCbDataFile cache
#
# Revision 1.6  2007/04/25 11:36:05  wreece
# Makes sure that the cache_date is written when the dataset is made for the first time.
#
# Revision 1.5  2007/04/25 09:52:19  wreece
# Merge from development branch to support DiracSplitter.
#
# Revision 1.4.2.2  2007/04/25 09:46:40  wreece
# Adds a cache date and a force flag (and some docs...)
#
# Revision 1.4  2007/04/18 13:33:56  andrew
# New dataset stuff
#
# Revision 1.3  2007/04/13 12:38:15  andrew
# new LHCbDataset model, with replica support
#
# Revision 1.2  2007/03/12 09:41:35  andrew
# Added an LHCbDataFile class
#
#
#
