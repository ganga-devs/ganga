#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

''' Splitter for DIRAC jobs. '''

__date__ = "$Date: 2009-02-05 09:28:05 $"
__revision__ = "$Revision: 1.2 $"

import string
from GangaLHCb.Lib.LHCbDataset import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from Ganga.Utility.util import unique 
import Ganga.Utility.logging
from GangaLHCb.Lib.Gaudi.Splitters import SplitByFiles

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracSplitter(SplitByFiles):
    """Query the LFC, via Dirac, to find optimal data file grouping.

    This Splitter will query the Logical File Catalog (LFC) to find
    at which sites a particular file is stored. Subjobs will be created
    so that all the data required for each subjob is stored in
    at least one common location. This prevents the submission of jobs that
    are unrunnable due to data availability.

    Currently only Logical File Names (LFNs) are well supported. Data specified
    with a Physical File Name (PFN) will not be intelligently grouped,
    but will be split in a similar fashion to the SplitByFiles splitter. 

    The splitter will produce an error if the data file specified can not
    be found in the LFC. This can be overridden by setting the ignoremissing
    flag to True. In this case any data files not found in the LFC will be
    not be added to a subjob.

    Splitting using the DiracSplitter can be slow compared to SplitByFiles
    due to the time needed to query the LFC. An estimate of the query time
    is printed at the start of the query. This is based on using the lxplus
    cluster at CERN, and may not be reliable for other nodes.
    """
    _name = 'DiracSplitter'
    _schema = Schema(Version(1,0),{
        'filesPerJob' : SimpleItem(defvalue=10,
                                   doc='Number of files per subjob'),
        'maxFiles':SimpleItem(defvalue=-1,
                              doc='Maximum number of files to use in ' + \
                              'a masterjob. A value of "-1" means all files'),
        'ignoremissing' : SimpleItem(defvalue=False,
                                     doc='Skip LFNs if they are not found ' + \
                                     'in the LFC.')
        })
    
    def _splitFiles(self, inputs):
        splitter = _diracSplitter(self.filesPerJob,self.maxFiles,
                                  self.ignoremissing)
        return splitter.split(inputs)    

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class _locations(object):
    """Class for holding locations and calculating hashes"""
    
    def __init__(self,locations):
        locations = unique(locations)
        locations.sort()        
        self.locations = locations
        self.string = self._genString()

    def _genString(self, seperator='@'):
        """Make a string of the locations stored."""
        return string.join(self.locations, seperator)
    
    def __hash__(self):
        """Uses the __string member for hash"""
        return self.string.__hash__()
    
    def __eq__(self,other):
        """Uses the string member for equality"""
        return self.string.__eq__(other.string)
    
    def __repr__(self):
        return self.locations.__repr__()
    
    def __str__(self):
        return self.locations.__str__()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class _diracSplitter(object):

    DIRAC_ERROR_MESSAGE = 'Replicas:  No such file or directory'

    def __init__(self,filesPerJob,maxFiles,ignoremissing):
        self.filesPerJob = filesPerJob
        self.maxFiles = maxFiles
        self.ignoremissing = ignoremissing

    def _copyToDataSet(self, data, replica_map, date = '',\
                       depth = LHCbDataset._schema.getDefaultValue('depth')):

        dataSet = LHCbDataset()
        dataSet.cache_date = date
        dataSet.depth = depth
        for d in data:
            lhcbFile = string_datafile_shortcut(d.name,None)
            lhcbFile.replicas = replica_map[d.name]
            dataSet.files.append(lhcbFile)
        return dataSet

    def split(self,inputs, data_set=None):
        ###FIXME### This method is 200 lines long--split it up, no pun intended
        logger.debug('Starting Split using Dirac')
        bulk = {}
        
        if data_set is None:
            data_set = LHCbDataset()
            data_set.datatype_string = inputs.datatype_string
            data_set.depth = inputs.depth
            data_set.files = inputs.files

        if data_set.cacheOutOfDate():
            estimated_query_time = 0.05 * len(inputs.files)
            logger.info('Estimated time to query the LFC: %dm%ds',
                        (estimated_query_time // 60) ,
                        (estimated_query_time % 60))
        data_set.updateReplicaCache()
        cache_date = data_set.cache_date
        depth = data_set.depth

        #make a map of replicas
        bad_file_list = []
        for f in data_set.files:
            file_exists = True
            if not f.replicas or (len(f.replicas) and \
                                  f.replicas[0] == self.DIRAC_ERROR_MESSAGE):
                #we can't do much with empty replicas
                f.updateReplicaCache()#check the file in the lfc
                if len(f.replicas) and \
                       f.replicas[0] == self.DIRAC_ERROR_MESSAGE:
                    if not self.ignoremissing:
                        logger.error('The file %s can not be found in the ' + \
                                     'LFC. Check that the file name is ' + \
                                     'correct.',f.name)
                        raise SplittingError('File Not Found: %s' % f.name)
                    else:
                        file_exists = False
                        logger.warning('The file %s can not be found in ' + \
                                       'the LFC and will be ignored. ' + \
                                       'Check that the file name is correct.',
                                       f.name)
                        bad_file_list.append(f.name)
            if file_exists:
                bulk[f.name] = f.replicas
        
        # loop though the files and sort into lists of files that live on the
        # same set of storage elements.
        files = {}
        debug_dict = {}
        for i in inputs.files:
            name = i.name
            if not name in bad_file_list:
                loc = _locations(bulk[name])
                debug_dict[name] = loc
                logger.debug('Location for file %s is %s', name, str(loc))
                if loc in files:
                    files[loc].append(i)
                else:
                    files[loc] = [i]
        logger.debug('Dirac bulk query done and stored in map')
        logger.debug('There are %d unique location sets.',len(files.keys()))
        logger.debug('The unique locations are %s',str(files.keys()))

        #we now have a list of files to split up
        result = []
        merge_list = {}
        for k in files.keys():
            files_length = len(files[k])
            if files_length < self.filesPerJob:
                #we will have to try merging
                merge_list[k] = files[k][:]
                logger.debug('1: Adding %s to the merge list - site is %s.',
                             str(files[k]), str(k))
            else:
                end = 0
                for i in range(files_length // self.filesPerJob):
                    start = i * self.filesPerJob
                    end = start + self.filesPerJob
                    #add a sublist of files
                    result.append(self._copyToDataSet(files[k][start:end],
                                                      bulk,cache_date,depth))
                    logger.debug('Added a separate job with the files ' + \
                                 '%s - sites are %s.',str(files[k][start:end]),
                                 str(k))
                    logger.debug('start %d, end %d, length %d.', start, end,
                                 files_length)                    
                if end < (files_length):
                    #put any residual files in the mergelist
                    merge_list[k] = files[k][end:]
                    logger.debug('2: Added %s to the merge list - sites ' + \
                                 'are %s.',str(merge_list[k]), str(k))

        logger.debug('Before merging we have %d jobs', len(result))
        logger.debug('The merge list is %s', str(merge_list))
        
        # now we are ready to merge            
        for k in merge_list.keys():
            # don't bother if list is already merged
            if len(merge_list[k]) == 0:
                continue

            #make list of all other locations
            loc = merge_list.keys()
            loc.remove(k)

            #make map of how large the intersect is
            overlap = {}
            max = 0
            # loop other all locations
            for l in loc:
                logger.debug('Looking at %s.',l)
                count = 0
                for m in l.locations:
                    logger.debug('Searching for %s in %s', str(m),
                                 str(k.locations))
                    if m in k.locations: count += 1
                overlap[l] = count
                if count > max: max = count
            logger.debug('Maximum intersect is %d',max)
            logger.debug('overlap map for %s is %s', k ,overlap)

            # bail out if there is no overlap
            if max == 0: continue
            
            # now add the files in order of greatest intersects first
            added = []

            #first add files from merge_list
            msg = 'Length should be less than filesPerJob'
            assert len(merge_list[k]) < self.filesPerJob, msg
            added.append([])
            while len(merge_list[k]) > 0:
                added[-1].append(merge_list[k].pop())
            assert not k in overlap.keys(),'We must have removed k'

            common = k.locations
            for j in range(max,0,-1):
                for o in overlap.keys():
                    if len(merge_list[o]) == 0: continue
                    if overlap[o] >= j:
                        # check for overlap amongst all files
                        # savannah 27430
                        cached_common = common[:]
                        for c in common:
                            if c not in o.locations:
                                cached_common.remove(c)
                        if len(cached_common) == 0:
                            continue
                        logger.debug('Setting common to %s',str(cached_common))
                        common = cached_common
                        # add to results
                        logger.debug('Overlap is %d. merge_list is %s.',
                                     overlap[o],merge_list[o])
                        while len(merge_list[o]) > 0 and \
                                  len(added[-1]) < self.filesPerJob:
                            logger.debug('Appending from merge list.')
                            logger.debug('Job is in location %s which ' + \
                                         'should match %s',o,k)
                            added[-1].append(merge_list[o].pop())

            logger.debug('Merge list is now %s', merge_list)
            #append
            logger.debug('Adding the subjob %s', added)
            result.extend([self._copyToDataSet(a,bulk,cache_date,depth) \
                           for a in added])
            logger.debug('We now have %d subjobs.',len(result))

        #anything left over is unmergable and has to become a subjob on its own
        for k in merge_list.keys():
            if not len(merge_list[k]): continue
            
            assert(len(merge_list[k]) < self.filesPerJob)
            result.append(self._copyToDataSet(merge_list[k],bulk,cache_date,depth))

        unique_files = []
        for r in result:
            for f in r.files:
                logger.debug('%s which is at %s.', str(f.name),
                             str(debug_dict[f.name]))
                unique_files.append(f)
        
        if (len(inputs.files)-len(bad_file_list)) != len(unique(unique_files)):
            raise SplittingError('Data files have been lost during ' + \
                                 'splitting. Please submit a bug report to' + \
                                 ' the Ganga team.')
        return result            

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
