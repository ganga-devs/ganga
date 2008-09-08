#######################################################################
#                                                                     #
#     Gaudi Splitters                                                 #
#                                                                     #
#######################################################################
from __future__ import division

from Ganga.GPIDev.Base import GangaObject

from Ganga.Core import ApplicationConfigurationError
from GangaLHCb.Lib.LHCbDataset import LHCbDataset,LHCbDataFile,string_dataset_shortcut,string_datafile_shortcut
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import  File
from Ganga.GPIDev.Adapters.ISplitter import ISplitter,SplittingError

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

#######################################################################
#                                                                     #
#     SplitByFiles                                                    #
#                                                                     #
#######################################################################

class SplitByFiles(ISplitter):
    """Splits a job into sub-jobs by partitioning the input data

    SplitByFiles can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitByFiles'
    _schema = Schema(Version(1,0),{
        'filesPerJob' : SimpleItem(defvalue=10, doc='Number of files per subjob'),
        'maxFiles':SimpleItem(defvalue=-1,doc='Maximum number of files to use in a masterjob. A value of "-1" means all files')
        })

    def _splitFiles(self, inputs):
        #split the files
        splitter = _simpleSplitter(self.filesPerJob,self.maxFiles)
        return splitter.split(inputs)    

    def split(self,job):
        if self.filesPerJob < 1:
            logger.error('filesPerJob must be greater than 0.')
            raise SplittingError('filesPerJob < 1 : %d' % self.filesPerJob)

        from Ganga.GPIDev.Lib.Job import Job
        subjobs=[]
        self.prepareSubjobs(job)
        inputs = LHCbDataset()
        inputs.datatype_string=self._extra.inputdata.datatype_string
        if int(self.maxFiles) == -1:
            inputs.files=self._extra.inputdata.files[:]
            logger.info("Using all %d input files for splitting" % len(inputs))
        else:
            inputs.files=self._extra.inputdata.files[:self.maxFiles]
            logger.info("Only using a maximum of %d inputfiles" %int(self.maxFiles))
        
        #store names to add cache info later
        dataset_files = {}
        for i in self._extra.inputdata.files:
            dataset_files[i.name] = i

        datasetlist = self._splitFiles(inputs)
        import time
        if self._extra.inputdata.cache_date:
            _time = time.mktime(time.strptime(self._extra.inputdata.cache_date))
        else:
            _time = time.time()*2
        _timeUpdate = False

        for dataset in datasetlist:

            j = self.createSubjob(job)
            j.application = job.application
            j.backend = job.backend

            #copy the dataset to the right place and configure
            j.inputdata = dataset
            j.application.extra.inputdata = dataset
            j.application.extra.dataopts = self.subjobsDiffOpts(dataset,len(subjobs)+1)
            j.application.extra._userdlls = job.application.extra._userdlls[:]
            j.outputsandbox = job.outputsandbox[:]
            subjobs.append( j)
            
            #copy the replicas back up the tree
            for f in dataset.files:
                dataset_files[f.name].replicas = f.replicas
            if dataset.cache_date:
                cache_time = time.mktime(time.strptime(dataset.cache_date))
                if cache_time < _time:
                    _time = cache_time
                    _timeUpdate = True
        if _timeUpdate:
            self._extra.inputdata.cache_date = time.asctime(time.localtime(_time))
        return subjobs

    def prepareSubjobs(self,job):
        #job.application.configure() #### FIXME: once the logic of job preparation is reversed, this line should disappear
        self._extra = job.application.extra
        return 
    
    def subjobsDiffOpts(self,dataset,i):
        # get the list of inputfiles
        # calculate the files to be returned
        # create a correct option file statemnet
        # return the option file statement
        s='\n////Data created for subjob %d\nEventSelector.Input   = {'% (i-1)
        for k in dataset.files:
            s+='\n'
            s+=""" "DATAFILE='%s' %s",""" % (k.name, dataset.datatype_string)

        #Delete the last , to be compatible with the new optiosn parser
        if s.endswith(","):
            logger.debug("_dataset2optsstring: removing trailing comma")
            s=s[:-1]

        s+="""\n};"""

        return s


class _abstractSplitter(object):
    """Abstract baseclass for splitters"""

    def __init__(self,filesPerJob,maxFiles):
        self.filesPerJob = filesPerJob
        self.maxFiles = maxFiles

    def split(self,inputs):
        raise NotImplementedError

class _simpleSplitter(_abstractSplitter):

    def split(self,inputs):
        """Just splits the files in the order they came"""
        result = []
        end = 0
        inputs_length = len(inputs.files)
        for i in range(inputs_length // self.filesPerJob):
            start = i * self.filesPerJob
            end = start + self.filesPerJob
            #add a sublist of files
            dataset = LHCbDataset()
            dataset.datatype_string=inputs.datatype_string
            dataset.files = inputs.files[start:end]
            dataset.cache_date = inputs.cache_date
            result.append(dataset)
        if end < (inputs_length):
            dataset = LHCbDataset()
            dataset.datatype_string=inputs.datatype_string
            dataset.files = inputs.files[end:]
            dataset.cache_date = inputs.cache_date
            result.append(dataset)
        #catch file loss
        result_length = 0
        for r in result:
            result_length += len(r.files)
        if result_length != inputs_length:
            raise SplittingError('Data files have been lost during splitting. Please submit a bug report to the Ganga team.')
        return result    

#######################################################################
#                                                                     #
#     DiracSplitter                                                   #
#                                                                     #
#######################################################################

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
        'filesPerJob' : SimpleItem(defvalue=10, doc='Number of files per subjob'),
        'maxFiles':SimpleItem(defvalue=-1,doc='Maximum number of files to use in a masterjob. A value of "-1" means all files'),
        'ignoremissing' : SimpleItem(defvalue = False, doc='Skip LFNs if they are not found in the LFC.')
        })
    
    def _splitFiles(self, inputs):
        #split the files
        splitter = _diracSplitter(self.filesPerJob,self.maxFiles,self.ignoremissing)
        return splitter.split(inputs)    

class _locations(object):
    """Class for holding locations and calculating hashes"""
    def __init__(self,locations):
        #unique does not preserve order
        from Ganga.Utility.util import unique
        locations = unique(locations)
        locations.sort()
        
        self.locations = locations
        #must be visible for eq,hash
        self.string = self._genString()

    def _genString(self, seperator='@'):
        """Make a string of the locations stored."""
        import string
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

class _diracSplitter(_abstractSplitter):

    DIRAC_ERROR_MESSAGE = 'Replicas:  No such file or directory'

    def __init__(self,filesPerJob,maxFiles,ignoremissing):
        super(_diracSplitter,self).__init__(filesPerJob,maxFiles)
        self.ignoremissing = ignoremissing

    def _copyToDataSet(self, data, replica_map, date = ''):

        dataSet = LHCbDataset()
        dataSet.cache_date = date
        for d in data:
            lhcbFile = string_datafile_shortcut(d.name,None)
            lhcbFile.replicas = replica_map[d.name]
            dataSet.files.append(lhcbFile)
        return dataSet

    def split(self,inputs, data_set = None):
        logger.debug('Starting Split using Dirac')

        bulk = {}
        if data_set is None:
            data_set = LHCbDataset()
            data_set.datatype_string=inputs.datatype_string
            data_set.files = inputs.files

        if data_set.cacheOutOfDate():
            #print an estimate of how long the query will take
            estimated_query_time = 0.05 * len(inputs.files)
            logger.info('Estimated time to query the LFC: %dm%ds',
                        (estimated_query_time // 60) , (estimated_query_time % 60))
        data_set.updateReplicaCache()
        cache_date = data_set.cache_date

        #make a map of replicas
        bad_file_list = []
        for f in data_set.files:

            file_exists = True
            if not f.replicas or (len(f.replicas) and f.replicas[0] == self.DIRAC_ERROR_MESSAGE):
                #we can't do much with empty replicas
                f.updateReplicaCache()#check the file in the lfc
                if len(f.replicas) and f.replicas[0] == self.DIRAC_ERROR_MESSAGE:
                    if not self.ignoremissing:
                        logger.error('The file %s can not be found in the LFC. '\
                                     'Check that the file name is correct.',f.name)
                        raise SplittingError('File Not Found: %s' % f.name)#get user to correct
                    else:
                        file_exists = False
                        logger.warning('The file %s can not be found in the LFC and will be ignored. '\
                                     'Check that the file name is correct.',f.name)
                        bad_file_list.append(f.name)
            #skip the file if it does not exist
            if file_exists:
                bulk[f.name] = f.replicas
        
        #loop though the files and sort into lists
        #of files that live on the same set of
        #storage elements 
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
                logger.debug('1: Adding %s to the merge list - site is %s.',str(files[k]), str(k))
            else:
                end = 0
                for i in range(files_length // self.filesPerJob):
                    start = i * self.filesPerJob
                    end = start + self.filesPerJob
                    #add a sublist of files
                    result.append(self._copyToDataSet(files[k][start:end],bulk,cache_date))
                    logger.debug('Added a separate job with the files %s - sites are %s.',str(files[k][start:end]), str(k))
                    logger.debug('start %d, end %d, length %d.', start, end, files_length)
                if end < (files_length):
                    #put any residual files in the mergelist
                    merge_list[k] = files[k][end:]
                    logger.debug('2: Added %s to the merge list - sites are %s.',str(merge_list[k]), str(k))

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
                    logger.debug('Searching for %s in %s', str(m), str(k.locations))
                    if m in k.locations: count += 1
                overlap[l] = count
                if count > max: max = count
            logger.debug('Maximum intersect is %d',max)
            logger.debug('overlap map for %s is %s', k ,overlap)

            # bail out if there is no overlap
            if max == 0: continue
            
            #now add the files in order of greatest
            #intersects first
            added = []

            #first add files from merge_list
            assert len(merge_list[k]) < self.filesPerJob, 'Length should be less than filesPerJob'
            added.append([])
            while len(merge_list[k]) > 0:
                added[-1].append(merge_list[k].pop())
            assert not k in overlap.keys(),'We must have removed k'

            common = k.locations
            for j in range(max,0,-1):
                for o in overlap.keys():
                    if len(merge_list[o]) == 0: continue
                    if overlap[o] >= j:
                        #check for overlap amongst all files
                        #savannah 27430
                        cached_common = common[:]
                        for c in common:
                            if c not in o.locations:
                                cached_common.remove(c)
                        if len(cached_common) == 0:
                            continue
                        logger.debug('Setting common to %s',str(cached_common))
                        common = cached_common
                        #add to results
                        logger.debug('Overlap is %d. merge_list is %s.',overlap[o],merge_list[o])
                        while len(merge_list[o]) > 0 and len(added[-1]) < self.filesPerJob:
                                logger.debug('Appending from merge list.')
                                logger.debug('Job is in location %s which should match %s',o,k)
                                added[-1].append(merge_list[o].pop())

            logger.debug('Merge list is now %s', merge_list)
            #append
            logger.debug('Adding the subjob %s', added)
            result.extend([self._copyToDataSet(a,bulk,cache_date) for a in added])
            logger.debug('We now have %d subjobs.',len(result))

        #anything left over is unmergable and has to become a subjob on its own    
        for k in merge_list.keys():
            if not len(merge_list[k]): continue
            
            assert(len(merge_list[k]) < self.filesPerJob)
            result.append(self._copyToDataSet(merge_list[k],bulk,cache_date))

        unique_files = []
        for r in result:
            for f in r.files:
                logger.debug('%s which is at %s.', str(f.name), str(debug_dict[f.name]))
                unique_files.append(f)
        
        from Ganga.Utility.util import unique 
        if (len(inputs.files) - len(bad_file_list)) != len(unique(unique_files)):
            raise SplittingError('Data files have been lost during splitting. Please submit a bug report to the Ganga team.')
        return result            

#######################################################################
#                                                                     #
#     Options File Splitter         
#                                                                     #
#######################################################################
class OptionsFileSplitter(ISplitter):
    '''Split a jobs based on a list of option file fragments
    
    This Splitter takes as argument a list of option file statements and will generate a job for each 
    item in this list. The value of the indevidual list item will be appended to the master options file.
    A use case of this splitter would be to change a parameter in an algorithm (e.g. a cut) and to recreate 
    a set of jobs with different cuts

    '''
    _name = "OptionsFileSplitter"
    _schema =Schema(Version(1,0),{
            'optsArray': SimpleItem(defvalue=[],doc="The list of option file strings. Each list item creates a new subjob")
            })

    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs=[]
        self._extra=job.application.extra
        job.application.extra.dataopts += "## Adding includes for subjobs\n"

        for i in self.optsArray:
            j=self.createSubjob(job)
            j.application=job.application
            j.backend=job.backend
            if job.inputdata:
                j.inputdata=job.inputdata[:]
            else:
                j.inputdata=None
            j.outputsandbox=job.outputsandbox[:]
            if job.inputdata: j.application.extra.inputdata=[x.name for x in job.inputdata.files]
            j.application.extra._userdlls=job.application.extra._userdlls[:]
            # GC: need to deal with this dataopts
            j.application.extra.dataopts+=i
            subjobs.append(j)
        return subjobs

#######################################################################
#                                                                     #
#     Gauss Splitter                                                  #
#                                                                     #
#######################################################################
class GaussSplitter(ISplitter):
    """Create a set of Gauss jobs based on the total number of jobs and the number of events per subjob
    
    This Splitter will create a set of Gauss jobs using two parameters: 'eventsPerJob' and 'numberOfJobs'.
    Each job uses a different random seed using the Gaudi options file statement 'GaussGen.FirstEventNumber'
    and will produce the amount of events sepcified in 'eventsPerJob'. The total number of generated events
    therefore will be 'eventsPerJob*numberOfJob'

    """
    _name = "GaussSplitter"
    _schema =Schema(Version(1,0),{
            'eventsPerJob': SimpleItem(defvalue=5,doc="Number of generated events per job"),
            'numberOfJobs': SimpleItem(defvalue=2,doc="Number of jobs to create")
            })

    def split(self,job):
        from Ganga.GPIDev.Lib.Job import Job
        subjobs=[]
        self._extra=job.application.extra
        job.application.extra.dataopts += '## Adding includes for subjobs\n'

        for i in range(self.numberOfJobs):
            j=self.createSubjob(job)
            j.application=job.application
            j.backend=job.backend
            if job.inputdata:
                j.inputdata=job.inputdata[:]
            else:
                j.inputdata=None
            j.outputsandbox=job.outputsandbox[:]
            if job.inputdata: j.application.extra.inputdata=[x.name for x in job.inputdata.files]
            j.application.extra._userdlls=job.application.extra._userdlls[:]
            firstEvent=i*self.eventsPerJob+1
            j.application.extra.dataopts  = 'ApplicationMgr.EvtMax = %d\n' % self.eventsPerJob
            j.application.extra.dataopts += 'GaussGen.FirstEventNumber = %d\n' % firstEvent
            logger.debug("Creating job "+ str(i) + " with FirstEventNumber = "+str(firstEvent))
            subjobs.append(j)
        return subjobs


#
#
# $Log: not supported by cvs2svn $
# Revision 1.8  2008/09/03 11:54:59  wreece
# Savannah 40910 - Also problem with datasets in options files
#
# Revision 1.7  2008/09/01 03:13:52  wreece
# fix for Savannah 40219 - Manages cache updating better.
#
# Revision 1.6  2008/08/22 10:07:24  uegede
# New features:
# =============
# The Gaudi and GaudiPython applications have a new attribute called
# 'setupProjectOptions'. It contains extra options to be passed onto the
# SetupProject command used for configuring the environment. As an
# example setting it to '--dev' will give access to the DEV area. For
# full documentation of the available options see
# https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject. The
# 'lhcb_release_area' attribute has been taken away as it was not useful.
#
# The Gaudi and GaudiPython applications can now read data from the
# detector. For this a new attribute, 'datatype_string', is added to the
# LHCbDataset. It contains the string that is added after the filename
# in the options to tell Gaudi how to read the data. If reading raw data
# (mdf files) it should be set to "SVC='LHCb::MDFSelector'".
#
# Minor changes:
# ==============
# The identification of which default application version to pick is now
# using SetupProject.
#
# Many test cases have been updated to Ganga 5.
#
# Revision 1.5  2008/08/15 15:52:21  uegede
# Changed the Dirac splitter to work with the modified LHCbDataset
#
# Revision 1.4  2008/08/14 15:54:43  uegede
# Added "datatype_string" to schema for LHCbDataset. This allows Ganga to run with
# cosmic data.
#
# Revision 1.3  2008/08/12 13:58:16  uegede
# Fixed gaudiPython to work with splitters
#
# Fixed bug in Gaudi handler causing projects not to be identified when
# masterpackage was used.
#
# Fixed bug in Gaudi handler when cmt_user_path included a ~.
#
# Took away some confusing debug statements in PythonOptionsParser
#
# Revision 1.2  2008/08/01 15:52:11  uegede
# Merged the new Gaudi application handler from branch
#
# Revision 1.1.2.1  2008/07/28 10:53:06  gcowan
# New Gaudi application handler to deal with python options. LSF and Dirac runtime handlers also updated. Old code removed.
#
# Revision 1.15.6.3.2.2  2008/07/14 19:08:38  gcowan
# Major update to PythonOptionsParser which now uses gaudirun.py to perform the complete options file flattening. Output flat_opts.opts file is made available and placed in input sandbox of jobs. LSF and Dirac handlers updated to cope with this new design. extraopts need to be in python. User can specify input .opts file and these will be converted to python in the flattening process.
#
# Revision 1.15.6.3.2.1  2008/07/03 12:52:07  gcowan
# Can now successfully submit and run Gaudi jobs using python job options to Local() and Condor() backends. Changes in Gaudi.py, GaudiLSFRunTimeHandler.py, PythonOptionsParser.py, Splitters.py and GaudiDiracRunTimeHandler.py. More substantial testing using alternative (and more complex) use cases required.
#
# Revision 1.15.6.3  2008/03/03 17:13:03  wreece
# Updates the LHCbDataset to respect the cache, and adds a test for this.
#
# Revision 1.15.6.2  2007/12/12 19:53:59  wreece
# Merge in changes from HEAD.
#
# Revision 1.18  2007/11/13 16:28:30  andrew
# updated Docs for OptionsSplitter
#
# Revision 1.17  2007/11/13 16:24:03  andrew
# Updated the documentation for the GaussSplitter
#
# Revision 1.16  2007/10/15 15:30:23  uegede
# Merge from Ganga_4-4-0-dev-branch-ulrik-dirac with new Dirac backend
#
# Revision 1.15.2.1  2007/09/07 15:08:38  uegede
# Dirac backend and runtime handlers updated to be controlled by a Python script.
# Gaudi jobs work with this as well now.
# Some problems with the use of absolute path in the DIRAC API are still unsolved.
# See workaround implemented in Dirac.py
#
# Revision 1.15  2007/07/31 10:38:04  uegede
# Fixed an faulty merge between previous updates.
#
# Revision 1.14  2007/07/17 10:36:49  wreece
# Fix for Savannah 27430. Makes sure that subjobs created from the
# mergelist have at least one common site.
#
# Revision 1.13  2007/07/17 09:57:51  uegede
# Fixed bug that caused SplitByFiles to enter all datasets into every single subjob and then fails
# at the end when checking the overall length.
#
# Revision 1.12  2007/06/28 19:24:42  andrew
# temporarily removed the code to parse .note files and to handle outputdata in DIrac
#
# Revision 1.11  2007/06/28 08:20:33  andrew
# Fix for bug 27531
#
# Revision 1.10  2007/06/13 13:18:51  andrew
# fix for bug 27151 (thanks Karl)
#
# Revision 1.9  2007/05/25 14:37:56  wreece
# Modifies the Splitters to return a list of LHCbDatasets rather than
# just strings. Reason is that replica info is propagated.
#
# Revision 1.7  2007/05/22 10:13:48  wreece
# Cleans up some of the debug info so that the split fails if we loose data
# files and puts in a time estimate for how long the lfc query will take.
#
# Revision 1.6  2007/04/26 11:57:31  wreece
# Adds a filesPerJob greater than 0 test to the SplitByFiles.
#
# Revision 1.3  2007/04/25 09:52:18  wreece
# Merge from development branch to support DiracSplitter.
#
# Revision 1.2.4.2  2007/04/25 09:48:03  wreece
# Refactors the SplitByFiles into two splitters, one of which supports Dirac and
# one which does not.
#
# Revision 1.2  2007/03/12 08:48:16  wreece
# Merge of the GangaLHCb-2-40 tag to head.
#
# Revision 1.1.2.1  2007/03/09 13:30:06  andrew
# Separate splitters
#
