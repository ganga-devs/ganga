#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Splitters for Gaudi applications in LHCb.'''

__date__ = "$Date: 2008-11-13 10:02:53 $"
__revision__ = "$Revision: 1.9 $"

from __future__ import division

import time
import string
from Ganga.Core import ApplicationConfigurationError
from GangaLHCb.Lib.LHCbDataset import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import  File
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.ISplitter import ISplitter, SplittingError
from Ganga.Utility.util import unique 
import Ganga.Utility.logging
from GaudiUtils import dataset_to_options_string

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class SplitByFiles(ISplitter):
    """Splits a job into sub-jobs by partitioning the input data

    SplitByFiles can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitByFiles'
    _schema = Schema(Version(1,0),{
        'filesPerJob' : SimpleItem(defvalue=10,
                                   doc='Number of files per subjob'),
        'maxFiles':SimpleItem(defvalue=-1,
                              doc='Maximum number of files to use in ' + \
                              'a masterjob. A value of "-1" means all files')
        })

    def _splitFiles(self, inputs):
        splitter = _simpleSplitter(self.filesPerJob,self.maxFiles)
        return splitter.split(inputs)    

    def split(self,job):
        if self.filesPerJob < 1:
            logger.error('filesPerJob must be greater than 0.')
            raise SplittingError('filesPerJob < 1 : %d' % self.filesPerJob)

        subjobs=[]
        self._extra = job.application.extra
        inputs = LHCbDataset()
        inputs.datatype_string=self._extra.inputdata.datatype_string
        if int(self.maxFiles) == -1:
            inputs.files=self._extra.inputdata.files[:]
            logger.info("Using all %d input files for splitting" % len(inputs))
        else:
            inputs.files=self._extra.inputdata.files[:self.maxFiles]
            logger.info("Only using a maximum of %d inputfiles"
                        % int(self.maxFiles))
        
        #store names to add cache info later
        dataset_files = {}
        for i in self._extra.inputdata.files:
            dataset_files[i.name] = i

        datasetlist = self._splitFiles(inputs)
        cache_date = self._extra.inputdata.cache_date
        if cache_date:
            _time = time.mktime(time.strptime(cache_date))
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
            j.application.extra.dataopts = self.subjobsDiffOpts(dataset,
                                                                len(subjobs)+1)
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
            t = time.localtime(_time)
            self._extra.inputdata.cache_date = time.asctime(t)
        return subjobs
    
    def subjobsDiffOpts(self,dataset,i):
        s='\n////Data created for subjob %d\n' % (i-1)
        s += dataset_to_options_string(dataset)
        return s


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class _simpleSplitter(object):

    def __init__(self,filesPerJob,maxFiles):
        self.filesPerJob = filesPerJob
        self.maxFiles = maxFiles

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
            msg = 'Data files have been lost during splitting. Please ' + \
                  'submit a bug report to the Ganga team.'
            raise SplittingError(msg)
        
        return result    

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class OptionsFileSplitter(ISplitter):
    '''Split a jobs based on a list of option file fragments
    
    This Splitter takes as argument a list of option file statements and will
    generate a job for each item in this list. The value of the indevidual list
    item will be appended to the master options file. A use case of this
    splitter would be to change a parameter in an algorithm (e.g. a cut) and to
    recreate a set of jobs with different cuts
    '''
    _name = "OptionsFileSplitter"
    _schema =Schema(Version(1,0),{
            'optsArray': SimpleItem(defvalue=[],
                                    doc="The list of option file strings. " + \
                                    "Each list item creates a new subjob")
            })

    def split(self,job):
        
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
            if job.inputdata:
                j.application.extra.inputdata = [x.name for \
                                                 x in job.inputdata.files]
            j.application.extra._userdlls=job.application.extra._userdlls[:]
            # GC: need to deal with this dataopts
            j.application.extra.dataopts+=i
            subjobs.append(j)
        return subjobs


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaussSplitter(ISplitter):
    """Create a set of Gauss jobs based on the total number of jobs and the
    number of events per subjob.
    
    This Splitter will create a set of Gauss jobs using two parameters:
    'eventsPerJob' and 'numberOfJobs'. Each job uses a different random seed
    using the Gaudi options file statement 'GaussGen.FirstEventNumber' and will
    produce the amount of events sepcified in 'eventsPerJob'. The total number
    of generated events therefore will be 'eventsPerJob*numberOfJob'.
    """
    _name = "GaussSplitter"
    _schema =Schema(Version(1,0),{
            'eventsPerJob': SimpleItem(defvalue=5,doc='Number of ' + \
                                       'generated events per job'),
            'numberOfJobs': SimpleItem(defvalue=2,
                                       doc="Number of jobs to create")
            })

    def split(self,job):

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
            if job.inputdata:
                j.application.extra.inputdata=[x.name for \
                                               x in job.inputdata.files]
            j.application.extra._userdlls=job.application.extra._userdlls[:]
            firstEvent=i*self.eventsPerJob+1
            dataopts = 'ApplicationMgr.EvtMax = %d;\n' % self.eventsPerJob
            dataopts += 'GaussGen.FirstEventNumber = %d;\n' % firstEvent
            # for when we move to .py only option files
            #dataopts = 'ApplicationMgr(EvtMax=%d)\n' % self.eventsPerJob
            #dataopts += 'GenInit(\"GaussGen\").FirstEventNumber = %d\n' \
            #            % firstEvent
            j.application.extra.dataopts  = dataopts
            logger.debug("Creating job "+ str(i) + \
                         " with FirstEventNumber = " + str(firstEvent))
            subjobs.append(j)
        return subjobs


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
