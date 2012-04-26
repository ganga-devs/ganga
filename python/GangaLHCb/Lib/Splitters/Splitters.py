from __future__ import division
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import time
import string
import copy
from tempfile import *
from Ganga.Core import ApplicationConfigurationError
from GangaLHCb.Lib.LHCbDataset import *
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import  File
from Ganga.GPIDev.Lib.File.FileBuffer import  FileBuffer
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.ISplitter import ISplitter, SplittingError
from Ganga.Utility.util import unique 
import Ganga.Utility.logging
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
from Francesc import GaudiExtras
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def copy_app(app):
    cp_app = app.__new__(type(app))
    cp_app.__init__()
    for name,item in cp_app._schema.allItems():
        if not item['copyable']:
            setattr(cp_app,name,cp_app._schema.getDefaultValue(name))
        else:
            c = copy.copy(getattr(app,name))
            setattr(cp_app,name,c)
##     if not hasattr(app,'extra'): return cp_app
##     cp_app.extra = GaudiExtras() 
##     cp_app.extra.input_buffers = app.extra.input_buffers.copy()
##     cp_app.extra.input_files = app.extra.input_files[:]
##     cp_app.extra.outputsandbox = app.extra.outputsandbox[:]
##     cp_app.extra.outputdata = app.extra.outputdata
    return cp_app 

def create_gaudi_subjob(job, inputdata):
    j = Job()
    j.name = job.name
    j.application = copy_app(job.application)
    j.backend = stripProxy(job.backend) # no need to deepcopy 
    if inputdata:
        j.inputdata = inputdata
##         if hasattr(j.application,'extra'):
##             j.application.extra.inputdata = j.inputdata
    #else:
##         j.inputdata = None
##         if hasattr(j.application,'extra'):
##             j.application.extra.inputdata = LHCbDataset()
    #j.inputsandbox = job.inputsandbox[:]#master input sandbox is added automatically.
    j.outputsandbox = job.outputsandbox[:]
    j.outputdata = job.outputdata
    return j

def simple_split(files_per_job, inputs):
    """Just splits the files in the order they came"""
    
    def create_subdataset(data_inputs,iter_begin,iter_end):
        dataset = LHCbDataset()
        dataset.depth = data_inputs.depth
        dataset.files = data_inputs.files[iter_begin:iter_end]
        return dataset

    result = []
    end = 0
    inputs_length = len(inputs.files)
        
    for i in range(inputs_length // files_per_job):
        start = i * files_per_job
        end = start + files_per_job
        result.append(create_subdataset(inputs,start,end))
            
    if end < (inputs_length):
        result.append(create_subdataset(inputs,end,None))
            
    #catch file loss
    result_length = 0
    for r in result: result_length += len(r.files)
    if result_length != inputs_length:
        raise SplittingError('Data files lost during splitting, please send '\
                             'a bug report to the Ganga team.')
        
    return result    

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class SplitByFiles(ISplitter):
    """Splits a job into sub-jobs by partitioning the input data

    SplitByFiles can be used to split a job into multiple subjobs, where
    each subjob gets an unique subset of the inputdata files.
    """
    _name = 'SplitByFiles'
    docstr = 'Maximum number of files to use in a masterjob (-1 = all files)'
    _schema = Schema(Version(1,0),{
        'filesPerJob' : SimpleItem(defvalue=10,
                                   doc='Number of files per subjob'),        
        'maxFiles' : SimpleItem(defvalue=-1, doc=docstr)})

    def _splitFiles(self,inputs):
        # don't let user use this if they're using the Dirac backend
        job = None 
        try:
            job = self.getJobObject()
        except:
            pass
        if job:
            if job.backend.__module__.find('Dirac') > 0:
                msg = 'SplitByFiles should not be used w/ the Dirac backend.'\
                      ' You probably want the DiracSplitter.'
                raise SplittingError(msg)

        return simple_split(self.filesPerJob,inputs)

    def split(self,job):
        if self.filesPerJob < 1:
            logger.error('filesPerJob must be greater than 0.')
            raise SplittingError('filesPerJob < 1 : %d' % self.filesPerJob)

        subjobs=[]

        inputdata = LHCbDataset()
        if job.inputdata:
            inputdata.files = job.inputdata.files
            inputdata.depth = job.inputdata.depth
        elif hasattr(job.application,"prep_inputdata"):
            inputdata.files = job.application.prep_inputdata.files
            inputdata.depth = job.application.prep_inputdata.depth

        inputs = LHCbDataset()
        inputs.depth = inputdata.depth
        if int(self.maxFiles) == -1:
            inputs.files = inputdata.files[:]
            logger.info("Using all %d input files for splitting" % len(inputs))
        else:
            inputs.files = inputdata.files[:self.maxFiles]
            logger.info("Only using a maximum of %d inputfiles"
                        % int(self.maxFiles))
        
        datasetlist = self._splitFiles(inputs)
        for dataset in datasetlist:
            subjobs.append(create_gaudi_subjob(job,dataset))
            
        return subjobs
    
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
    docstr = "List of option-file strings, each list item creates a new subjob"
    _schema =Schema(Version(1,0),
                    {'optsArray': SimpleItem(defvalue=[],doc=docstr)})

    def split(self,job):        
        subjobs=[]
        for i in self.optsArray:
            if job.application.prep_inputdata.files and not job.inputdata:
                j = create_gaudi_subjob(job, job.application.prep_inputdata)
            else:
                j = create_gaudi_subjob(job, job.inputdata)
            path = NamedTemporaryFile().name+'_data.py'
            j.inputsandbox +=[File(FileBuffer(path,i).create().name)]
            #j.application.extra.input_buffers['data.py'] += i
            #j.inputsandbox.append(File(FileBuffer(path,i).create().name))
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
            'eventsPerJob': SimpleItem(defvalue=5,doc='Number of '  \
                                       'generated events per job'),
            'numberOfJobs': SimpleItem(defvalue=2,doc="No. of jobs to create")
            })

    def split(self,job):
        subjobs=[]
        for i in range(self.numberOfJobs):
            if job.application.prep_inputdata.files and not job.inputdata:
                j = create_gaudi_subjob(job, job.application.prep_inputdata)
            else:
                j = create_gaudi_subjob(job, job.inputdata)
            first = i*self.eventsPerJob + 1
            opts = 'from Gaudi.Configuration import * \n'
            opts += 'from Configurables import GenInit \n'
            opts += 'ApplicationMgr().EvtMax = %d\n' % self.eventsPerJob
            #opts += 'from Configurables import LHCbApp\n'
            #opts += 'LHCbApp().EvtMax = %d\n' % self.eventsPerJob
            opts += 'GenInit("GaussGen").FirstEventNumber = %d\n' % first
            spillOver = ["GaussGenPrev","GaussGenPrevPrev","GaussGenNext"] 
            for s in spillOver : 
                opts += 'GenInit("%s").FirstEventNumber = %d\n' % (s,first) 
            #j.application.extra.input_buffers['data.py'] += opts
            path = NamedTemporaryFile().name+'_data.py'
            j.inputsandbox +=[File(FileBuffer(path,opts).create().name)]
            #j.inputsandbox.append(File(FileBuffer(path,opts).create().name))
            logger.debug("Creating job %d w/ FirstEventNumber = %d"%(i,first))
            subjobs.append(j)
            
        return subjobs

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
