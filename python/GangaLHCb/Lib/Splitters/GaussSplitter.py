from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
import pickle
import os
import copy

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

    def _create_subjob(self, job, inputdata):
        j=copy.deepcopy(job)
        j.splitter = None
        j.merger = None
        j.inputsandbox = [] ## master added automatically
        j.inputdata = inputdata

        return j


    def split(self,job):
        subjobs=[]


        inputdata = job.inputdata
        if not job.inputdata:
            share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                      'shared',
                                      getConfig('Configuration')['user'],
                                      job.application.is_prepared.name,
                                      'inputdata',
                                      'options_data.pkl')

            if os.path.exists(share_path):
                f=open(share_path,'r+b')
                inputdata = pickle.load(f)
                f.close()


        for i in range(self.numberOfJobs):
            j = self._create_subjob(job, inputdata)
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
            j._splitter_data = opts
            #j.inputsandbox.append(File(FileBuffer(path,opts).create().name))
            logger.debug("Creating job %d w/ FirstEventNumber = %d"%(i,first))
            subjobs.append(j)
            
        return subjobs
