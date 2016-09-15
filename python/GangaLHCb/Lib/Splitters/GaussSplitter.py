import pickle
import os
import copy
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
from Ganga.GPIDev.Lib.Job import Job
from Ganga.Utility.logging import getLogger
from GangaLHCb.Lib.Applications.GaudiExec import GaudiExec

logger = getLogger()

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
    _schema = Schema(Version(1, 0), {
        'eventsPerJob': SimpleItem(defvalue=5, doc='Number of generated events per job'),
        'numberOfJobs': SimpleItem(defvalue=2, doc="No. of jobs to create"),
        'firstEventNumber': SimpleItem(defvalue=0, doc="First event number for first subjob")
    })

    def _create_subjob(self, job, inputdata):
        j = Job()
        j.copyFrom(job)
        j.splitter = None
        j.merger = None
        j.inputsandbox = []  # master added automatically
        j.inputfiles = []
        j.inputdata = inputdata

        return j

    def split(self, job):
        """
            Method to do the splitting work
            Args:
                job (Job): master job to be used as a template to split subjobs
        """

        from GangaLHCb.Lib.Applications import Gauss
        if not isinstance(job.application, (Gauss, GaudiExec)):
            logger.warning("This application is of type: '%s', be careful how you use it with the GaussSplitter!" % type(job.application))

        subjobs = []

        inputdata = job.inputdata

        if not isinstance(job.application, GaudiExec):
            # I'm assuming this file is created by the Gauss Application at some stage?
            if not job.inputdata:
                share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                          'shared',
                                          getConfig('Configuration')['user'],
                                          job.application.is_prepared.name,
                                          'inputdata',
                                          'options_data.pkl')

                if os.path.exists(share_path):
                    f = open(share_path, 'r+b')
                    #FIXME should this have been an addition?
                    inputdata = pickle.load(f)
                    f.close()

        for i in range(self.numberOfJobs):
            j = self._create_subjob(job, inputdata)
            # FIXME this starts from the 1st event and not zero, is it clear why?
            first = self.firstEventNumber + i * self.eventsPerJob + 1
            opts = 'from Gaudi.Configuration import * \n'
            opts += 'from Configurables import GenInit \n'
            opts += 'ApplicationMgr().EvtMax = %d\n' % self.eventsPerJob
            opts += 'GenInit("GaussGen").FirstEventNumber = %d\n' % first
            spillOver = ["GaussGenPrev", "GaussGenPrevPrev", "GaussGenNext"]
            for s in spillOver:
                opts += 'GenInit("%s").FirstEventNumber = %d\n' % (s, first)
            #j.application.extra.input_buffers['data.py'] += opts
            if isinstance(job.application, GaudiExec):
                j.application.extraOpts = j.application.extraOpts + '\n' + opts
            else:
                j._splitter_data = opts
            # j.inputsandbox.append(File(FileBuffer(path,opts).create().name))
            logger.debug("Creating job %d w/ FirstEventNumber = %d" % (i, first))
            subjobs.append(j)

        return subjobs


