###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: StagerJobSplitter.py,v 1.1 2008-09-02 12:50:45 hclee Exp $
###############################################################################
# Athena StagerJobSplitter

import math
import os.path

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Schema import *
from Ganga.Lib.Mergers import RootMerger 

from Ganga.Utility.logging import getLogger

logger = getLogger()

class StagerJobSplitter(ISplitter):
    '''Job splitting for StagerDataset'''

    _name     = 'StagerJobSplitter'
    _category = 'splitters'
    _schema   = Schema(Version(1,0), {
        'numfiles' : SimpleItem(defvalue=0,doc='Number of files per subjob'),
        'scheme'   : SimpleItem(defvalue='local', doc='The job splitting scheme: \'local\', \'lcg\''),
    })

    _GUIPrefs = [ { 'attribute' : 'numfiles', 'widget' : 'Int' },
                  { 'attribute' : 'scheme'  , 'widget' : 'String_Choice', 'choices' : [ 'local', 'lcg' ]} ]

    def split(self,job):

        logger.debug('StagerJobSplitter called')

        if job.inputdata._name <> 'StagerDataset':
            raise ApplicationConfigurationError(None,'StagerJobSplitter requires StagerDataset as input')

        if job.application._name <> 'AMAAthena':
            raise ApplicationConfigurationError(None,'StagerJobSplitter requires AMAAthena as application')

        if self.numfiles <= 0: 
            self.numfiles = 1

        total_evnts = -1
        try:
            total_evnts = int(job.application.max_events)
        except ValueError:
            pass

        sjob_evnts  = -1

        subjobs = []

        ## split scheme for local jobs: simple splitting
        if self.scheme.lower() == 'local':
            job.inputdata.fill_guids()

            myguids = []
            for guid in job.inputdata.guids:
                myguids.append(guid)

            nrjob = int(math.ceil(len(myguids)/float(self.numfiles)))

            if total_evnts != -1:
                sjob_evnts = int(math.ceil( total_evnts/float(nrjob) ))

            for i in xrange(0,nrjob):
          
                j = Job()

                j.name            = job.name
                j.inputdata       = job.inputdata
                j.inputdata.guids = myguids[i*self.numfiles:(i+1)*self.numfiles]
    
                logger.debug('subjob datafiles: %s' % str(j.inputdata.guids))

                j.outputdata    = job.outputdata
                j.application   = job.application
                if sjob_evnts != -1:
                    j.application.max_events = str(sjob_evnts)
                j.backend       = job.backend
                j.inputsandbox  = j.inputsandbox
                j.outputsandbox = j.outputsandbox

                subjobs.append(j)

        ## split scheme for grid jobs
        ## N.B. a smarter splitting scheme based on data location is needed
        if self.scheme.lower() == 'lcg':

            # resolving file locations
            f_locations = job.input.get_file_locations()

            # grouping files according to sites
            for f in f_locations.keys():
                pass
                

        ## setup up the corresponding merger for output auto-merging
        conf_name   = os.path.basename(job.application.driver_config.config_file.name)
        sample_name = 'mySample'
        if job.name:
            sample_name = job.name

        if (not job.merger) or (job.merger._name <> 'RootMerger'):
            logger.debug('enforce job to use RootMerger')
            job.merger = RootMerger()

        job.merger.files = [ 'summary/summary_%s_confFile_%s_nEvts_%d.root' % (sample_name, conf_name, sjob_evnts) ]
        job.merger.ignorefailed = True
        job.merger.overwrite = True

        return subjobs
