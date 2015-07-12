###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: StagerJobSplitter.py,v 1.2 2008-12-11 20:37:10 hclee Exp $
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
        'scheme'   : SimpleItem(defvalue='local', doc='The job splitting scheme: \'local\', \'lcg\'', hidden=1),
    })

    _GUIPrefs = [ { 'attribute' : 'numfiles', 'widget' : 'Int' } ]
    
    def __make_subjob__(self, mj, guids, names, sjob_evnts=-1, sites=None):
        
        """
        private method to create subjob object
        """
        
        logger.debug('generating subjob to run %d events in-total on files: %s' % (sjob_evnts, repr(guids)))
        j = Job()

        j.name            = mj.name
        j.inputdata       = mj.inputdata

        if j.inputdata.type in ['','DQ2']:
            j.inputdata.guids = guids

        j.inputdata.names = names

        j.outputdata    = mj.outputdata
        j.application   = mj.application
        if sjob_evnts != -1:
            j.application.max_events = sjob_evnts
        j.backend       = mj.backend
        
        if j.backend._name in ['LCG'] and j.backend.requirements._name == 'AtlasLCGRequirements':
            if sites:
                j.backend.requirements.sites = sites
        
        j.inputsandbox  = mj.inputsandbox
        j.outputsandbox = mj.outputsandbox

        return j

    def split(self,job):

        logger.debug('StagerJobSplitter called')

        if job.inputdata._name != 'StagerDataset':
            raise ApplicationConfigurationError(None,'StagerJobSplitter requires StagerDataset as input')

        if job.application._name not in  ['Athena']:
            raise ApplicationConfigurationError(None,'StagerJobSplitter requires Athena as application')

        if self.numfiles <= 0: 
            self.numfiles = 1

        total_evnts = -1
        try:
            total_evnts = int(job.application.max_events)
        except ValueError:
            pass

        sjob_evnts  = -1

        subjobs = []

        ## resolve pfns and guids
        pfns = job.inputdata.get_surls()
        myguids = pfns.keys()

        myguids.sort()

        mynames = []
        for guid in myguids:
            mynames.append( pfns[guid] )

        if job.inputdata.type in ['', 'DQ2']:
            job.inputdata.guids = myguids

        job.inputdata.names = mynames

        nrjob = int(math.ceil(len(mynames)/float(self.numfiles)))

        if total_evnts != -1:
            sjob_evnts = int(math.ceil( total_evnts/float(nrjob) ))

        ## split scheme for local jobs: simple splitting
        if self.scheme.lower() == 'local': 
            for i in xrange(0,nrjob):
                subjobs.append( self.__make_subjob__(job, myguids[i*self.numfiles:(i+1)*self.numfiles], mynames[i*self.numfiles:(i+1)*self.numfiles], sjob_evnts) )

        return subjobs
