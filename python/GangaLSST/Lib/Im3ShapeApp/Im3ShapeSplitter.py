###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: ArgSplitter.py,v 1.1 2008-07-17 16:40:59 moscicki Exp $
###############################################################################

import copy
from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem
from Ganga.GPIDev.Lib.Dataset.GangaDataset import GangaDataset
from GangaLSST.Lib.Im3ShapeApp.Im3ShapeApp import Im3ShapeApp

from Ganga.Utility.logging import getLogger
logger = getLogger()


class Im3ShapeSplitter(ISplitter):

    """
    This splitter splits jobs using the Im3ShapeApp application using the size parameter.

    If a splitter is configured with size = 5, split_by_file = True, then it will create 5 subjobs per file in the master_job.inputdata
    If a splitter is configured with size = 5, split_by_file = False, then it will create 5 subjobs total and configure all subjobs to use all given data.

    In the future there may be support for splitting based upon regex and namePatterns in the inputdata to allow a certain subset of data to be put in each subjob.
    """
    _name = "Im3ShapeSplitter"
    _schema = Schema(Version(1, 0), {'size': SimpleItem(defvalue=5, doc='Size of the tiles which are to be split.'),
                                     'split_by_file': SimpleItem(defvalue=True, doc='Should we auto-split into subjobs here on a per-file basis?')})

    def split(self, job):
        """
        Actually perform the splitting of the given master job. The generated subjobs of the splitting are returned
        Args:
            job (Job): This is the master job object which is to be split and the subjobs of which are returned
        """

        assert isinstance(job.application, Im3ShapeApp)

        subjobs = []

        def getApp(job, rank, size):
            app = copy.deepcopy(job.application)
            app.rank = rank
            app.size = size
            return app

        if self.split_by_file:
            for this_file in job.inputdata:
                for rank in range(0, self.size):
                    j = self.createSubjob(job, ['application'])
                    # Add new arguments to subjob
                    j.application = getApp(job, rank, self.size)
                    j.inputdata = GangaDataset(files = [stripProxy(this_file)])
                    subjobs.append(j)
        else:
            for rank in range(0,self.size):
                j = self.createSubjob(job, ['application'])
                j.application = getApp(job, rank, self.size)
                j.inputdata = job.inputdata
                logger.debug('Rank for split job is: ' + str(rank))
                subjobs.append(j)

        return subjobs

