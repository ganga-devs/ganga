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
    Split job by changing the args attribute of the application.

    This splitter only applies to the applications which have args attribute (e.g. Executable, Root).
    It is a special case of the GenericSplitter.

    This splitter allows the creation of a series of subjobs where
    the only difference between different jobs are their
    arguments. Below is an example that executes a ROOT script ~/analysis.C

    void analysis(const char* type, int events) {
      std::cout << type << "  " << events << std::endl;
    }

    with 3 different sets of arguments.

    s = ArgSplitter(args=[['AAA',1],['BBB',2],['CCC',3]])
    r = Root(version='5.10.00',script='~/analysis.C')
    j.Job(application=r, splitter=s)

    Notice how each job takes a list of arguments (in this case a list
    with a string and an integer). The splitter thus takes a list of
    lists, in this case with 3 elements so there will be 3 subjobs.

    Running the subjobs will produce the output:
    subjob 1 : AAA  1
    subjob 2 : BBB  2
    subjob 3 : CCC  3
"""
    _name = "Im3ShapeSplitter"
    _schema = Schema(Version(1, 0), {'size': SimpleItem(defvalue=5, doc='Size of the tiles which are to be split.'),
                                     'split_by_file': SimpleItem(defvalue=True, doc='Should we auto-split into subjobs here on a per-file basis?')})

    def split(self, job):

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

