"""Threaded submitter IAction implementation.

The ThreadedSubmitter class provides a generic submitter implementation.
 
"""

from GangaRobot.Lib.Core.CoreSubmitter import CoreSubmitter
from GangaCore.Utility.logging import getLogger
from GangaCore.GPI import load
from GangaCore.Core.GangaThread.MTRunner import MTRunner, Data, Algorithm
from GangaCore.GPIDev.Base.Proxy import stripProxy

logger = getLogger()


class ThreadedSubmitter(CoreSubmitter):

    """Threaded submitter IAction implementation.
    
    A submitter which submits exported jobs in a configurable number of threads. See handlesubmit() for details.
    
    """

    def handlesubmit(self, jobids, runid):
        """Submits exported jobs as identified by a list of path patterns.
        
        Keyword arguments:
        jobids -- A list of the submitted job ids.
        runid -- A UTC ID string which identifies the run.
        
        """
        # get configuration properties
        patterns = self.getoption('CoreSubmitter_Patterns')
        logger.info("Searching for job files matching patterns %s.", patterns)
        matches = self._getmatches(patterns)
        logger.info("Found %d matching job files.", len(matches))
        runner = MTRunner(
            'ThreadedSubmitterMTRunner',
            algorithm=ThreadedSubmitterAlgorithm(), 
            data=Data([(m,jobids) for m in matches]), 
            numThread=int(self.getoption('ThreadedSubmitter_numThreads'))
        )
        runner.start()
        runner.join()


class ThreadedSubmitterAlgorithm(Algorithm):
    def process(self, item):
        (match,jobids) = item
        jobs = load(match)
        logger.info("Loaded %d jobs from '%s'.", len(jobs), match)
        for j in jobs:
            stripProxy(j.application).is_prepared = True
            j.submit()
            jobids.append(j.id)

