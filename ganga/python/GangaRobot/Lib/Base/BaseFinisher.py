"""Base finisher IAction implementation.

The BaseFinisher class provides an abstract finisher implementation.
 
"""

from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from Ganga.Utility.logging import getLogger
from Ganga.GPI import *
import time

logger = getLogger()


class BaseFinisher(IAction):
    
    """Base finisher IAction implementation.
    
    An abstract action implementation providing a basis for concrete finisher
    implementations. It contains a chain of finishers which are processed in the
    execute() method by calling handleisfinished() for each job associated with
    the runid, on each finisher in turn.

    Implementing a sub-class involves doing any of the following:
    - Override the __init__() method to add additional finishers to the chain
    attribute.
    - Override the handleisfinished() method to indicate that the given job is
    finished.
    
    """

    def __init__(self):
        """Create a new base finisher.
        
        The chain attribute, a list of finishers, is initialised to [self].
        
        Sub-classes can override this constructor to set the chain attribute.
        e.g.
        self.chain = [CoreFinisher(), self]

        """
        self.chain = [self]

    def execute(self, runid):
        """Invoke each of the finishers in the chain.
        
        Keyword arguments:
        runid -- A UTC ID string which identifies the run.
        
        Loops until the run is finished or the elapsed time exceeds the
        configurable BaseFinisher_Timeout (seconds).
        
        Each job in the jobtree directory named by the runid, e.g.
        /2007-06-25_09.18.46, is passed to the handleisfinished() method of each
        finisher in the chain. If any finisher returns False, for any job,
        then the run is considered unfinished.
        
        The sleep period in the loop is 1/10th the timeout, constrained to the
        range [10, 60] seconds.
        
        """
        logger.info("Finishing jobs for run '%s'.", runid)
        startsecs = time.time()
        timeoutsecs = self.getoption('BaseFinisher_Timeout')
        sleepsecs = timeoutsecs / 10
        if sleepsecs < 10: sleepsecs = 10
        if sleepsecs > 60: sleepsecs = 60
        
        logger.info('Waiting for jobs to finish. Timeout %d seconds.'
                    ' Sleep period %d seconds', timeoutsecs, sleepsecs)

        path = Utility.jobtreepath(runid)
        jobs = jobtree.getjobs(path)
        
        while True:
            #suppose finished is True
            finished = True
            for j in jobs:
                #if one job not finished then finished is False
                for finisher in self.chain:
                    if not finisher.handleisfinished(j):
                        finished = False
                        break
            elapsedsecs = time.time() - startsecs
            # test break condition here to avoid sleep after satisfied condition
            if finished or timeoutsecs < elapsedsecs:
                break
            # break condition on satisfied, so sleep
            time.sleep(sleepsecs)
        
        if finished:
            logger.info('All jobs finished after %d seconds.', elapsedsecs)
        else:
            logger.info('Timeout after %d seconds.', elapsedsecs)
            
    def handleisfinished(self, job):
        """Default implementation returns False.
        
        Keyword arguments:
        job -- A Ganga Job associated with the run.
        
        Sub-classes can override this method to indicate if the given job is
        finished. Return True if the job is finished, False otherwise.
        e.g.
        def handleisfinished(self, job):
            return job.status in ['completed', 'killed', 'failed']

        This method is called in the context of a Ganga session.
        
        """
        return False
    
