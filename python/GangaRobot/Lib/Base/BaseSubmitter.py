"""Base submitter IAction implementation.

The BaseSubmitter class provides an abstract submitter implementation.
 
"""

from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from Ganga.Utility.logging import getLogger
from Ganga.GPI import *

logger = getLogger()


class BaseSubmitter(IAction):
    
    """Base submitter IAction implementation.
    
    An abstract action implementation providing a basis for concrete job
    submitter implementations. It contains a chain of submitters which are
    processed in the execute() method by calling handlesubmit() on each
    submitter in turn.
    
    Implementing a sub-class involves doing any of the following:
    - Override the __init__() method to add additional submitters to the chain
    attribute.
    - Override the handlesubmit() method to submit jobs using the Ganga GPI.
    
    """

    def __init__(self):
        """Create a new base submitter.
        
        The chain attribute, a list of submitters, is initialised to [self].
        
        Sub-classes can override this constructor to set the chain attribute.
        e.g.
        self.chain = [CoreSubmitter(), self]

        """
        self.chain = [self]

    def execute(self, runid):
        """Invoke each of the submitters in the chain.
        
        Keyword arguments:
        runid -- A UTC ID string which identifies the run.
        
        An empty list of jobids is created and passed to the handlesubmit()
        method of each the submitter in the chain.
        
        Any job, whose id is added to the argument jobids, is added to the
        jobtree in the directory named by the runid, e.g. /2007-06-25_09.18.46.

        """
        logger.info("Submitting jobs for run '%s'.", runid)
        jobids = []
        for submitter in self.chain:
            submitter.handlesubmit(jobids, runid)
        path = Utility.jobtreepath(runid)
        jobtree.mkdir(path)
        for id in jobids:
            jobtree.add(jobs(id), path)
        
        logger.info("%d submitted jobs added to jobtree path '%s'.", len(jobids), path)
        
        
    def handlesubmit(self, jobids, runid):
        """Empty default implementation.
        
        Keyword arguments:
        jobids -- A list of the submitted job ids.
        runid -- A UTC ID string which identifies the run.
        
        Sub-classes can override this method to submit jobs using the Ganga GPI.
        The id of any submitted job should be added to the jobids list.
        e.g.
        def handlesubmit(self, jobids, runid):
            j = Job()
            j.application.is_prepared = True
            j.submit()
            jobids.append(j.id)

        This method is called in the context of a Ganga session.
        
        """
        pass
