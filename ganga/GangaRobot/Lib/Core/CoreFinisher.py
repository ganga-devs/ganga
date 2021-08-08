"""Core finisher IAction implementation.

The CoreFinisher class provides a generic finisher implementation.
 
"""

from GangaRobot.Lib.Base.BaseFinisher import BaseFinisher


class CoreFinisher(BaseFinisher):
    
    """Core finisher IAction implementation.
    
    A finisher which evaluates if jobs are finished in a generic way. See
    handleisfinished() for details.
    
    This finisher can be reused by adding it to the chain of finishers
    initialised in the constructor of any implementation of BaseFinisher.
    e.g.
    def __init__(self):
        self.chain = [CoreSubmitter(), self]
    
    """

    def handleisfinished(self, job):
        """Evaluate if job is finished based on job status.
        
        Keyword arguments:
        job -- A Ganga Job associated with the run.
        
        Return True if job status is 'completed', 'killed', 'failed',
        'unknown', or 'new' (i.e. if reverted to 'new' after submission failed).
        
        Return False otherwise.
        
        """
        return job.status in ['completed', 'killed', 'failed', 'unknown', 'new']
    