"""Core submitter IAction implementation.

The CoreSubmitter class provides a generic submitter implementation.
 
"""

from GangaRobot.Lib.Base.BaseSubmitter import BaseSubmitter
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility import files
from GangaCore.GPI import *
import os

logger = getLogger()


class CoreSubmitter(BaseSubmitter):

    """Core submitter IAction implementation.
    
    A submitter which submits exported jobs. See handlesubmit() for details.
    
    This submitter can be reused by adding it to the chain of extractors
    initialised in the constructor of any implementation of BaseExtractor.
    e.g.
    def __init__(self):
        self.chain = [CoreSubmitter(), self]
    
    """

    def handlesubmit(self, jobids, runid):
        """Submits exported jobs as identified by a list of path patterns.
        
        Keyword arguments:
        jobids -- A list of the submitted job ids.
        runid -- A UTC ID string which identifies the run.
        
        The following parameters are loaded from the Robot configuration:
        CoreSubmitter_Patterns e.g. ['GangaRobot/Lib/Core/jobs/core_jobs.txt']

        All files matched by the patterns are assumed to contain single or
        multiple exported jobs, as created using the GPI function 'export'.

        Relative paths are evaluated against the current working directory and
        the Ganga python root, with duplicates being removed.
        
        The jobs are loaded from the files into the job repository, submitted
        and their ids are added to the jobids list.
        
        """
        # get configuration properties
        patterns = self.getoption('CoreSubmitter_Patterns')
        logger.info("Searching for job files matching patterns %s.", patterns)
        matches = self._getmatches(patterns)
        logger.info("Found %d matching job files.", len(matches))

        for match in matches:
            jobs = load(match)
            logger.info("Loaded %d jobs from '%s'.", len(jobs), match)
            for j in jobs:
                j.submit()
                jobids.append(j.id)
        
        
    def _getmatches(self, patterns):
        """Return a list of paths matching the list of path patterns.
        
        Keyword arguments:
        patterns -- A list of path patterns, identifying exported job files.

        Relative paths are evaluated against the current working directory and
        against the Ganga python root, i.e. ganga/python/. Duplicate matches are
        removed (based on absolute file name).
        
        """
        # expand patterns
        patterns = [files.expandfilename(p) for p in patterns]
        # add local patterns absolute or relative to cwd
        cwd = os.getcwd()
        localpatterns = [os.path.join(cwd, p) for p in patterns]
        # add local patterns absolute or relative to Ganga python root
        gangapythonroot = config['System']['GANGA_PYTHONPATH']
        syspatterns = [os.path.join(gangapythonroot, p) for p in patterns]
        # find all matches removing duplicates
        matches = files.multi_glob(localpatterns + syspatterns)
        return matches
