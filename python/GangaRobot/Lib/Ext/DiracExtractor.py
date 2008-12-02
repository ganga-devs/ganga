"""Dirac-specific extractor IAction implementation.

The DiracExtractor class extracts data specific to the Dirac backend.

N.B. Since only a few data are extracted, this class should currently be
considered only as an example of a backend-specific extractor. More complete and
relevant data may be extracted in a later version.
 
"""

from GangaRobot.Lib.Base.BaseExtractor import BaseExtractor
from Ganga.GPI import *
from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper

from GangaLHCb.Lib.Dirac import DiracShared


class DiracExtractor(BaseExtractor):
    
    """Dirac-specific extractor IAction implementation.
    
    An extractor which extracts some data specific to the Dirac backend for
    Ganga jobs using Dirac as the backend. See handlejobnode() for details.
    
    The extracted run node has the structure:
    <run>
        <job>
            <dirac/>
        </job>
        <job>
            <dirac/>
        </job>
        ...
    </run>

    This extractor can be reused by adding it to the chain of extractors
    initialised in the constructor of any implementation of BaseExtractor.
    e.g.
    def __init__(self):
        self.chain = [CoreExtractor(), DiracExtractor(), self]
    
    """

    def handlejobnode(self, jobnode, job):
        """Extracts data specific to the Dirac backend for the given job.
        
        Keyword arguments:
        jobnode -- An Extract.Node for the given job.
        job -- A Ganga Job associated with the run.
        
        If the job uses the Dirac backend then the following nodes are added to
        the job node:
        <dirac>
            <status/>
            <application-status/>
            <edg-wl-jobid/>
            <cpu/>
        </dirac>
        where the subnodes of the dirac node contain text values from querying
        the getJobSummary and getJobParams methods of DIRAC.Client.Dirac.Dirac.
        
        Subnodes are guaranteed to be present although their text values may be
        empty if querying Dirac returns no corresponding value.
        
        """
        if job.backend._impl._name != 'Dirac':
            return
        diracnode = jobnode.addnode('dirac')
        # data to extract
        status = None
        applicationstatus = None
        edgwljobid = None
        cpu = None
        # extract data
        diracid = job.backend.id
        try:
                command="""
jobsummary=dirac.getJobSummary(%i)
if not result.get('OK',False): rc = -1
storeResult(jobsummary)
"""%diracid
                result=diracwrapper(command)
                jobSummary=result.getOutput()['Value'][diracid]
                

                status = jobSummary['Status']
                applicationstatus = jobSummary['ApplicationStatus']
        except KeyError:
                pass # data unavailable
        try:
                command="""
jobParams=dirac.getJobParams(%i)
if not result.get('OK',False): rc = -1
storeResult(jobParams)
"""%diracid
                dc=diracwrapper(command)
                jobParams =dc.getOutput()['Value']
                edgwljobid = jobParams['EDG_WL_JOBID']
                cpu = jobParams['CPU']
        except KeyError:
                pass # data unavailable
        # add subnodes
        diracnode.addnode('status', status)
        diracnode.addnode('application-status', applicationstatus)
        diracnode.addnode('edg-wl-jobid', edgwljobid)
        diracnode.addnode('cpu', cpu)

