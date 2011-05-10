"""Core extractor IAction implementation.

The CoreExtractor class provides a generic extractor implementation.
 
"""

from GangaRobot.Framework import Utility
from GangaRobot.Lib.Base.BaseExtractor import BaseExtractor
from Ganga.GPI import *


class CoreExtractor(BaseExtractor):

    """Core extractor IAction implementation.
    
    An extractor which extracts generic data common to most Ganga jobs
    irrespective of application or backend used. See handlerunnode() and
    handlejobnode() for details.
    
    The extracted run node has the structure:
    <run>
        <core/>
        <job>
            <core/>
        </job>
        <job>
            <core/>
        </job>
        ...
    </run>
    
    This extractor can be reused by adding it to the chain of extractors
    initialised in the constructor of any implementation of BaseExtractor.
    e.g.
    def __init__(self):
        self.chain = [CoreExtractor(), self]
    
    """

    def handlerunnode(self, runnode, runid):
        """Extracts generic data for the given run.
        
        Keyword arguments:
        runnode -- An Extract.Node for the run.
        runid -- A UTC ID string which identifies the run.
        
        Example of nodes added to the run node:
        <core>
            <id>2007-06-22_13.17.51</id>
            <start-time>2007/06/22 13:17:51</start-time>
            <extract-time>2007/06/22 13:18:55</extract-time>
        </core>
        
        All nodes are guaranteed to be present. All times are in UTC.
        
        """
        corenode = runnode.addnode('core')
        corenode.addnode('id', runid)
        corenode.addnode('start-time', Utility.utctime(runid))
        corenode.addnode('extract-time', Utility.utctime())

    def handlejobnode(self, jobnode, job):
        """Extracts generic data for the given job.
        
        Keyword arguments:
        jobnode -- An Extract.Node for the given job.
        job -- A Ganga Job associated with the run.
        
        Example of nodes added to the job node:
        <core>
            <id>4</id>
            <name>Core_2007-06-22_13.17.51_0</name>
            <status>completed</status>
            <application>Executable</application>
            <backend>Local</backend>
            <backend-id>19253</backend-id>
            <backend-actualCE>lx09.hep.ph.ic.ac.uk</backend-actualCE>
        </core>
        
        All nodes are guaranteed to be present although their text values may be
        empty if querying the job returns no corresponding value.
        
        """
        corenode = jobnode.addnode('core')
        corenode.addnode('id', job.id)
        corenode.addnode('name', job.name)
        corenode.addnode('status', job.status)
        #FIXME: Add job timestamps when available
        corenode.addnode('application', typename(job.application))
        corenode.addnode('backend', typename(job.backend))
        backendid = None
        statusInfo = None
        if hasattr(job.backend,'statusInfo'):
            statusInfo = job.backend.statusInfo
        corenode.addnode('minor-status',statusInfo)
        if hasattr(job.backend, 'id'):
            backendid = job.backend.id 
        corenode.addnode('backend-id', backendid)
        backendactualCE = None
        if hasattr(job.backend, 'actualCE'):
            backendactualCE = job.backend.actualCE
        corenode.addnode('backend-actualCE', backendactualCE)
