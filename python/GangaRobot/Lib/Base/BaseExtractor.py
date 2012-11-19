"""Base extractor IAction implementation.

The BaseExtractor class provides an abstract extractor implementation.
 
"""

from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from GangaRobot.Lib.Base.Extract import Node
from Ganga.Utility.logging import getLogger
from Ganga.GPI import *

logger = getLogger()


class BaseExtractor(IAction):
    
    """Base extractor IAction implementation.
    
    An abstract action implementation providing a basis for concrete run data
    extractor implementations. It contains a chain of extractors which are
    processed in the execute() method by calling handlerunnode() once, and
    handlejobnode() for each job associated with the runid, on each extractor
    in turn.
    
    Implementing a sub-class involves doing any of the following:
    - Override the __init__() method to add additional extractors to the chain
    attribute.
    - Override the handlerunnode() method to add extracted data to the run node.
    - Override the handlejobnode() method to add extracted data to the job node
    for the given job.
    
    The extracted run node has the structure:
    <run>
        <job/>
        <job/>
        ...
    </run>
    
    """
    
    def __init__(self):
        """Create a new base extractor.
        
        The chain attribute, a list of extractors, is initialised to [self].
        
        Sub-classes can override this constructor to set the chain attribute.
        e.g.
        self.chain = [CoreExtractor(), self]

        """
        self.chain = [self]

    def execute(self, runid):
        """Invoke each of the extractors in the chain.
        
        Keyword arguments:
        runid -- A UTC ID string which identifies the run.

        An empty run node is created as an instance of Extract.Node and passed
        to the handlerunnode() method of each extractor in the chain.
        
        An empty job node is created, as a subnode of the run node, for each job
        in the jobtree directory named by the runid, e.g. /2007-06-25_09.18.46,
        and passed to the handlejobnode() method of each extractor in the chain.
        
        The run node is saved as XML to the configurable BaseExtractor_XmlFile,
        replacing ${runid} with the current run id.
        e.g. ~/gangadir/robot/extract/${runid}.xml
        
        """
        logger.info("Extracting data for run '%s'.", runid)
        runnode = Node('run')
        for extractor in self.chain:
            extractor.handlerunnode(runnode, runid)
        path = Utility.jobtreepath(runid)
        ids = jobtree.listjobs(path)
        ids.sort()
        for id in ids:
            jobnode = Node('job')
            job = jobs(id)
            for extractor in self.chain:
                extractor.handlejobnode(jobnode, job)
            runnode.nodes.append(jobnode)
        self._saveextract(runnode, runid)
        logger.info('Extract data saved.')

    def _saveextract(self, runnode, runid):
        filename = Utility.expand(self.getoption('BaseExtractor_XmlFile'), runid = runid)
        content = runnode.toprettyxml()
        Utility.writefile(filename, content)

    def handlerunnode(self, runnode, runid):
        """Empty default implementation.
        
        Keyword arguments:
        runnode -- An Extract.Node for the run.
        runid -- A UTC ID string which identifies the run.
        
        Sub-classes can override this method to add extracted data to the run
        node, possibly using the Ganga GPI.
        e.g.
        def handlerunnode(self, runnode, runid):
            corenode = runnode.addnode('core')
            corenode.addnode('id', runid)

        This method is called in the context of a Ganga session.
        
        """
        pass

    def handlejobnode(self, jobnode, job):
        """Empty default implementation.
        
        Keyword arguments:
        jobnode -- An Extract.Node for the given job.
        job -- A Ganga Job associated with the run.
        
        Sub-classes can override this method to add extracted data to the job
        node, possibly using the Ganga GPI.
        e.g.
        def handlejobnode(self, jobnode, job):
            corenode = jobnode.addnode('core')
            corenode.addnode('id', job.id)

        This method is called in the context of a Ganga session.
        
        """
        pass

    
