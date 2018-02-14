"""Base reporter IAction implementation.

The BaseReporter class provides an abstract reporter implementation.
 
"""

from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from GangaRobot.Lib.Base.Extract import Node
from GangaRobot.Lib.Base.Report import Report
from GangaCore.Utility.logging import getLogger

logger = getLogger()


class BaseReporter(IAction):
    
    """Base reporter IAction implementation.
    
    An abstract action implementation providing a basis for concrete run data
    reporter implementations. It contains a chain of reporters which are
    processed in the execute() method by calling handlereport() on each reporter
    in turn.
    
    Implementing a sub-class involves doing any of the following:
    - Override the __init__() method to add additional reporters to the chain
    attribute.
    - Override the handlereport() method to report on extracted data.
    
    """

    def __init__(self):
        """Create a new base reporter.
        
        The chain attribute, a list of reporters, is initialised to [self].
        
        Sub-classes can override this constructor to set the chain attribute.
        e.g.
        self.chain = [CoreReporter(), self]

        """
        self.chain = [self]

    def execute(self, runid):
        """Invoke each of the reporters in the chain.
        
        Keyword arguments:
        runid -- A UTC ID string which identifies the run.

        A run node is loaded as an instance of Extract.Node from previously
        extracted data from the configurable BaseExtractor_XmlFile, replacing
        ${runid} with the current run id.
        e.g. ~/gangadir/robot/extract/${runid}.xml

        An empty report is created as an instance of Report.Report and passed
        with the run node to the handlereport() method of each of the reporters
        in the chain.
        
        The report is saved as TEXT and HTML to the configurable
        BaseReporter_TextFile and BaseReporterHtmlFile respectively, replacing
        ${runid} with the current run id.
        e.g. ~/gangadir/robot/report/${runid}.txt
             ~/gangadir/robot/report/${runid}.html
        
        """
        logger.info("Reporting on run '%s'.", runid)
        runnode = self._loadextract(runid)
        report = Report()
        for reporter in self.chain:
            reporter.handlereport(report, runnode)
        self._savereport(report, runid)
        logger.info('Reports saved.')
        
    def _loadextract(self, runid):
        filename = Utility.expand(self.getoption('BaseExtractor_XmlFile'), runid = runid)
        content = Utility.readfile(filename)
        return Node.fromxml(content)

    def _savereport(self, report, runid):
        #text
        textfilename = Utility.expand(self.getoption('BaseReporter_TextFile'), runid = runid)
        textcontent = str(report)
        Utility.writefile(textfilename, textcontent)
        #html
        htmlfilename = Utility.expand(self.getoption('BaseReporter_HtmlFile'), runid = runid)
        htmlcontent = report.tohtml()
        Utility.writefile(htmlfilename, htmlcontent)

    def handlereport(self, report, runnode):
        """Empty default implementation.
        
        Keyword arguments:
        report -- A Report.Report for the run.
        runnode -- An Extract.Node containing the extracted data for the run.
        
        Sub-classes can override this method to modify the report or perhaps
        email or upload it, etc.
        e.g.
        def handlereport(self, report, runnode):
            report.addline('Run id   : ' + runnode.getvalue('core.id'))

        This method is called in the context of a Ganga session.
        
        """
        pass
