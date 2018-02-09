"""Demonstration IAction implementations.

The DemoExtractor and DemoReporter classes provide simple examples of how to
reuse Core actions adding to the extracted data and summary reports.

This example is referenced in the WIKI page:
http://twiki.cern.ch/twiki/bin/view/ArdaGrid/HowToGangaRobot

See DEMO.INI for suitable configuration options.

"""

from GangaRobot.Lib.Base.BaseExtractor import BaseExtractor
from GangaRobot.Lib.Base.BaseReporter import BaseReporter
from GangaRobot.Lib.Core.CoreExtractor import CoreExtractor
from GangaRobot.Lib.Core.CoreReporter import CoreReporter
import random

class DemoExtractor(BaseExtractor):
    """Simple example of how to reuse CoreExtractor, adding to the extracted data."""
    
    def __init__(self):
        """Add CoreExtractor and self to chain of extractors."""
        self.chain = [CoreExtractor(), self]

    def handlejobnode(self, jobnode, job):
        """Add a 'demo' subnode to each 'job' node with a random integer value in range [0,100]."""
        dn = jobnode.addnode('demo')
        dn.addnode('value', random.randint(0,100))

class DemoReporter(BaseReporter):
    """Simple example of how to reuse CoreReporter, adding to the summary report."""
    
    def __init__(self):
        """Add CoreReporter and self to chain of reporters."""
        self.chain = [CoreReporter(), self]
    
    def handlereport(self, report, runnode):
        """Set the report title and add a line reporting the average of the demo values."""
        report.title = 'Demo Report'
        values = runnode.getvalues('job.demo.value')
        total = 0
        for value in values:
            total += int(value)
        avg = total / max(len(values), 1)
        report.addline('Average value: %d' % avg)
