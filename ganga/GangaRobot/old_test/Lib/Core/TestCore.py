from GangaTest.Framework.tests import GangaGPITestCase
from GangaRobot.Lib.Core.CoreSubmitter import CoreSubmitter
from GangaRobot.Lib.Core.CoreFinisher import CoreFinisher
from GangaRobot.Lib.Core.CoreExtractor import CoreExtractor
from GangaRobot.Lib.Core.CoreReporter import CoreReporter
from GangaRobot.Framework import Utility


class TestCore(GangaGPITestCase):

    """Tests of Core actions and hence indirectly of Base actions."""

    def setUp(self):
        """Create test actions."""
        super(TestCore, self).setUp()
        self.runid = Utility.utcid()
        self.submitter = CoreSubmitter()
        #force submitter patterns
        self.submitter.options = {'CoreSubmitter_Patterns':['GangaRobot/old_test/Lib/Core/test-jobs.txt']}
        self.finisher = CoreFinisher()
        #force finisher timeout to 5 mins
        self.finisher.options = {'BaseFinisher_Timeout':300}
        #test extractor fakes save
        class TestCoreExtractor(CoreExtractor):
            def _saveextract(self, runnode, runid):
                self.runnode = runnode #can be accessed in the test
        self.extractor = TestCoreExtractor()
        #test reporter fakes load and save
        class TestCoreReporter(CoreReporter):
            def _loadextract(self, runid):
                return self.runnode #must be set in the test
            def _savereport(self, report, runid):
                self.report = report #can be accessed in the test 
        self.reporter = TestCoreReporter()

    def tearDown(self):
        """Dereference test actions."""
        self.submitter = None
        self.finisher = None
        self.extractor = None
        self.reporter = None 
        super(TestCore, self).tearDown()

    def test_submitter(self):
        """Test submitter submits 3 jobs and adds them to the jobtree."""
        #execute action
        self.submitter.execute(self.runid)
        #check jobs are added to jobtree
        path = Utility.jobtreepath(self.runid)
        js = jobtree.getjobs(path)
        assert len(js) == 3, 'number of jobs added to jobtree path is not 3'
        for j in js:
            assert j.status != 'new', 'job status is new indicating that it may not have been submitted'

    def test_finisher(self):
        """Test finisher waits for jobs to finish."""
        #submit jobs
        self.submitter.execute(self.runid)
        #execute action
        self.finisher.execute(self.runid)
        #get jobs from jobtree
        path = Utility.jobtreepath(self.runid)
        js = jobtree.getjobs(path)
        #check jobs are finished
        for j in js:
            assert j.status == 'completed', 'job status is not completed indicating that it may not have finished'

    def test_extractor(self):
        """Test extractor creates runnode with correct id and 3 jobs."""
        #submit jobs
        self.submitter.execute(self.runid)
        #execute action
        self.extractor.execute(self.runid)
        #get runnode
        runnode = self.extractor.runnode
        #check runnode contains runid
        assert runnode.getvalue('core.id') == self.runid, 'extracted runnode does not contain correct run id'
        #check runnode contains 3 job nodes
        assert len(runnode.getnodes('job')) == 3, 'extracted runnode does not contain 3 job nodes'

    def test_reporter(self):
        """Test reporter creates report with title and lines and converts to text and html."""
        #submit jobs
        self.submitter.execute(self.runid)
        #extract runnode
        self.extractor.execute(self.runid)
        #set runnode to reporter
        self.reporter.runnode = self.extractor.runnode
        #execute action
        self.reporter.execute(self.runid)
        #get report
        report = self.reporter.report
        #check report is created
        assert report is not None, 'no report created'
        #check report has a title
        assert report.title is not None, 'report does not have a title'
        #check report has some lines
        assert len(report.lines) != 0, 'report does not have any lines'
        #check report converts to plain text
        text = str(report)
        assert text is not None, 'str(report) returns None'
        assert len(text) != 0, 'text version of report is empty'
        #check report converts to html
        html = report.tohtml()
        assert html is not None, 'report.tohtml() returns None'
        assert len(text) != 0, 'html version of report is empty'
