from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.api import internal


# ******************** Test Class ******************** #

# Job API Tests
class TestGangaGUIInternalJobsAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUIInternalJobsAPI, self).setUp(extra_opts=[])

        # App config and database creation
        internal.config["TESTING"] = True

        # Flask test client
        self.app = internal.test_client()

    # Jobs API - GET Method
    def test_GET_method_jobs_list(self):
        from GangaCore.GPI import Job, GenericSplitter, Local

        # Create 20 test jobs with subjobs
        for i in range(0, 20):
            j = Job()
            j.name = f"Job Test {i}"
            j.application.exe = "sleep"
            j.splitter = GenericSplitter()
            j.splitter.attribute = 'application.args'
            j.splitter.values = [['3'] for _ in range(0, 3)]
            j.backend = Local()
            j.submit()

        # GET reqeust
        res = self.app.get(f"/internal/jobs")
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == 20)

        # Assert response
        supported_attributes = ["id", "fqid", "status", "name", "subjobs", "application", "backend", "backend.actualCE",
                                "comment", "subjob_statuses"]
        for i in range(0, 20):
            for attribute in supported_attributes:
                self.assertTrue(attribute in res.json[i])
            self.assertTrue(res.json[i]["name"] == f"Job Test {i}")

    # Jobs API - GET Method, Incomplete IDs list
    def test_GET_method_jobs_incomplete_ids_list(self):
        res = self.app.get(f"/internal/jobs/incomplete-ids")
        self.assertTrue(res.status_code == 200)

    # Tear down
    def tearDown(self):
        super(TestGangaGUIInternalJobsAPI, self).tearDown()

# ******************** EOF ******************** #
