from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.api import internal


# ******************** Test Class ******************** #

# JobTree API Tests
class TestGangaGUIInternalJobTreeAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUIInternalJobTreeAPI, self).setUp(extra_opts=[])

        # App config and database creation
        internal.config["TESTING"] = True

        # Flask test client
        self.app = internal.test_client()

    # Job Tree API - GET Method
    def test_GET_method_jobtree_list(self):
        from GangaCore.GPI import Job, jobtree

        # Create 10 dir in jobtree and add 10 jobs to each
        for i in range(0, 10):
            jobtree.mkdir(f"Jobdir{i}")
            jobtree.cd(f"Jobdir{i}")
            for j in range(0, 10):
                jobtree.add(Job(name=f"Job{i}-{j}"))
            jobtree.cd()

        # GET reqeust
        res = self.app.get(f"/internal/jobtree")
        self.assertTrue(res.status_code == 200)

        # Assert presence of the created dirs in the response
        for i in range(0, 10):
            self.assertTrue(f"Jobdir{i}" in res.json["/"].keys())

    # Tear down
    def tearDown(self):
        super(TestGangaGUIInternalJobTreeAPI, self).tearDown()

# ******************** EOF ******************** #
