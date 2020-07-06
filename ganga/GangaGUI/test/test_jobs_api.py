import os
import uuid
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User


# ******************** Global Variables ******************** #

currentdir = os.path.dirname(os.path.abspath(__file__))
token = None


# ******************** Test Class ******************** #

# Job API Tests
class TestGangaGUIJobsAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUIJobsAPI, self).setUp(extra_opts=[])

        # App config and database creation
        app.config["TESTING"] = True
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(currentdir, "gui_test.sqlite")
        db.create_all()

        # Temp user for the test
        tempuser = User(public_id=str(uuid.uuid4()), user="GangaGUITestUser", role="Admin")
        tempuser.store_password_hash("testpassword")
        db.session.add(tempuser)
        db.session.commit()

        # Flask test client
        self.app = app.test_client()

        # Generate token for requests
        global token
        res = self.app.post("/token", data={"user": "GangaGUITestUser", "password": "testpassword"})
        token = res.json["token"]

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
        res = self.app.get(f"/api/jobs", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == 20)

        # Assert response
        supported_attributes = ["id", "fqid", "status", "name", "subjobs", "application", "backend", "backend.actualCE", "comment", "subjob_statuses"]
        for i in range(0, 20):
            for attribute in supported_attributes:
                self.assertTrue(attribute in res.json[i])
            self.assertTrue(res.json[i]["name"] == f"Job Test {i}")

    # Jobs API - GET Method, ID list
    def test_GET_method_jobs_ids_list(self):
        from GangaCore.GPI import Job, GenericSplitter, Local

        # Store ids of test jobs
        stored_ids = []

        # Create test 20 jobs with subjobs
        for i in range(0, 20):
            j = Job()
            j.name = f"Job Test {i}"
            j.application.exe = "sleep"
            j.splitter = GenericSplitter()
            j.splitter.attribute = 'application.args'
            j.splitter.values = [['3'] for _ in range(0, 3)]
            j.backend = Local()
            j.submit()
            stored_ids.append(j.id)

        # GET request
        res = self.app.get(f"/api/jobs/ids", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == 20)

        # Check ids in the response
        for id in stored_ids:
            self.assertTrue(id in res.json)

    # Jobs API - GET Method, Incomplete IDs list
    def test_GET_method_jobs_incomplete_ids_list(self):
        res = self.app.get(f"/api/jobs/incomplete_ids", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)

    # Tear down
    def tearDown(self):
        super(TestGangaGUIJobsAPI, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
