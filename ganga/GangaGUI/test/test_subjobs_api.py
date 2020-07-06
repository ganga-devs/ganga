# ******************** Imports ******************** #

import os
import uuid
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User

# ******************** Global Vairables ******************** #

currentdir = os.path.dirname(os.path.abspath(__file__))
token = None


# ******************** Test Class ******************** #

# Subjobs API Tests
class TestGangaGUISubjobsAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUISubjobsAPI, self).setUp(extra_opts=[])

        # App config setup and test database creation
        app.config["TESTING"] = True
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(currentdir, "gui_test.sqlite")
        db.create_all()

        # Temp user for testing purpose
        tempuser = User(public_id=str(uuid.uuid4()), user="GangaGUITestUser", role="Admin")
        tempuser.store_password_hash("testpassword")
        db.session.add(tempuser)
        db.session.commit()

        # Flask test client
        self.app = app.test_client()

        # Token for HTTP requests
        global token
        res = self.app.post("/token", data={"user": "GangaGUITestUser", "password": "testpassword"})
        token = res.json["token"]

    # Subjobs API - GET Method, Subjobs List - Job ID out of range
    def test_GET_method_job_id_out_of_range(self):
        res = self.app.get(f"/api/job/1/subjobs", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Subjobs API - GET Method, Subjobs List - Job ID negative
    def test_GET_method_job_id_negative(self):
        res = self.app.get(f"/api/job/-1/subjobs", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Subjobs API - GET Method, Subjobs List - Job ID string
    def test_GET_method_job_id_string(self):
        res = self.app.get(f"/api/job/test/subjobs", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Subjobs API - GET Method, Subjobs List - No subjob case
    def test_GET_method_job_id_no_subjob(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "Test Name"

        # GET request
        res = self.app.get(f"/api/job/{j.id}/subjobs", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == 0)

    # Subjob API - GET Method, Subjobs List
    def test_GET_method_job_id(self):
        from GangaCore.GPI import Job, GenericSplitter, Local

        # Create test job
        j = Job()
        j.name = "Subjob test"
        j.application.exe = "sleep"
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['5'] for _ in range(0, 20)]
        j.backend = Local()
        j.submit()

        # GET request
        res = self.app.get(f"/api/job/{j.id}/subjobs", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == 20)

        # Supported attributes list and assertions
        supported_attributes = ["id", "fqid", "status", "name", "application", "backend", "backend.actualCE", "comment"]
        for i in range(0, 20):
            for attribute in supported_attributes:
                self.assertTrue(attribute in res.json[i].keys())

    # Subjobs API - GET Method, Single Subjob info - Job ID out of range
    def test_GET_method_single_subjob_info_job_id_out_of_range(self):
        res = self.app.get(f"/api/job/1/subjob/2", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Subjobs API - GET Method, Single Subjob info - Job ID negative
    def test_GET_method_single_subjob_info_job_id_negative(self):
        res = self.app.get(f"/api/job/-1/subjob/2", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Subjobs API - GET Method, Single Subjob info - Job ID string
    def test_GET_method_single_subjob_info_job_id_string(self):
        res = self.app.get(f"/api/job/test/subjob/2", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Subjobs API - GET Method, Single Subjob info - No subjob case
    def test_GET_method_single_subjob_info_no_subjob(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "Test Name"

        # GET request
        res = self.app.get(f"/api/job/{j.id}/subjob/2", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Subjobs API - GET Method, Single Subjob info - subjob id out of index
    def test_GET_method_single_subjob_info_subjob_id_out_of_index(self):
        from GangaCore.GPI import Job, GenericSplitter, Local

        # Create test job with subjobs
        j = Job()
        j.name = "Subjob test"
        j.application.exe = "sleep"
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['5'] for _ in range(0, 10)]
        j.backend = Local()
        j.submit()

        # GET request
        res = self.app.get(f"/api/job/{j.id}/subjob/21", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Subjobs API - GET Method, Single Subjob Info
    def test_GET_method_single_subjob_info(self):
        from GangaCore.GPI import Job, GenericSplitter, Local

        # Create test job with subjobs
        j = Job()
        j.name = "Subjob test"
        j.application.exe = "sleep"
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['5'] for _ in range(0, 20)]
        j.backend = Local()
        j.submit()

        # GET request
        res = self.app.get(f"/api/job/{j.id}/subjob/{j.subjobs[2].id}", headers={"X-Access-Token": token})
        print(res.json)
        self.assertTrue(res.status_code == 200)

        # Attributes in the API response body
        supported_attributes = ["id", "fqid", "status", "name", "application", "backend", "backend.actualCE", "comment"]
        for attribute in supported_attributes:
            self.assertTrue(attribute in res.json.keys())

    # Subjobs API - GET Method, Single Subjob Attribute info - Job ID out of range
    def test_GET_method_single_subjob_attr_info_job_id_out_of_range(self):
        res = self.app.get(f"/api/job/1/subjob/2/fqid", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Subjobs API - GET Method, Single Subjob Attribute info - Job ID negative
    def test_GET_method_single_subjob_attr_info_job_id_negative(self):
        res = self.app.get(f"/api/job/-1/subjob/2/fqid", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Subjobs API - GET Method, Single Subjob Attribute info - Job ID string
    def test_GET_method_single_subjob_attr_info_job_id_string(self):
        res = self.app.get(f"/api/job/test/subjob/2/fqid", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Subjobs API - GET Method, Single Subjob Attribute info - No subjob case
    def test_GET_method_single_subjob_attr_info_no_subjob(self):
        from GangaCore.GPI import Job

        # Create test job
        j = Job()
        j.name = "Test Name"

        # GET request
        res = self.app.get(f"/api/job/{j.id}/subjob/2/fqid", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Subjobs API - GET Method, Single Subjob Attribute info - subjob id out of index
    def test_GET_method_single_subjob_attr_info_subjob_id_out_of_index(self):
        from GangaCore.GPI import Job, GenericSplitter, Local

        # Create job with subjobs
        j = Job()
        j.name = "Subjob test"
        j.application.exe = "sleep"
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['5'] for _ in range(0, 10)]
        j.backend = Local()
        j.submit()

        # GET request
        res = self.app.get(f"/api/job/{j.id}/subjob/21/fqid", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Subjobs API - GET Method, Single Subjob Attribute Info
    def test_GET_method_single_subjob_attr_info(self):
        from GangaCore.GPI import Job, GenericSplitter, Local

        # Create test job with subjobs
        j = Job()
        j.name = "Subjob test"
        j.application.exe = "sleep"
        j.splitter = GenericSplitter()
        j.splitter.attribute = 'application.args'
        j.splitter.values = [['5'] for _ in range(0, 5)]
        j.backend = Local()
        j.submit()

        supported_attributes = ["application", "backend", "do_auto_resubmit", "fqid", "id", "info", "inputdir",
                                "inputfiles",
                                "master", "name", "outputdir",
                                "outputfiles", "parallel_submit", "status", "time"]

        for i in range(0, 5):
            for attribute in supported_attributes:
                res = self.app.get(f"/api/job/{j.id}/subjob/{j.subjobs[i].id}/{attribute}",
                                   headers={"X-Access-Token": token})
                self.assertTrue(res.status_code == 200)
                self.assertTrue(attribute in res.json.keys())

    # Tear down
    def tearDown(self):
        super(TestGangaGUISubjobsAPI, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
