# ******************** Imports ******************** #

import os
import uuid
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User


# ******************** Global Variables ******************** #

currentdir = os.path.dirname(os.path.abspath(__file__))
token = None


# ******************** Test Class ******************** #

# Templates API Tests
class TestGangaGUITemplatesAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUITemplatesAPI, self).setUp(extra_opts=[])

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

    # Templates API - GET Method
    def test_GET_method_jobs_list(self):
        from GangaCore.GPI import templates, JobTemplate, GenericSplitter, Local

        # Create 20 test templates
        for i in range(0, 20):
            t = JobTemplate()
            t.name = f"Template Test {i}"
            t.application.exe = "sleep"
            t.splitter = GenericSplitter()
            t.splitter.attribute = 'application.args'
            t.splitter.values = [['3'] for _ in range(0, 3)]
            t.backend = Local()

        # GET request
        res = self.app.get(f"/api/templates", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == 20)

        # Response data assertions
        supported_attributes = ["id", "fqid", "status", "name", "subjobs", "application", "backend", "comment"]
        for i in range(0, 20):
            for attribute in supported_attributes:
                self.assertTrue(attribute in res.json[i])
            self.assertTrue(res.json[i]["name"] == f"Template Test {i}")

    # Templates API - DELETE Method, ID Out of Index
    def test_DELETE_method_id_out_of_range(self):
        res = self.app.delete(f"/api/template/1", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 400)

    # Templates API - DELETE Method, ID is Negative
    def test_DELETE_method_id_negative(self):
        res = self.app.delete(f"/api/template/-1", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Templates API - DELETE Method, ID is String
    def test_DELETE_method_id_string(self):
        res = self.app.delete(f"/api/template/test", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 404)

    # Templates API - DELETE Method
    def test_DELETE_method_templates_list(self):
        from GangaCore.GPI import templates, JobTemplate, GenericSplitter, Local

        # Clean template repository check
        self.assertTrue(len(templates) == 0)

        # Create 20 test templates
        created_template_ids = []
        for i in range(0, 20):
            t = JobTemplate()
            t.name = f"Template Test {i}"
            created_template_ids.append(t.id)

        self.assertTrue(len(templates) == 20)
        self.assertTrue(len(created_template_ids) == 20)

        # Delete one template every request and assert the deletion
        for i in range(0,20):
            self.assertTrue(created_template_ids[i] in templates.ids())
            res = self.app.delete(f"/api/template/{created_template_ids[i]}", headers={"X-Access-Token": token})
            self.assertTrue(res.status_code == 200)
            self.assertTrue(len(templates) == (20-(i+1)))
            self.assertTrue(created_template_ids[i] not in templates.ids())

    # Tear down
    def tearDown(self):
        super(TestGangaGUITemplatesAPI, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
