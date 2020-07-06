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

# JobTree API Tests
class TestGangaGUIJobTreeAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUIJobTreeAPI, self).setUp(extra_opts=[])

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
        res = self.app.get(f"/api/jobtree", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)

        # Assert presence of the created dirs in the response
        for i in range(0, 10):
            self.assertTrue(f"Jobdir{i}" in res.json["/"].keys())

    # Tear down
    def tearDown(self):
        super(TestGangaGUIJobTreeAPI, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
