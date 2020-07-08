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

# Config API Tests
class TestGangaGUIConfigAPI(GangaUnitTest):

    # Setup
    def setUp(self, extra_opts=[]):
        super(TestGangaGUIConfigAPI, self).setUp(extra_opts=[])

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

    # Config API - GET Method
    def test_GET_method_config_list(self):
        from GangaCore.GPI import config
        from GangaCore.Utility.Config import getConfig

        # To store sections of config
        list_of_sections = []

        # Get each section information and append to the list
        for section in config:

            config_section = getConfig(section)
            options_list = []

            # Get options information for the particular config section
            for o in config_section.options.keys():
                options_list.append({
                    "name": str(config_section.options[o].name),
                    "value": str(config_section.options[o].value),
                    "docstring": str(config_section.options[o].docstring),
                })

            # Append config section data to the list
            list_of_sections.append({
                "name": str(config_section.name),
                "docstring": str(config_section.docstring),
                "options": options_list,
            })

        res = self.app.get(f"/api/config", headers={"X-Access-Token": token})
        self.assertTrue(res.status_code == 200)
        self.assertTrue(len(res.json) == len(list_of_sections))

    # Tear down
    def tearDown(self):
        super(TestGangaGUIConfigAPI, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
