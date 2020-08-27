import os
import uuid
import jwt
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui.routes import gui, db, User
from GangaCore.Runtime.GPIexport import exportToGPI
from GangaGUI.start import start_gui


# Export start_gui function to GPI
exportToGPI("start_gui", start_gui, "Functions")

# Path of current directory
currentdir = os.path.dirname(os.path.abspath(__file__))


# ******************** Test Class ******************** #


class TestGangaGUITokenAuthentication(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        super(TestGangaGUITokenAuthentication, self).setUp(extra_opts=[])
        gui.config["TESTING"] = True
        gui.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(currentdir, "gui_test.sqlite")
        gui.config["LOGIN_DISABLED"] = True
        gui.config["INTERNAL_PORT"] = 5000

        # Create database and add a temp user to the database
        db.create_all()
        tempuser = User(public_id=str(uuid.uuid4()), user="GangaGUITestUser", role="Admin")
        tempuser.store_password_hash("testpassword")
        db.session.add(tempuser)
        db.session.commit()

        # Flask testing client
        self.app = gui.test_client()

        # Start internal API server
        from GangaCore.GPI import start_gui
        start_gui(only_internal=True, internal_port=5000)

    # Test existence of the database file
    def test_database_existence(self):
        self.assertTrue(os.path.exists(os.path.join(currentdir, "gui_test.sqlite")))

    # Test response of GUI server when no credential information is provided in the request body
    def test_no_credential(self):
        res = self.app.post("/token")
        self.assertTrue(res.status_code == 401)
        self.assertFalse(res.json["success"])
        self.assertFalse("token" in res.json.keys())

    # Test provision on invalid credentials in the request body
    def test_wrong_credential(self):
        res = self.app.post("/token", json={"user": "wronguser", "password": "wrongpassword"})
        self.assertTrue(res.status_code == 401)
        self.assertFalse(res.json["success"])
        self.assertFalse("token" in res.json.keys())

    # Test provision of valid credentails in the request body
    def test_right_credential(self):
        res = self.app.post("/token", json={"username": "GangaGUITestUser", "password": "testpassword"})
        print(res.json)
        self.assertTrue(res.status_code == 200)
        self.assertTrue("token" in res.json.keys())
        self.assertFalse(res.json["token"] is None)

        token = res.json["token"]

        data = jwt.decode(token, gui.config["SECRET_KEY"], algorithms=["HS256"])
        current_user = User.query.filter_by(public_id=data["public_id"]).first()

        self.assertIsNotNone(current_user)
        self.assertTrue(current_user.user == "GangaGUITestUser")
        self.assertTrue(current_user.role == "Admin")
        self.assertTrue(current_user.verify_password("testpassword"))

    # Tear down, remove temp database file
    def tearDown(self):
        super(TestGangaGUITokenAuthentication, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))

# ******************** EOF ******************** #
