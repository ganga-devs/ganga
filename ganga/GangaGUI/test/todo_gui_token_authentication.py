import os
import uuid
import jwt
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User

currentdir = os.path.dirname(os.path.abspath(__file__))


class TestGangaGUITokenAuthentication(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        super(TestGangaGUITokenAuthentication, self).setUp(extra_opts=[])
        app.config["TESTING"] = True
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(currentdir, "gui_test.sqlite")
        db.create_all()
        tempuser = User(public_id=str(uuid.uuid4()), user="GangaGUITestUser", role="Admin")
        tempuser.store_password_hash("testpassword")
        db.session.add(tempuser)
        db.session.commit()
        self.app = app.test_client()

    def test_database_existence(self):
        self.assertTrue(os.path.exists(os.path.join(currentdir, "gui_test.sqlite")))

    def test_no_credential(self):
        res = self.app.post("/token")
        self.assertTrue(res.status_code == 401)
        self.assertFalse(res.json["success"])
        self.assertFalse("token" in res.json.keys())

    def test_wrong_credential(self):
        res = self.app.post("/token", data={"user": "wronguser", "password": "wrongpassword"})
        self.assertTrue(res.status_code == 401)
        self.assertFalse(res.json["success"])
        self.assertFalse("token" in res.json.keys())

    def test_right_credential(self):
        res = self.app.post("/token", data={"user": "GangaGUITestUser", "password": "testpassword"})
        self.assertTrue(res.status_code == 200)
        self.assertTrue("token" in res.json.keys())
        self.assertFalse(res.json["token"] is None)

        token = res.json["token"]

        data = jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
        current_user = User.query.filter_by(public_id=data["public_id"]).first()

        self.assertIsNotNone(current_user)
        self.assertTrue(current_user.user == "GangaGUITestUser")
        self.assertTrue(current_user.role == "Admin")
        self.assertTrue(current_user.verify_password("testpassword"))

    def tearDown(self):
        super(TestGangaGUITokenAuthentication, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
