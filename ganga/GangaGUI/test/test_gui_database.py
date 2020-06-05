import os
import uuid
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app, db
from GangaGUI.gui.models import User

currentdir = os.path.dirname(os.path.abspath(__file__))


class TestGangaGUIDatabase(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        super(TestGangaGUIDatabase, self).setUp(extra_opts=[])
        app.config["TESTING"] = True
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(currentdir, "gui_test.sqlite")
        self.app = app.test_client()

    def test_database_creation(self):
        db.create_all()
        self.assertTrue(os.path.exists(os.path.join(currentdir, "gui_test.sqlite")))

        tempuser = User(public_id=str(uuid.uuid4()), user="GangaGUITestUser", role="Admin")
        tempuser.store_password_hash("testpassword")
        db.session.add(tempuser)
        db.session.commit()

        db_users = User.query.all()

        self.assertTrue(len(db_users) == 1)
        self.assertTrue(db_users[0].public_id == tempuser.public_id)
        self.assertTrue(db_users[0].user == tempuser.user)
        self.assertTrue(db_users[0].role == tempuser.role)
        self.assertTrue(db_users[0].password_hash == tempuser.password_hash)
        self.assertTrue(db_users[0].verify_password("testpassword"))

    def tearDown(self):
        super(TestGangaGUIDatabase, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))
