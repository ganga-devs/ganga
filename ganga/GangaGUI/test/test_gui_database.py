import os
import uuid
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui.routes import gui, db, User
from GangaCore.Runtime.GPIexport import exportToGPI
from GangaGUI.start import start_gui

# Export start_gui function to GPI
exportToGPI("start_gui", start_gui, "Functions")

# Path of current directory
currentdir = os.path.dirname(os.path.abspath(__file__))


# ******************** Test Class ******************** #


class TestGangaGUIDatabase(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        super(TestGangaGUIDatabase, self).setUp(extra_opts=[])
        gui.config["TESTING"] = True
        gui.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///" + os.path.join(currentdir, "gui_test.sqlite")
        gui.config["LOGIN_DISABLED"] = True
        gui.config["INTERNAL_PORT"] = 5000

        # Flask testing client
        self.app = gui.test_client()

        # Start internal API server
        from GangaCore.GPI import start_gui
        start_gui(only_internal=True, internal_port=5000)

    # Test creation of database file
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

    # Teardown, remove dummy database file
    def tearDown(self):
        super(TestGangaGUIDatabase, self).tearDown()
        db.session.remove()
        db.drop_all()
        os.remove(os.path.join(currentdir, "gui_test.sqlite"))

# ******************** EOF ******************** #