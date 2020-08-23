from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui.routes import gui
from GangaCore.Runtime.GPIexport import exportToGPI
from GangaGUI.start import start_gui

# Export start_gui function to GPI
exportToGPI("start_gui", start_gui, "Functions")


# ******************** Test Class ******************** #

# Test dashboard view of the GUI
class TestGangaGUIDashboard(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        super(TestGangaGUIDashboard, self).setUp(extra_opts=[])
        gui.config["TESTING"] = True
        gui.config["LOGIN_DISABLED"] = True
        gui.config["INTERNAL_PORT"] = 5000
        self.app = gui.test_client()

        from GangaCore.GPI import start_gui
        start_gui(only_internal=True, internal_port=5000)

    def test_dashboard(self):
        res = self.app.get("/")
        assert res.status_code == 200

# ******************** EOF ******************** #
