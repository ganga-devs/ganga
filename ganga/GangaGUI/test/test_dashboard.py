from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.start import start_gui
from GangaGUI.gui import app


class TestGangaGUIDashboard(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        super(TestGangaGUIDashboard, self).setUp(extra_opts=[])
        app.config["TESTING"] = True
        self.app = app.test_client()

    def test_dashboard(self):
        res = self.app.get("/")
        assert b"Hello GangaGUI" in res.data



        

