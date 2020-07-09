from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaGUI.gui import app


class TestGangaGUIDashboard(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        super(TestGangaGUIDashboard, self).setUp(extra_opts=[])
        app.config["TESTING"] = True
        self.app = app.test_client()

    # TODO trivial test, remove later
    def test_dashboard(self):
        res = self.app.get("/")
        assert res.status_code == 200
        