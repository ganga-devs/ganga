# from GangaCore.testlib.GangaUnitTest import GangaUnitTest
# from GangaGUI.gui.routes import gui
#
#
# class TestGangaGUIDashboard(GangaUnitTest):
#
#     def setUp(self, extra_opts=[]):
#         super(TestGangaGUIDashboard, self).setUp(extra_opts=[])
#         gui.config["TESTING"] = True
#         self.app = gui.test_client()
#
#     # TODO trivial test, remove later
#     def test_dashboard(self):
#         res = self.app.get("/")
#         assert res.status_code == 200
#