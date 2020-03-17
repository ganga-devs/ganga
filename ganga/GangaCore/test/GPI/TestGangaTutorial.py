from GangaCore.testlib.GangaUnitTest import GangaUnitTest
import os

class TestGangaTutorial(GangaUnitTest):

    def setUp(self, extra_opts=[]):
        os.environ["GANGA_CONFIG_PATH"] = "GangaTutorial/Tutorial.ini"
        super().setUp(extra_opts=[])

    def test_ganga_tutorial(self):
        
        from GangaCore.GPI import Job, PrimeFactorizer, PrimeTableDataset

        j = Job()
        j.application = PrimeFactorizer(number=123456)
        j.inputdata = PrimeTableDataset(table_id_lower=1, table_id_upper=1)
        j.submit()
        self.assertIn(j.status, ['submitted','running','completed'])

    def tearDown(self):
        super().tearDown()
        del os.environ["GANGA_CONFIG_PATH"]