# A trivial test to see if Jenkins gives positive result
from GangaCore.testlib.GangaUnitTest import GangaUnitTest

class TestGangaTutorial(GangaUnitTest):

    def test_ganga_tutorial(self):
        
        from GangaCore.GPI import Job

        j = Job()
        j.submit()

        # j = Job()
        # j.application = PrimeFactorizer(number=123456)
        # j.inputdata = PrimeTableDataset(table_id_lower=1, table_id_upper=1)
        # j.submit()

        self.assertIn(j.status, ['submitted','running','completed'])
