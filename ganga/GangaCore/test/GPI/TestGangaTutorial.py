# A trivial test to see if Jenkins gives positive result
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaTutorial.Lib.PrimeFactorizer import PrimeFactorizer
from GangaTutorial.Lib.PrimeTableDataset import PrimeTableDataset
from GangaCore.Runtime.GPIexport import exportToGPI
import os, GangaTutorial

TUTDIR = os.path.dirname(GangaTutorial.__file__)
exportToGPI("TUTDIR",TUTDIR,"Objects")

class TestGangaTutorial(GangaUnitTest):

    def test_ganga_tutorial(self):
        
        from GangaCore.GPI import Job

        j = Job()
        j.application = PrimeFactorizer()
        j.application.number = 123456
        j.inputdata = PrimeTableDataset()
        j.inputdata.table_id_lower = 1
        j.inputdata.table_id_upper = 1
        j.submit()

        self.assertIn(j.status, ['submitted','running','completed'])
