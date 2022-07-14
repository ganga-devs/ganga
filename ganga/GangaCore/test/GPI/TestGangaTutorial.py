from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaTutorial.Lib.PrimeFactorizer import PrimeFactorizer
from GangaTutorial.Lib.PrimeTableDataset import PrimeTableDataset
from GangaTutorial.Lib.PrimeFactorizerSplitter import PrimeFactorizerSplitter
from GangaCore.Runtime.GPIexport import exportToGPI
import os
import GangaTutorial

# Exporting TUTDIR to GPI
TUTDIR = os.path.dirname(GangaTutorial.__file__)
exportToGPI("TUTDIR", TUTDIR, "Objects")


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

        self.assertIn(j.status, ['submitted', 'running', 'completed'])

    def test_ganga_tutorial_subjobs(self):

        from GangaCore.GPI import Job

        j = Job()
        j.splitter = PrimeFactorizerSplitter()
        j.splitter.numsubjobs = 2
        j.application = PrimeFactorizer()
        j.application.number = 123456
        j.inputdata = PrimeTableDataset()
        j.inputdata.table_id_lower = 1
        j.inputdata.table_id_upper = 2
        j.submit()

        assert len(j.subjobs) == 2
        assert j.status in ['submitted', 'running', 'completed']

        for sj in j.subjobs:
            assert sj.status in ['submitted', 'running', 'completed']
