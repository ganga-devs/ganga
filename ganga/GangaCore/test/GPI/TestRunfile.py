import os
from GangaCore.testlib.GangaUnitTest import GangaUnitTest, start_ganga, stop_ganga
from GangaCore.testlib.fixtures import gpi


class TestRunFile(GangaUnitTest):
    def setUp(self):
        start_ganga('./')
        with open('myscript.py', 'w') as f:
            f.write("j = Job()\nj.submit()\n")
    def test_runfile(self):
        from GangaCore.Runtime.GPIFunctions import runfile
        runfile('myscript.py')

    def tearDown(self):
        stop_ganga()
        os.remove('myscript.py')

