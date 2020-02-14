import os
from GangaCore.testlib.GangaUnitTest import GangaUnitTest, start_ganga, stop_ganga

class TestRunfile(GangaUnitTest):
    def test(self):
        from GangaCore.Runtime.GPIFunctions import runfile
        with open('test_file.py', 'w') as f:
            f.write('j = Job()\nj.submit()')
            f.close()
        runfile('test_file.py')
        os.remove('test_file.py')