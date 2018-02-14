from __future__ import absolute_import
from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.testlib.mark import external
from GangaCore.testlib.monitoring import run_until_completed, run_until_state
import os


class TestExeDiracRTHandler(GangaUnitTest):

    @external
    def testFailure(self):
        """
        Check a simple job fails and raises the correct exception
        """
        from GangaCore.GPI import Job, Dirac, Executable
        import time
        j =  Job(backend = Dirac())
        j.application = Executable(exe = 'ech')
        j.application.args = ['Hello World']
        j.submit()
        assert run_until_state(j, 'failed', 220)
        filepath = os.path.join(j.outputdir, 'Ganga_Executable.log')
        i = 0
        while not os.path.exists(filepath) and i < 10:
            i=i+1
            time.sleep(5)

        found = False
        with open(filepath, 'r') as f:
            for line in f:
                if "Exception occured in running process: ['ech', 'Hello World']" in line:
                    found = True
        assert found
