######################################################
#
# test_rat_user.py
#
# Test functions to help ensure RATUser application
# works between changes to the GangaSNOplus code.
#
# Author: M Mottram <m.mottram@qmul.ac.uk>
#
######################################################

import os
import unittest
import time

from Ganga import Core
from GangaSNOplus.Lib.RTHandlers.RTRATUser import UserRTHandler
from GangaSNOplus.Lib.Applications.RATUser import RATUser

from Ganga.Utility.logging import getLogger
logger = getLogger(modulename=True)

class TestRatUser(unittest.TestCase):

    def _clean_application(self):
        '''This creates a clean job.
        '''
        self.rat_user = RATUser()
        self.rat_user.nEvents = 1
        temp_dir = "/tmp"
        try:
            temp_dir = os.environ["TMPDIR"]
        except KeyError:
            pass
        self.rat_user.outputDir = temp_dir
        self.rat_user.outputFile = 'temp_ratusertest'
        self.rat_user.ratBaseVersion = 'dev'
        self.rat_user.ratMacro = 'testing_macro.mac'

    def test_user_rthandler(self):
        '''Hack to test that RATUser can be configured successfully
        '''
        self._clean_application()
        rthandler = UserRTHandler()
        j = Job(backend=Local(), application=self.rat_user)
        app_config = j.application._impl.master_configure()
        app_master_config = app_config[1]
        app_sub_config = j.application._impl.configure(app_config)
        app_config = (app_master_config, app_sub_config)
        job_master_config = rthandler.master_prepare(j.application, app_master_config)
        rthandler.prepare(j.application._impl, app_sub_config, app_master_config, job_master_config)
        j.remove()

    def test_user_runs(self):
        '''Test a standard job can run successfully.

        Should only be run on systems with GangaSNOplus configured correctly.
        '''
        self._clean_application()
        logger.info("Running final test; may take a while")
        j = Job(backend=Local(), application=self.rat_user)
        j.submit()
        # Wait for 2 mins
        # Also need to ensure that the monitoring loop runs during this time!
        break_states = ['failed', 'killed', 'new', 'removed', 'unknown']
        timeout = 120
        while j.status not in break_states and j.status!='completed' and timeout>0:
            Core.monitoring_component.runMonitoring(timeout=1)
            time.sleep(1)
            timeout -= 1
        self.assertEqual(j.status, "completed")
        

def main():
    unittest.main()

if __name__ == "__main__":
    main()
