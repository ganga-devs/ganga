######################################################
#
# test_job_utils.py
#
# Test utility functions run by both RATUser and RATProd
# applications.
#
# Author: M Mottram <m.mottram@qmul.ac.uk>
#
######################################################

import unittest
from GangaSNOplus.Lib.Applications import job_tools, check_root_output


class TestJobUtils(unittest.TestCase):

    def test_execute_complex_failure(self):
        '''Ensure execute_complex raises CommandExceptions for failed scripts
        '''
        script = 'FunctionFails\n'
        self.assertRaises(job_tools.CommandException, job_tools.execute_complex, script, exit_if_fail=True)

    def test_execute_complex_passes(self):
        '''Ensure execute_complex returns 0, output, error for passed scripts
        '''
        script = 'echo "done"\n'
        rtc, out, err = job_tools.execute_complex(script)
        self.assertEquals(rtc, 0)
        self.assertEquals(out[0], "done")
        

class TestJobChecker(unittest.TestCase):

    def test_version_check(self):
        '''Ensure that rat versions run OK.
        '''
        # Would like a checkIsInstance here, not available until python 2.7
        check_root_output.get_checker('dev')
        check_root_output.get_checker('4.5.0')
        check_root_output.get_checker('4.7.0')
        self.assertRaises(check_root_output.CheckRootException, check_root_output.get_checker, 'none')


def main():
    unittest.main()

if __name__ == "__main__":
    main()
