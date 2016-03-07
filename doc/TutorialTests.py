try:
    import unittest2 as unittest
except ImportError:
    import unittest

from ganga import *

class TutorialTests(unittest.TestCase):

    # A set of tests that are explicitly quoted in the tutorial docs
    # DON'T CHANGE THE COMMENTS as these are used to pick out the code!
    # Should also add more checks for completed jobs, etc.
    def testInstallAndBasicUsage(self):

        # -- INSTALLANDBASICUSAGE HELP START
        help(Job)
        # -- INSTALLANDBASICUSAGE HELP STOP

        # -- INSTALLANDBASICUSAGE SUBMIT START
        j = Job()
        j.submit()
        # -- INSTALLANDBASICUSAGE SUBMIT STOP

        # -- INSTALLANDBASICUSAGE JOBS START
        jobs(0)
        # -- INSTALLANDBASICUSAGE JOBS STOP

        # -- INSTALLANDBASICUSAGE JOBSAPP START
        jobs(0).application
        # -- INSTALLANDBASICUSAGE JOBSAPP STOP

        # -- INSTALLANDBASICUSAGE EXECFILE START
        open('submit.py', 'w').write("""
j = Job()
j.submit()
""")
        execfile('submit.py')
        # -- INSTALLANDBASICUSAGE EXECFILE STOP
