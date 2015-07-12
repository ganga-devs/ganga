##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestMergeFailures.py,v 1.1.4.1 2009-07-24 13:39:40 ebke Exp $
##########################################################################
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state
import os


class TestMergeFailures(GangaGPITestCase):

    def testMergeThatAlwaysFails(self):

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(files=['out.txt'])

        j.submit()

        sleep_until_completed(j, 120)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(
            j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeThatAlwaysFailsIgnoreFailed(self):

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(files=['out.txt'], ignorefailed=True)

        j.submit()

        sleep_until_completed(j, 120)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(
            j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeThatAlwaysFailsOverwrite(self):

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(files=['out.txt'], overwrite=True)

        j.submit()

        sleep_until_completed(j, 120)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(
            j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeThatAlwaysFailsFlagsSet(self):

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(
            files=['out.txt'], ignorefailed=True, overwrite=True)

        j.submit()

        sleep_until_completed(j, 120)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(
            j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeRemoval(self):

        # see Savannah 33710
        j = Job()
        jobID = j.id
        # job will run for at least 20 seconds
        j.application = Executable(
            exe='sh', args=['-c', 'sleep 20; echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(files=['out.txt'])

        j.postprocessors[0].ignorefailed = True
        j.postprocessors[0].alwaysfail = True
        j.postprocessors[0].wait = 10

        j.submit()
        sleep_until_state(j, state='running')
        j.remove()

        try:
            jobs(jobID)
            assert False, 'Job should not be found'
        except KeyError:
            pass
