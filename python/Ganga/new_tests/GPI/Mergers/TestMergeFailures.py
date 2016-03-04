from __future__ import absolute_import

import os

import pytest

from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state
from Ganga.GPIDev.Base.Proxy import getProxyClass

from ..GangaUnitTest import GangaUnitTest
from .MergerTester import MergerTester
from .CopySplitter import CopySplitter

CopySplitter = getProxyClass(CopySplitter)
MergerTester = getProxyClass(MergerTester)


class TestMergeFailures(GangaUnitTest):

    def testMergeThatAlwaysFails(self):
        from Ganga.GPI import Job, Executable, Local, LocalFile

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(files=['out.txt'])

        j.submit()

        sleep_until_completed(j, 60)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeThatAlwaysFailsIgnoreFailed(self):
        from Ganga.GPI import Job, Executable, Local, LocalFile

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(files=['out.txt'], ignorefailed=True)

        j.submit()

        sleep_until_completed(j, 60)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeThatAlwaysFailsOverwrite(self):
        from Ganga.GPI import Job, Executable, Local, LocalFile

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(files=['out.txt'], overwrite=True)

        j.submit()

        sleep_until_completed(j, 60)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeThatAlwaysFailsFlagsSet(self):
        from Ganga.GPI import Job, Executable, Local, LocalFile

        j = Job()
        j.application = Executable(exe='sh', args=['-c', 'echo foo > out.txt'])
        j.backend = Local()
        j.outputfiles = [LocalFile('out.txt')]
        j.splitter = CopySplitter()
        j.postprocessors = MergerTester(
            files=['out.txt'], ignorefailed=True, overwrite=True)

        j.submit()

        sleep_until_completed(j, 60)
        assert j.status == 'failed'
        assert os.path.exists(os.path.join(j.outputdir, 'out.txt.merge_summary')), 'Summary file should be created'

    def testMergeRemoval(self):
        from Ganga.GPI import Job, Executable, Local, LocalFile, jobs

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

        with pytest.raises(KeyError):
            jobs(jobID)
