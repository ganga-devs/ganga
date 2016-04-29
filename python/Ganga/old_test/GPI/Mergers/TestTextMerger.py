##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestTextMerger.py,v 1.1 2008-07-17 16:41:12 moscicki Exp $
##########################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, file_contains, write_file, sleep_until_state
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
import os
import tempfile


class TestTextMerger(GangaGPITestCase):

    def __init__(self):

        self.jobslice = []
        self.file_name = 'id_echo.sh'

    def setUp(self):

        for i in range(4):

            j = Job(application=Executable(), backend=Local())

            scriptString = '''
            #!/bin/sh
            echo "Output from job $1." > out.txt
            echo "Output from job $2." > out2.txt
            '''

            # write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir, self.file_name)

            write_file(fileName, scriptString)

            j.application.exe = 'sh'
            j.application.args = [File(fileName), str(j.id), str(j.id * 10)]
            j.outputfiles = [LocalFile('out.txt'), LocalFile('out2.txt')]
            self.jobslice.append(j)

    def runJobSlice(self):

        for j in self.jobslice:
            j.submit()

            if not sleep_until_completed(j):
                assert False, 'Test timed out'
            assert j.status == 'completed'

    def tearDown(self):

        for j in self.jobslice:
            j.remove()

    def testDirectMerge(self):

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        tm = TextMerger()
        tm.files = ['out.txt', 'out2.txt']
        assert tm.merge(self.jobslice, tmpdir), 'Merge should complete'

        for j in self.jobslice:
            output = os.path.join(j.outputdir, 'out.txt')
            assert file_contains(output, 'Output from job %d.' %
                                 j.id), 'File must contain the output of each individual job'

        for j in self.jobslice:
            output = os.path.join(j.outputdir, 'out2.txt')
            assert file_contains(output, 'Output from job %d.' % (
                j.id * 10)), 'File must contain the output of each individual job'

    def testFilesMissing(self):

        self.jobslice.append(Job(application=Executable(), backend=Local()))
        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        tm = TextMerger()
        tm.files = ['out.txt']
        try:
            tm.merge(self.jobslice, tmpdir)
        except PostProcessException:
            pass
        else:
            assert False, 'Merge should raise exception'

    def testOverWriteProtection(self):

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        write_file(os.path.join(tmpdir, 'out.txt'), "I shouldn't exist!")

        tm = TextMerger()
        tm.files = ['out.txt']
        try:
            tm.merge(self.jobslice, tmpdir)
        except PostProcessException:
            pass
        else:
            assert False, 'Merge should raise exception'

    def testOverWriteProtectionForce(self):

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        write_file(os.path.join(tmpdir, 'out.txt'), "I shouldn't exist!")

        tm = TextMerger()
        tm.files = ['out.txt']
        assert tm.merge(
            self.jobslice, tmpdir, overwrite=True), 'Merge should pass'

    def testBailOnFailedJobs(self):

        self.runJobSlice()

        # fail if jobs are failed
        self.jobslice[0]._impl.updateStatus('failed')

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        tm = TextMerger()
        tm.files = ['out.txt']
        try:
            tm.merge(self.jobslice, tmpdir)
        except PostProcessException:
            pass
        else:
            assert False, 'Merge should fail with an error'

    def testMergeOnTransitionOneJob(self):

        j = self.jobslice[0]
        j.splitter = CopySplitter()
        j.splitter.number = 5

        tm = TextMerger()
        tm.files = ['out.txt']
        j.postprocessors = tm

        assert j.postprocessors, 'Postprocessors should be set'

        j.submit()
        if not sleep_until_completed(j):
            assert False, 'Test timed out'
        assert j.status == 'completed', 'Job should be finished'

        assert len(j.subjobs) == 5, 'Job should have split correctly'

        output = os.path.join(j.outputdir, 'out.txt')
        assert os.path.exists(output)

        for sj in j.subjobs:
            out_txt = os.path.join(sj.outputdir, 'out.txt')
            assert file_contains(
                output, out_txt), 'File must contain the output of each individual job'

    def testRecursiveMergeOnTransition(self):

        out_list = []

        # add a splitter to each job in jobslice
        for j in self.jobslice:

            j.splitter = CopySplitter()
            j.splitter.number = 5

        # remove one splitter, so that not every job has subjobs
        self.jobslice[-1].splitter = None

        # submit all the jobs and wait
        self.runJobSlice()

        # collect a list of subjob outfiles
        for j in self.jobslice:

            output = os.path.join(j.outputdir, 'out.txt')
            out_list.append(output)
            for sj in j.subjobs:
                out_list.append(os.path.join(sj.outputdir, 'out.txt'))

        # run a merge on a the entire slice
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        tm = TextMerger()
        tm.files = ['out.txt']
        assert tm.merge(self.jobslice, tmpdir), 'Merge should pass'

        outfile = os.path.join(tmpdir, 'out.txt')
        assert os.path.exists(outfile), 'File should have been created'

        for o in out_list:
            assert file_contains(
                outfile, o), 'File must contain output from all subjobs'

    def testMergeWhenSubjobsHaveFailed(self):

        j = self.jobslice[0]
        j.splitter = CopySplitter()
        j.splitter.number = 5
        j.splitter.function_hook = 'makeFirstSubJobFailExecutable'

        tm = TextMerger()
        tm.files = ['out.txt']
        tm.ignorefailed = True
        j.postprocessors = tm

        j.submit()

        if not sleep_until_state(j, state='failed'):
            assert False, 'Test timed out'
        assert j.status == 'failed', 'Job should be failed'

        output = os.path.join(j.outputdir, 'out.txt')
        assert os.path.exists(output)

        for sj in j.subjobs[1:]:
            out_txt = os.path.join(sj.outputdir, 'out.txt')
            assert file_contains(
                output, out_txt), 'File must contain the output of each individual job'

        out_txt = os.path.join(j.subjobs[0].outputdir, 'out.txt')
        assert not file_contains(
            output, out_txt), 'The failed subjob must have been skipped'

    def testMultipleMergeOnTransitionOneJob(self):
        """Use the MultipleMerger to merge all the text files."""

        j = self.jobslice[0]
        j.splitter = CopySplitter()
        j.splitter.number = 5

        tm1 = TextMerger()
        tm1.files = ['out.txt']

        tm2 = TextMerger()
        tm2.files = ['out2.txt']

        j.postprocessors.append(tm1)
        j.postprocessors.append(tm2)

        j.submit()
        if not sleep_until_completed(j):
            assert False, 'Test timed out'
        assert j.status == 'completed', 'Job should be finished'

        for out in ['out.txt', 'out2.txt']:
            output = os.path.join(j.outputdir, out)
            assert os.path.exists(output), 'File %s must exist' % output

            for sj in j.subjobs:
                out_txt = os.path.join(sj.outputdir, out)
                assert file_contains(
                    output, out_txt), 'File must contain the output of each individual job'

    def testSingleJobWithNoSubjobs(self):

        j = Job(application=Executable(), backend=Local())

        tm = TextMerger()
        tm.files = ['stdout', 'stderr']

        j.postprocessors = tm

        j.submit()

        if not sleep_until_completed(j):
            assert False, 'Test timed out'
        assert j.status == 'completed', 'Job should be finished'

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)
        assert tm.merge(j, tmpdir), 'Merge should run but do nothing'

        assert not os.path.exists(
            os.path.join(tmpdir, 'stdout')), 'No merge so no file'

    def testGZipFunction(self):

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        tm = TextMerger()
        tm.compress = True
        tm.files = ['out.txt', 'out2.txt']
        assert tm.merge(self.jobslice, tmpdir), 'Merge should complete'

        for f in tm.files:
            assert os.path.exists(
                os.path.join(tmpdir, '%s.gz' % f)), 'GZip File Should exist'

    def testOutputDirIsJob(self):

        self.runJobSlice()

        tm = TextMerger()
        tm.files = ['out.txt']

        j = self.jobslice[0]
        output_file = os.path.join(j.outputdir, 'out.txt')
        os.remove(output_file)  # file existed from running job

        assert tm.merge(self.jobslice[1:], outputdir=j), 'Merge should run'
        assert os.path.exists(output_file), 'File should have been created'

    def testMultipleMergeOnTransitionOneJobArray(self):
        """Use the MultipleMerger to merge all the text files."""

        j = self.jobslice[0]
        j.splitter = CopySplitter()
        j.splitter.number = 5

        tm1 = TextMerger()
        tm1.files = ['out.txt']

        tm2 = TextMerger()
        tm2.files = ['out2.txt']

        j.postprocessors = [tm1, tm2]

        j.submit()
        if not sleep_until_completed(j):
            assert False, 'Test timed out'
        assert j.status == 'completed', 'Job should be finished'

        for out in ['out.txt', 'out2.txt']:
            output = os.path.join(j.outputdir, out)
            assert os.path.exists(output), 'File %s must exist' % output

            for sj in j.subjobs:
                out_txt = os.path.join(sj.outputdir, out)
                assert file_contains(
                    output, out_txt), 'File must contain the output of each individual job'
