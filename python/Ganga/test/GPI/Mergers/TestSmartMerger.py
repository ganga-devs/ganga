from __future__ import absolute_import

import os
import tempfile

from GangaTest.Framework.utils import sleep_until_completed, file_contains, write_file
from Ganga.Lib.Mergers.Merger import findFilesToMerge

from ..GangaUnitTest import GangaUnitTest


class TestSmartMerger(GangaUnitTest):

    def setUp(self):
        super(TestSmartMerger, self).setUp()
        from Ganga.GPI import Job, Executable, Local, File, LocalFile, config

        config['Mergers']['associate'] = {'txt': 'TextMerger'}

        self.jobslice = []
        self.file_name = 'id_echo.sh'

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
            assert sleep_until_completed(j), 'Timeout on job submission: job is still not finished'
            assert j.status == 'completed'

    def tearDown(self):

        for j in self.jobslice:
            j.remove()

        super(TestSmartMerger, self).tearDown()

    def testNoFilesSpecifiedAllSame(self):
        from Ganga.GPI import LocalFile, Job

        files = [LocalFile('foo.root'), LocalFile(
            'bar.root'), LocalFile('out.log')]

        j1 = Job(outputfiles=files)
        j2 = Job(outputfiles=files)

        assert j1.outputfiles == j2.outputfiles, 'File lists should be the same'

        assert findFilesToMerge([j1, j2]) == ['foo.root', 'bar.root', 'out.log'], 'Should merge all files'

    def testNoFilesSpecifiedSomeOverlap(self):
        from Ganga.GPI import LocalFile, Job

        j1 = Job(outputfiles=[LocalFile('foo.root'), LocalFile('bar.root'), LocalFile('out.log')])
        j2 = Job(outputfiles=[LocalFile('a.root'), LocalFile('b.root'), LocalFile('out.log')])

        assert findFilesToMerge([j1, j2]) == ['out.log'], 'Should merge only some files'

    def testNoFilesSpecifiedNoOverlap(self):
        from Ganga.GPI import LocalFile, Job

        j1 = Job(outputfiles=[LocalFile('foo.root'), LocalFile('bar.root'), LocalFile('out.log')])
        j2 = Job(outputfiles=[LocalFile('a.root'), LocalFile('b.root'), LocalFile('c.log')])

        assert findFilesToMerge([j1, j2]) == [], 'Should merge no files'

    def testActualMergeJob(self):
        from Ganga.GPI import SmartMerger

        self.runJobSlice()
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        sm = SmartMerger()
        assert sm.merge(self.jobslice, tmpdir), 'Merge should complete'

        for j in self.jobslice:
            output = os.path.join(j.outputdir, 'out.txt')
            assert file_contains(output, 'Output from job %d.' % j.id), 'File must contain the output of each individual job'

        for j in self.jobslice:
            output = os.path.join(j.outputdir, 'out2.txt')
            assert file_contains(output, 'Output from job %d.' % (j.id * 10)), 'File must contain the output of each individual job'
