from __future__ import absolute_import

import os
import tempfile

import pytest

from GangaTest.Framework.utils import sleep_until_completed, write_file
from Ganga.GPIDev.Base.Proxy import getProxyClass
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException

from ..GangaUnitTest import GangaUnitTest
from .CopySplitter import CopySplitter

CopySplitter = getProxyClass(CopySplitter)


class TestCustomMerger(GangaUnitTest):

    def setUp(self):
        super(TestCustomMerger, self).setUp()
        from Ganga.GPI import Job, Executable, Local, File, LocalFile

        self.jobslice = []
        self.file_name = 'id_echo.sh'

        for i in range(2):

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

    def tearDown(self):
        for j in self.jobslice:
            j.remove()

        super(TestCustomMerger, self).tearDown()

    def testSimpleCustomMerge(self):
        from Ganga.GPI import CustomMerger

        self.runJobSlice()
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        file_name = os.path.join(tmpdir, 'merge.py')
        with open(file_name, 'w') as module_file:
            module_file.write("""from __future__ import print_function
def mergefiles(file_list, output_file):
    '''Free script for merging files'''
    
    with file(output_file,'w') as out:
        for f in file_list:
            print(f, file=out)
    
    return True
        """)

        cm = CustomMerger(module=file_name)
        cm.files = ['out.txt', 'out2.txt']
        assert cm.merge(self.jobslice, tmpdir), 'Merge should complete'
        assert os.path.exists(os.path.join(tmpdir, 'out.txt')), 'out.txt must exist'
        assert os.path.exists(os.path.join(tmpdir, 'out2.txt')), 'out2.txt must exist'

    def testFailJobOnMerge(self):
        from Ganga.GPI import CustomMerger

        self.runJobSlice()
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        file_name = os.path.join(tmpdir, 'merge.py')
        with open(file_name, 'w') as module_file:
            module_file.write("""def mergefiles(file_list, output_file):
    '''Free script for merging files'''
    return False
        """)

        cm = CustomMerger(module=file_name)
        cm.files = ['out.txt', 'out2.txt']
        with pytest.raises(PostProcessException):
            cm.merge(self.jobslice, tmpdir)

        j = self.jobslice[0].copy()
        j.splitter = CopySplitter()
        j.postprocessors = cm
        j.submit()

        sleep_until_completed(j)
        assert j.status == 'failed'
