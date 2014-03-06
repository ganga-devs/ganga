################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestStdOut.py,v 1.1 2008-07-17 16:41:12 moscicki Exp $
################################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,write_file
import os
import tempfile

class TestStdOut(GangaGPITestCase):

    def __init__(self):

        self.jobslice = []
        self.file_name = 'id_echo.sh'

    def setUp(self):

        for i in range(2):

            j = Job(application=Executable(),backend=Local())

            scriptString = '''
            #!/bin/sh
            echo "Output from job $1." > out.txt
            echo "Output from job $2." > out2.txt
            '''

            #write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir,self.file_name)

            write_file(fileName, scriptString)

            j.application.exe = 'sh'
            j.application.args = [File(fileName), str(j.id),str(j.id*10)]
            j.outputfiles = [LocalFile('out.txt'),LocalFile('out2.txt')]
            self.jobslice.append(j)

    def runJobSlice(self):

        for j in self.jobslice:
            j.submit()
            
            if not sleep_until_completed(j):
                assert False, 'Test timed out' 
            assert j.status == 'completed'

    def testCanSetStdOutMerge(self):

        from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        sm = SmartMerger()
        sm.files = ['stdout']
        try:
            assert not sm.merge(self.jobslice,tmpdir), 'Merge should fail'
        except PostProcessException:
            pass

                
