from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,file_contains,write_file,sleep_until_state
import os
import tempfile

from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException

class TestCustomMerger(GangaGPITestCase):
    
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
            j.outputsandbox = ['out.txt','out2.txt']
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


    def testSimpleCustomMerge(self):
        
        self.runJobSlice()
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)
        
        file_name = os.path.join(tmpdir,'merge.py')
        module_file = file(file_name,'w')
        module_file.write("""def mergefiles(file_list, output_file):
    '''Free script for merging files'''
    
    try:
        out = file(output_file,'w')
        for f in file_list:
            print >> out, f
    finally:
        out.close()
    
    return 0
        """)
        module_file.close()
        
        cm = CustomMerger(module = file_name)
        cm.files = ['out.txt','out2.txt']
        assert cm.merge(self.jobslice,tmpdir), 'Merge should complete'
        assert os.path.exists(os.path.join(tmpdir,'out.txt')),'out.txt must exist'
        assert os.path.exists(os.path.join(tmpdir,'out2.txt')),'out2.txt must exist'
        
    def testFailJobOnMerge(self):
        
        self.runJobSlice()
        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)
        
        file_name = os.path.join(tmpdir,'merge.py')
        module_file = file(file_name,'w')
        module_file.write("""def mergefiles(file_list, output_file):
    '''Free script for merging files'''
    return 1
        """)
        module_file.close()
        
        cm = CustomMerger(module = file_name)
        cm.files = ['out.txt','out2.txt']
        try:
            cm.merge(self.jobslice,tmpdir)
            assert False,'Merge should fail'
        except PostProcessException:
            pass
        
        j = self.jobslice[0].copy()
        j.splitter = CopySplitter()
        j.postprocessors = cm
        j.submit()
        
        sleep_until_completed(j)
        assert j.status == 'failed'
        
        
        
