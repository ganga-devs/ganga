from GangaLHCb.Lib.Gaudi.DSTMerger import _DSTMergeTool as dstTool

from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,file_contains,write_file,sleep_until_state

from Ganga.GPIDev.Lib.File import  FileBuffer

import os
import shutil
import tempfile

from Ganga.GPIDev.Adapters.IMerger import MergerError

class TestDSTMerger(GangaGPITestCase):
    
    
    def testDSTMerge(self):
        
        #copy the file into a few places
        dst = './test.dst'
        
        assert os.path.exists(dst), 'The original file should exist'
        
        num_files = 5
        
        dst_files = []
        for i in xrange(num_files):
            dst1 = tempfile.mktemp('.dst')
            shutil.copyfile(dst,dst1)
            assert os.path.exists(dst1), 'Test file must be created'
            dst_files.append(dst1)
        
        outdst = tempfile.mktemp('.dst')
        
        tool = dstTool()
        tool.mergefiles(dst_files,outdst)
        assert os.path.exists(outdst), 'Test file must be created'
        
        test_script = """#!/usr/bin/env python
import ROOT

def xxxtest():
    dst = ROOT.TFile.Open('%s')
    tree = ROOT.gDirectory.Get('_Event')
    entries = tree.GetEntries()
    
    try:
        out = file('out.txt','w')
        print >> out, entries
    finally:
        out.close()

    dst.Close()
    
if __name__ == '__main__':
    xxxtest()
        """ % outdst
        
        outpy = '/tmp/xxxtest.py'
        try:
            outpy_file = file(outpy,'w')
            outpy_file.write(test_script)
        finally:
            outpy_file.close()
        
        j = Job(application = Root(script = outpy,version = '5.18.00d'), backend = Local())
        j.outputsandbox = ['out.txt']
        j.submit()
        
        sleep_until_completed(j)
        assert j.status == 'completed'

        log_file = os.path.join(j.outputdir,'out.txt')
        assert os.path.exists(log_file), 'The output file must have been created'
        assert file_contains(log_file,str(num_files)), 'It must contain the right number'
        
        for d in dst_files:
            os.unlink(d)
        os.unlink(outpy)
        os.unlink(outdst)