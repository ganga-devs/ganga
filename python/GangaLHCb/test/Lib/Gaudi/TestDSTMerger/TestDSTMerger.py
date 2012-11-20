import os
import shutil
import tempfile
from GangaLHCb.Lib.Mergers.DSTMerger import _DSTMergeTool as dstTool
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,file_contains
from Ganga.GPIDev.Lib.File import  FileBuffer
from Ganga.GPIDev.Adapters.IMerger import MergerError

class TestDSTMerger(GangaGPITestCase):

    # add some coverage for the public interface
    def test_DSTMerger_merge(self):
        d = DSTMerger()
        d.merge([])

    # Since DSTMerger::merge is just a thin wrapper around this method, there's
    # no real point to including a separate test.
    def test__DSTMergeTool_mergefiles(self):
        
        dst = './test.dst'  # test file w/ 1 event
        assert os.path.exists(dst), 'The original file should exist'
        
        num_files = 5
        dst_files = []
        for i in xrange(num_files):
            dst1 = tempfile.mktemp('.dst')
            shutil.copyfile(dst,dst1)
            assert os.path.exists(dst1), 'Test file must be created'
            dst_files.append(dst1)
        
        fout, outdst = tempfile.mkstemp('.dst')
        tool = dstTool()
        tool.version = 'v23r1'
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

        root = Root(script=outpy,version='5.22.00b')
        j = Job(application=root,backend=Local())
        j.outputsandbox = ['out.txt']
        j.submit()
        
        sleep_until_completed(j)
        assert j.status == 'completed'

        log_file = os.path.join(j.outputdir,'out.txt')
        assert os.path.exists(log_file), 'The output file was NOT created'

        f = open(log_file)
        num_entries = int(f.read())
        f.close()
        
        assert num_entries == num_files, 'Incorrect number of events'
        
        for d in dst_files:
            os.unlink(d)
        os.unlink(outpy)
        os.unlink(outdst)

