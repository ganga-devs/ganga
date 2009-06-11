from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset.LHCbDataFile import *

class TestLHCbDataFile(GangaGPITestCase):

    def setUp(self):
        self.file_name = '/lhcb/production/DC06/phys-v2-lumi2/00001889/DST/' \
                         '0000/00001889_00000003_5.dst'

    def test_LHCbDataFile_updateReplicaCache(self):
        df = LHCbDataFile('LFN:' + self.file_name)
        df.updateReplicaCache()
        assert df.replicas, 'replicas should now be full'
        df = LHCbDataFile('PFN:' + self.file_name)
        df.updateReplicaCache()
        assert not df.replicas, 'replicas should be empty'

    def test_LHCbDataFile__stripFileName(self):
        df = LHCbDataFile('LFN:' + self.file_name)
        ok = df._stripFileName() == self.file_name
        assert ok, 'stripping should remove LFN:'

    def test_LHCbDataFile_isLFN(self):
        fname = self.file_name
        files = ['LFN:' + fname, 'Lfn:' + fname, 'lfn:' + fname]
        
        for f in files:
            df = LHCbDataFile(f)
            assert df.isLFN(), '%s is LFN' % f

        df = LHCbDataFile('PFN:' + fname)
        assert not df.isLFN(), '%s is NOT LFN' % 'PFN:' + fname

    def test_string_datafile_shortcut(self):
        file = 'LFN:' + self.file_name
        df = string_datafile_shortcut(file,None)
        assert df, 'datafile should not be None'
        assert df.name == file, 'file name should be set properly'
