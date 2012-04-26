################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestDiracSplitter.py,v 1.7 2009-06-16 12:03:39 jwilliam Exp $
################################################################################
from __future__ import division
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaLHCb.test import addDiracTestSubmitter
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state

addDiracTestSubmitter()

class TestDiracSplitter(GangaGPITestCase):

    def testSplit(self):

        inputdata = LHCbDataset(files=[
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000919_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000922_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000915_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000920_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000916_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000914_1.dimuon.dst'
            ])

        len_files = len(inputdata.files)
        ds = DiracSplitter()
        ds.filesPerJob = 2        
        result = ds._impl._splitFiles(inputdata._impl)
        assert len(result) >= 3, 'Unexpected number of subjobs'

    def testIgnoreMissing(self):
        
        inputdata = LHCbDataset(files=[
            'LFN:/not/a/file.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000919_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000922_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000915_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000920_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000916_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000914_1.dimuon.dst'
            ])

        ds = DiracSplitter()
        ds.ignoremissing = True
        # shouldn't throw exception
        result = ds._impl._splitFiles(inputdata._impl)
        print 'result = ', result
        ds.ignoremissing = False
        # should throw exception
        threw = False
        try:
            result = ds._impl._splitFiles(inputdata)
            print 'result = ', result
        except:
            threw = True
        assert threw, 'should have thrown exception'

    
        
