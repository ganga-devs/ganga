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
from GangaLHCb.Lib.DIRAC.DiracSplitter import DiracSplitter
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
addDiracTestSubmitter()

class TestDiracSplitter(GangaGPITestCase):

    def testSplit(self):
        j=Job()
        j.inputdata = LHCbDataset()
        j.inputdata.files+=[
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000919_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000922_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000915_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000920_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000916_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000914_1.dimuon.dst'
            ]
        j.inputdata.metadata=None

        #len_files = len(inputdata.files)
        ds = DiracSplitter()
        ds.filesPerJob = 2        
        result = ds.split(j)
        assert len(result) >= 3, 'Unexpected number of subjobs'

    def testIgnoreMissing(self):
        j=Job()
        j.inputdata = LHCbDataset()
        j.inputdata.files+=[
            'LFN:/not/a/file.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000919_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000922_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000915_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000920_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000916_1.dimuon.dst',
            'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000914_1.dimuon.dst'
            ]
        j.inputdata.metadata=None

        ds = DiracSplitter()
        ds.ignoremissing = True
        # shouldn't throw exception
        result = ds.split(j)
        print 'result = ', result
        ds.ignoremissing = False
        # should throw exception
        threw = False
        try:
            result = ds.split(j)
            print 'result = ', result
        except:
            threw = True
        assert threw, 'should have thrown exception'

    
        
