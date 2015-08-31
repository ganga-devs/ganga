##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestDiracSplitter.py,v 1.7 2009-06-16 12:03:39 jwilliam Exp $
##########################################################################
from __future__ import division
from Ganga.GPIDev.Adapters.ISplitter import SplittingError
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state

from Ganga.GPI import *

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except Exception as x:
    print(str(x))
    doConfig = True

if doConfig:
    #from GangaGaudi.Lib.Splitters.GaudiInputDataSplitter import GaudiInputDataSplitter
    #from GangaLHCb.Lib.Splitters.SplitByFiles import SplitByFiles
    from GangaLHCb.Lib.LHCbDataset.LHCbDataset import LHCbDataset
    from GangaLHCb.test import addDiracTestSubmitter
    GangaLHCb.test.addDiracTestSubmitter()

# LFNs = [ 'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000919_1.dimuon.dst',
#         'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000922_1.dimuon.dst',
#         'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000915_1.dimuon.dst',
#         'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000920_1.dimuon.dst',
#         'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000916_1.dimuon.dst',
#         'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000914_1.dimuon.dst'
#          ]


class TestDiracSplitter(GangaGPITestCase):

    def testSplit(self):
        j = Job(backend=Dirac())
        j.inputdata = LHCbDataset()
        # j.inputdata.files+=[
        #    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00012533/0000/00012533_00000120_1.dimuon.dst',
        #    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00012368/0000/00012368_00000600_1.dimuon.dst',
        #    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00012368/0000/00012368_00000682_1.dimuon.dst',
        #    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00012368/0000/00012368_00000355_1.dimuon.dst',
        #    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00012368/0000/00012368_00000620_1.dimuon.dst',
        #    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00012533/0000/00012533_00000074_1.dimuon.dst'
        #    ]
        #j.inputdata = LFNs

        # print("try1")
        # somedata = BKQuery('LFN:/LHCb/Collision12/Beam4000GeV-VeloClosed-MagUp/Real Data/Reco14/Stripping20/90000000/DIMUON.DST', dqflag=['OK']).getDataset()#[0:5]
        #j.inputdata = somedata[0:5]
        # print("try2")
        j.inputdata = BKQuery(
            '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagUp/Real Data/Reco14/Stripping20/90000000/DIMUON.DST', dqflag=['OK']).getDataset()[0:5]
        #j.inputdata = BKQuery('LFN:/lhcb/LHCb/Collision10/DIMUON.DST/00010942/0000/00010942_00000218_1.dimuon.dst', dqflag=['OK']).getDataset()[0:5]
        #j.inputdata = BKQuery('/lhcb/LHCb/Collision10/DIMUON.DST/00010942/0000/00010942_00000218_1.dimuon.dst', dqflag=['OK']).getDataset()[0:5]

        #len_files = len(inputdata.files)
        ds = SplitByFiles()
        ds.bulksubmit = False
        ds.filesPerJob = 2
        result = ds.split(j)
        print("Got %s subjobs" % len(result))
        assert len(result) >= 3, 'Unexpected number of subjobs'

    def testIgnoreMissing(self):
        j = Job(backend=Dirac())
        j.inputdata = LHCbDataset()
        # j.inputdata.files+=[
        #    'LFN:/not/a/file.dst',
        #    'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000919_1.dimuon.dst',
        #    'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000922_1.dimuon.dst',
        #    'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000915_1.dimuon.dst',
        #    'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000920_1.dimuon.dst',
        #    'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000916_1.dimuon.dst',
        #    'LFN:/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000914_1.dimuon.dst'
        #    ]
        import copy
        #myLFNs = copy.deepcopy(LFNs)
        #myLFNs = BKQuery('LFN:/lhcb/LHCb/Collision10/DIMUON.DST/00010942/0000/00010942_00000218_1.dimuon.dst', dqflag=['OK']).getDataset()[0:5]

        myLFNs = BKQuery(
            '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagUp/Real Data/Reco14/Stripping20/90000000/DIMUON.DST', dqflag=['OK']).getDataset()[0:5]
        myLFNs.append('LFN:/not/a/file.dst')

        print(myLFNs)

        j.inputdata = myLFNs

        ds = SplitByFiles()
        ds.bulksubmit = False
        ds.ignoremissing = True
        # shouldn't throw exception
        result = ds.split(j)
        print('result = ', result)
        ds.ignoremissing = False
        # should throw exception
        threw = False
        try:
            result = ds.split(j)
            print('result = %s' % str(result))
        except:
            threw = True
        assert threw, 'should have thrown exception'
