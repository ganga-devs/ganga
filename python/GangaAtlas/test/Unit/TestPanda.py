try:
    import unittest2 as unittest
except ImportError:
    import unittest

from GangaPanda.Lib.Panda import getLibFileSpecFromLibDS

class TestPanda(unittest.TestCase):

    def test_getLibFileSpecFromLibDS(self):
        print getLibFileSpecFromLibDS("data12_8TeV:data12_8TeV.00206725.physics_JetTauEtmiss.merge.AOD.r4065_p1278_tid01057677_00")
