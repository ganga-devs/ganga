from GangaTest.Framework.tests                     import GangaGPITestCase
from GangaDirac.Lib.Files.DiracFile                import DiracFile
#from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
#from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
#from Ganga.Core.exceptions                         import ApplicationConfigurationError, GangaException
from Ganga.GPI                                     import *
#import GangaDirac.Lib.Server.DiracServer as DiracServer
#GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest, tempfile, os

class TestDiracFile(GangaGPITestCase):
    def setUp(self):
        self.df = DiracFile('np', 'ld', 'lfn')
       
    def test__init__(self):
        self.assertEqual(self.df.namePattern, 'np',  'namePattern not initialised as np')
        self.assertEqual(self.df.lfn,         'lfn', 'lfn not initialised as lfn')
        self.assertEqual(self.df.localDir,    'ld',  'localDir not initialised as ld')

        d1=DiracFile()
        self.assertEqual(d1.namePattern, '', 'namePattern not default initialised as empty')
        self.assertEqual(d1.lfn,         '', 'lfn not default initialised as empty')
        self.assertEqual(d1.localDir,    '', 'localDir not default initialised as empty')
        self.assertEqual(d1.locations,   [], 'locations not initialised as empty list')

        d2=DiracFile(namePattern='np', lfn='lfn', localDir='ld')
        self.assertEqual(d2.namePattern, 'np',  'namePattern not keyword initialised as np')
        self.assertEqual(d2.lfn,         'lfn', 'lfn not keyword initialised as lfn')
        self.assertEqual(d2.localDir,    'ld',  'localDir not keyword initialised as ld')
        
    def test__attribute_filter__set__(self):
        self.assertEqual(self.df._attribute_filter__set__('dummyAttribute',12), 12, 'Pass throught of non-specified attribute failed')
        self.assertEqual(self.df._attribute_filter__set__('lfn', 'a/whole/newlfn'), 'a/whole/newlfn', "setting of lfn didn't return the lfn value")
        self.assertEqual(self.df.namePattern, 'newlfn', "Setting the lfn didn't change the namePattern accordingly")
        self.assertEqual(self.df._attribute_filter__set__('localDir','~'), os.path.expanduser('~'), "Didn't fully expand the path")

    def test___on_attribute__set__(self):
        d1 = self.df._on_attribute__set__('','dummyAttrib')
        d2 = self.df._on_attribute__set__(Job()._impl,'outputfiles')
        self.assertEqual(d1, self.df, "didn't create a copy as default action")
        self.assertNotEqual(d2, self.df, "didn't modify properly when called with Job and outputfiles")
        self.assertEqual(d2.namePattern, self.df.namePattern, 'namePattern should be unchanged')
        self.assertEqual(d2.localDir, '', "localDir should be blanked")
        self.assertEqual(d2.lfn, '', "lfn should be blanked")

    def test__repr__(self):
        self.assertEqual(repr(self.df), "DiracFile(namePattern='%s', lfn='%s')" % (self.df.namePattern, self.df.lfn))

    def test_getEnv(self):
        def getDiracEnv():
            print "WOOT"
        self.assertEqual(self.df._env, None, "_env should start out as None")
        self.df.__dict__['getDiracEnv'] = getDiracEnv

