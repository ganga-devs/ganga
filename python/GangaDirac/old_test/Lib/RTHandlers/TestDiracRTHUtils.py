from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.Lib.RTHandlers.DiracRTHUtils import *
#from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
#from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
#from Ganga.Core.exceptions                         import ApplicationConfigurationError, GangaException
from Ganga.GPI import *

# GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest  # , tempfile, os


class TestDiracRTHUtils(GangaGPITestCase):

    def test_API_nullifier(self):
        self.assertEqual(
            API_nullifier(None), None, "Didn't return None for None input ")
        self.assertEqual(
            API_nullifier([]), None, "Didn't return None for empty list input")
        self.assertEqual(
            API_nullifier([12]), [12], "Didn't return [12] for [12] input")
        self.assertEqual(
            API_nullifier(['str']), ['str'], "Didn't return ['str'] for ['str'] input")
