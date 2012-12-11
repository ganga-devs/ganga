from GangaTest.Framework.tests                     import GangaGPITestCase
from GangaDirac.Lib.Backends.DiracUtils            import result_ok, get_job_ident, get_parametric_datasets
#from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
#from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
from Ganga.Core.exceptions                         import BackendError
#from Ganga.GPI                                     import *
#import GangaDirac.Lib.Server.DiracServer as DiracServer
#GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest#, tempfile, os

class TestDiracUtils(GangaGPITestCase):
    def setUp(self):
        pass

    def test_result_ok(self):

        self.assertFalse(result_ok(None),"Didn't return False with None arg")
        self.assertFalse(result_ok(''),  "Didn't return False with non-dict arg")
        self.assertFalse(result_ok({}),  "Didn't return False as default dict extraction")
        self.assertFalse(result_ok({'OK':False}), "OK not handled properly")
        self.assertTrue(result_ok({'OK':True}), "Didn't return True")

    def test_get_job_ident(self):

        error_script = """
from DIRAC import Job
"""
        script = """
from DIRAC import Job
j=Job()
j.outputsomething('output.root')
"""
        
        self.assertRaises(BackendError, get_job_ident, error_script.splitlines())
        self.assertEqual('j', get_job_ident(script.splitlines()),
                         "Didn't get the right job ident")

    def test_get_parametric_dataset(self):
        error_script1 = """
from DIRAC import Job
j=Job()
j.outputsomething('output.root')
"""
        error_script2 = """
from DIRAC import Job
j=Job()
j.outputsomething('output.root')
j.setParametricInputData([['a','b','c'],['d','e','f'],['g','h','i']])
j.setParametricInputData([['a','b','c'],['d','e','f'],['g','h','i']])
"""
        script = """
from DIRAC import Job
j=Job()
j.outputsomething('output.root')
j.setParametricInputData([['a','b','c'],['d','e','f'],['g','h','i']])
j.somethingelse('other')
"""

        self.assertRaises(BackendError, get_parametric_datasets, error_script1.splitlines())
        self.assertRaises(BackendError, get_parametric_datasets, error_script2.splitlines())
        self.assertEqual(get_parametric_datasets(script.splitlines()),
                         [['a','b','c'],['d','e','f'],['g','h','i']],
                         "parametric dataset not correctly extracted")
        self.assertTrue(isinstance(get_parametric_datasets(script.splitlines()),list))
