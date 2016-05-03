from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.Lib.Backends.DiracUtils import result_ok, get_job_ident, get_parametric_datasets, outputfiles_iterator
#from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
#from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
from Ganga.Core.exceptions import BackendError
#from Ganga.GPI                                     import *
#import GangaDirac.Lib.Server.DiracServer as DiracServer
# GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest  # , tempfile, os


class TestDiracUtils(GangaGPITestCase):

    def setUp(self):
        pass

    def test_result_ok(self):

        self.assertFalse(result_ok(None), "Didn't return False with None arg")
        self.assertFalse(
            result_ok(''),  "Didn't return False with non-dict arg")
        self.assertFalse(
            result_ok({}),  "Didn't return False as default dict extraction")
        self.assertFalse(result_ok({'OK': False}), "OK not handled properly")
        self.assertTrue(result_ok({'OK': True}), "Didn't return True")

    def test_get_job_ident(self):

        error_script = """
from DIRAC import Job
"""
        script = """
from DIRAC import Job
j=Job()
j.outputsomething('output.root')
"""

        self.assertRaises(
            BackendError, get_job_ident, error_script.splitlines())
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

        self.assertRaises(
            BackendError, get_parametric_datasets, error_script1.splitlines())
        self.assertRaises(
            BackendError, get_parametric_datasets, error_script2.splitlines())
        self.assertEqual(get_parametric_datasets(script.splitlines()),
                         [['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']],
                         "parametric dataset not correctly extracted")
        self.assertTrue(
            isinstance(get_parametric_datasets(script.splitlines()), list))

    def test_outputfiles_iterator(self):

        ########################################################
        class testfile(object):

            def __init__(this, name, subfiles=[]):
                this.name = name
                this.subfiles = subfiles

        class testfileA(testfile):

            def __init__(this, name, subfiles=[]):
                super(testfileA, this).__init__(name, subfiles)

        class testfileB(testfile):

            def __init__(this, name, subfiles=[]):
                super(testfileB, this).__init__(name, subfiles)

        class testJob(object):

            def __init__(this, outputfiles=[], nc_outputfiles=[]):
                this.outputfiles = outputfiles
                this.non_copyable_outputfiles = nc_outputfiles

        def predA(f):
            return f.name == 'A2'

        def predB(f):
            return f.name == 'BS2'
        ########################################################

        test_job = testJob(outputfiles=[testfileA('A1', subfiles=[testfileA('AS1')]), testfileA('A2'),
                                        testfileB('B1', subfiles=[testfileB('BS1')]), testfileA('A3')],
                           nc_outputfiles=[testfileB('B2'), testfileA('A4'),
                                           testfileB('B3', subfiles=[testfileB('BS2'), testfileB('BS3')]), testfileB('B4')])

        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfile)],
                         ['AS1', 'A2', 'BS1', 'A3', 'B2', 'A4', 'BS2', 'BS3', 'B4'])
        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfileA)],
                         ['AS1', 'A2', 'A3', 'A4'])
        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfileB)],
                         ['BS1', 'B2', 'BS2', 'BS3', 'B4'])

        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfile, include_subfiles=False)],
                         ['A1', 'A2', 'B1', 'A3', 'B2', 'A4', 'B3', 'B4'])
        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfileA, include_subfiles=False)],
                         ['A1', 'A2', 'A3', 'A4'])
        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfileB, include_subfiles=False)],
                         ['B1', 'B2', 'B3', 'B4'])

        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfile, selection_pred=predA)],
                         ['A2'])
        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfile, selection_pred=predB)],
                         ['BS2'])
        self.assertEqual([f.name for f in outputfiles_iterator(test_job, testfile, selection_pred=predB, include_subfiles=False)],
                         [])
