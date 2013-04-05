from GangaTest.Framework.tests                     import GangaGPITestCase
#from GangaDirac.Lib.Files.DiracFile                import DiracFile
#from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
#from Ganga.GPIDev.Adapters.StandardJobConfig       import StandardJobConfig
#from Ganga.Core.exceptions                         import ApplicationConfigurationError, GangaException
from Ganga.GPI                                     import *
#import GangaDirac.Lib.Server.DiracServer as DiracServer
#GangaTest.Framework.utils defines some utility methods
from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest, tempfile, os

class TestDiracFile(GangaGPITestCase):
    def setUp(self):
        script='''#!/bin/bash
echo HelloWorld > a.root
echo WorldHello > b.root
'''
        tmpf = tempfile.NamedTemporaryFile(delete=False)
        tmpf.write(script)
        self.root, self.filename = os.path.split(tmpf.name)
        tmpf.close()
        self.filepath = os.path.join(self.root, self.filename)
             
    def tearDown(self):
        os.remove(self.filepath)

    def test_standalone_put(self):
        tmpf = tempfile.NamedTemporaryFile(delete=False)
        tmpf.write('TESTFILE')
        root, filename = os.path.split(tmpf.name)
        tmpf.close()
        
        d1=DiracFile(filename, root)
        d1.put()
        self.assertNotEqual(d1.lfn,'','lfn not set upon return')
        self.assertNotEqual(d1.guid,'','guid not set upon return')
        self.assertNotEqual(d1.locations,[],'lfn not set upon return')
        d1.remove()
        os.remove(os.path.join(root, filename))

    def test_local_job_put_single_file(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), outputfiles=[DiracFile('a.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j.outputfiles),1)
        self.assertEqual(j.outputfiles[0].namePattern,'a.root')
        self.assertNotEqual(j.outputfiles[0].lfn,'')
        self.assertNotEqual(j.outputfiles[0].guid,'')
        self.assertNotEqual(j.outputfiles[0].locations,[])
        j.outputfiles[0].remove()

    def test_local_job_put_multiple_files(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), outputfiles=[DiracFile('a.root'), DiracFile('b.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j.outputfiles),2)
        for df in j.outputfiles:
            self.assertIn(df.namePattern,['a.root','b.root'])
            self.assertNotEqual(df.lfn,'')
            self.assertNotEqual(df.guid,'')
            self.assertNotEqual(df.locations,[])
            df.remove()

    def test_local_job_put_wildcard_files(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), outputfiles=[DiracFile('*.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j.outputfiles),2)
        for df in j.outputfiles:
            self.assertIn(df.namePattern,['a.root','b.root'])
            self.assertNotEqual(df.lfn,'')
            self.assertNotEqual(df.guid,'')
            self.assertNotEqual(df.locations,[])
            df.remove()

    def test_local_job_wildcard_expansion(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), outputfiles=[DiracFile('*.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j._impl.outputfiles),1)
        self.assertEqual(j._impl.outputfiles[0].namePattern, '*.root')
        self.assertEqual(len(j._impl.outputfiles[0].subfiles),2)
        for df in j._impl.outputfiles[0].subfiles:
            self.assertIn(df.namePattern, ['a.root','b.root'])
            df.remove()

    def test_Dirac_job_put_single_file(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), backend=Dirac(), outputfiles=[DiracFile('a.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j.outputfiles),1)
        self.assertEqual(j.outputfiles[0].namePattern,'a.root')
        self.assertNotEqual(j.outputfiles[0].lfn,'')
        self.assertNotEqual(j.outputfiles[0].guid,'')
        self.assertNotEqual(j.outputfiles[0].locations,[])
        j.outputfiles[0].remove()

    def test_Dirac_job_put_multiple_files(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), backend=Dirac(), outputfiles=[DiracFile('a.root'), DiracFile('b.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j.outputfiles),2)
        for df in j.outputfiles:
            self.assertIn(df.namePattern,['a.root','b.root'])
            self.assertNotEqual(df.lfn,'')
            self.assertNotEqual(df.guid,'')
            self.assertNotEqual(df.locations,[])
            df.remove()

    def test_Dirac_job_put_wildcard_files(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), backend=Dirac(), outputfiles=[DiracFile('*.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j.outputfiles),2)
        for df in j.outputfiles:
            self.assertIn(df.namePattern,['a.root','b.root'])
            self.assertNotEqual(df.lfn,'')
            self.assertNotEqual(df.guid,'')
            self.assertNotEqual(df.locations,[])
            df.remove()

    def test_Dirac_job_wildcard_expansion(self):
        j=Job(application=Executable(exe=File(self.filepath),args=[]), backend=Dirac(), outputfiles=[DiracFile('*.root')])
        j.submit()
        sleep_until_completed(j)

        self.assertEqual(len(j._impl.outputfiles),1)
        self.assertEqual(j._impl.outputfiles[0].namePattern, '*.root')
        self.assertEqual(len(j._impl.outputfiles[0].subfiles),2)
        for df in j._impl.outputfiles[0].subfiles:
            self.assertIn(df.namePattern, ['a.root','b.root'])
            df.remove()
