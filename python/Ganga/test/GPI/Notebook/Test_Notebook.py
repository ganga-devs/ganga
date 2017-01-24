from __future__ import absolute_import
from os.path import dirname, join, abspath, exists
import inspect

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from GangaTest.Framework.utils import sleep_until_completed
from Ganga.Core.exceptions import ApplicationPrepareError, ProtectedAttributeError

class TestNotebook(GangaUnitTest):

#    def setUp(self):
#        """Make sure that the Job object isn't destroyed between tests"""
#        extra_opts = [ ('TestingFramework', 'AutoCleanup', 'False') ]
#        super(TestNotebook, self).setUp(extra_opts=extra_opts)

    def testPrepareCycle(self):
        """
        Prepare a Notebook application and test preparation cycle
        """
        from Ganga.GPI import Job, Notebook

        a = Notebook()
        assert a.is_prepared == None

        a.prepare()
        assert a.is_prepared != None

        b = a.copy()
        assert b.is_prepared != None

        a.unprepare()
        assert a.is_prepared == None
        assert b.is_prepared != None

        a.unprepare()
        assert a.is_prepared == None

        a.prepare()
        try:
            a.prepare()
        except ApplicationPrepareError:
            pass
        except Exception as err:
            assert False, 'Wrong exception: %s' % err

        a.prepare(force=True)
            
    def testAssign(self):
        """Test that assignment works and that properties are protected"""
        from Ganga.GPI import Job, Notebook

        a = Notebook()
        a.version = 999
        a.prepare()
        try:
            a.version = 1
            assert False, 'Assignment after prepare step should not be allowed'
        except ProtectedAttributeError:
            pass
        except Exception as err:
            assert False, 'Wrong exception: %s' % err
        assert a.version == 999
        
    def testRun(self):
        """
        Run a notebook application and check that it is executed
        """
        from Ganga.GPI import Job, Notebook, LocalFile, jobs
        j = Job()
        a = Notebook()

        testfilename = 'Test.ipynb'
        dir = dirname(abspath(inspect.getfile(inspect.currentframe())))
        

        j.inputfiles=[LocalFile(join(dir, testfilename))]
        j.outputfiles=[LocalFile(testfilename)]
        j.submit()
        sleep_until_completed(jobs(0))
        assert j.status in ['completed']
        assert exists(join(j.outputdir, 'Test.ipynb'))
