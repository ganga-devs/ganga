from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.Lib.Backends.DiracBase import DiracBase
#from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import get_share_path
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
#from Ganga.Core.exceptions                         import ApplicationConfigurationError, GangaException
from Ganga.GPI import *
#import GangaDirac.Lib.Server.DiracServer as DiracServer
# GangaTest.Framework.utils defines some utility methods
#from GangaTest.Framework.utils import sleep_until_completed,sleep_until_state
import unittest
import tempfile
import os


class TestDiracBase(GangaGPITestCase):

    def setUp(self):
        self.returnObject = None
        self.toCheck = {}

        def execute(command, timeout=60, env=None, cwd=None, shell=False):
            import inspect
            frame = inspect.currentframe()
            fedInVars = inspect.getargvalues(frame).locals
            del frame

            for key, value in self.toCheck.iteritems():
                if key in fedInVars:
                    self.assertEqual(fedInVars[key], value)

            return self.returnObject

        def add_process(this, command, command_args=(), command_kwargs={}, timeout=60, env=None, cwd=None, shell=False,
                        priority=5, callback_func=None, callback_args=(), callback_kwargs={}):
            import inspect
            frame = inspect.currentframe()
            fedInVars = inspect.getargvalues(frame).locals
            del frame

            for key, value in self.toCheck.iteritems():
                if key in fedInVars:
                    self.assertEqual(fedInVars[key], value)

            return self.returnObject

        self.db = DiracBase()
        from Ganga.Core.GangaThread.WorkerThreads.WorkerThreadPool import WorkerThreadPool
        setattr(sys.modules[self.db.__module__], 'execute', execute)
        setattr(WorkerThreadPool, "add_process", add_process)
        self.script = """
# dirac job created by ganga
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
###DIRAC_IMPORT###
###DIRAC_JOB_IMPORT###
dirac = ###DIRAC_OBJECT###
j = ###JOB_OBJECT###

# default commands added by ganga
j.setName('###NAME###')
j.setApplicationScript('###APP_NAME###','###APP_VERSION###','###APP_SCRIPT###',logFile='###APP_LOG_FILE###')
j.setRootPythonScript('###ROOTPY_VERSION###', '###ROOTPY_SCRIPT###', ###ROOTPY_ARGS###, '###ROOTPY_LOG_FILE###')
j.setRootMacro('###ROOT_VERSION###', '###ROOT_MACRO###', ###ROOT_ARGS###, '###ROOT_LOG_FILE###')
j.setExecutable('###EXE###','###EXE_ARG_STR###','###EXE_LOG_FILE###')
j.setExecutionEnv(###ENVIRONMENT###)
j.setInputSandbox(##INPUT_SANDBOX##)
j.setOutputSandbox(###OUTPUT_SANDBOX###)
j.setInputData(###INPUTDATA###)
j.setParametricInputData(###PARAMETRIC_INPUTDATA###)
j.setOutputData(###OUTPUTDATA###,OutputPath='###OUTPUT_PATH###',OutputSE=###OUTPUT_SE###)
j.setSystemConfig('###PLATFORM###')

# <-- user settings
###SETTINGS###
# user settings -->

# diracOpts added by user
###DIRAC_OPTS###

# submit the job to dirac
result = dirac.submit(j)

output(result)
"""

    def test__setup_subjob_dataset(self):
        self.assertEqual(self.db._setup_subjob_dataset([]), None, 'Not None')

    def test__addition_sandbox_content(self):
        self.assertEqual(
            self.db._addition_sandbox_content(None), [], 'Not empty list')

    def test__setup_bulk_subjobs(self):
        from Ganga.Core import BackendError
        from Ganga.GPIDev.Lib.Dataset.Dataset import Dataset

        fd, name = tempfile.mkstemp()
        file = os.fdopen(fd, 'w')
        file.write(self.script.replace('###PARAMETRIC_INPUTDATA###',
                                       str([['a'], ['b']])))
        file.close()
        self.assertRaises(BackendError,
                          self.db._setup_bulk_subjobs,
                          [],
                          name)

        d = Dirac()
        j = Job(application=DaVinci(),
                splitter=SplitByFiles(),
                #              merger=SmartMerger(),
                inputdata=Dataset(),
                backend=d)
        d._impl._parent = j._impl
#        self.db._parent = j._impl
        dirac_ids = [123, 456]

        def _setup_subjob_dataset(dataset):
            self.assertTrue(
                dataset in [['a'], ['b']], 'dataset not passed properly')
            return None

        setattr(self.db, '_setup_subjob_dataset', _setup_subjob_dataset)
        self.assertTrue(
            d._impl._setup_bulk_subjobs(dirac_ids, name), 'didnt run')

        self.assertEqual(len(j.subjobs), len(dirac_ids), 'didnt work')
        for id, backend_id, subjob in zip(range(len(dirac_ids)), dirac_ids, j.subjobs):
            self.assertEqual(id, subjob.id, 'ids dont match')
            self.assertEqual(
                backend_id, subjob.backend.id, 'backend.ids dont match')
            self.assertTrue(isinstance(
                subjob.application._impl, j.application._impl.__class__), 'apps dont match')
            self.assertEqual(subjob.splitter, None, 'splitter not done')
 #           self.assertEqual(subjob.merger, None,'mergers dont match')
            # self.assertEqual(subjob.inputdata, None)#,'inputdata dont match')
            self.assertTrue(isinstance(
                subjob.backend._impl, j.backend._impl.__class__), 'backend dont match')

        os.remove(name)

    def test__common_submit(self):
        from Ganga.Core import BackendError
        j = Job(backend=self.db)
        self.db._parent = j._impl

        fd, name = tempfile.mkstemp()
        file = os.fdopen(fd, 'w')
        file.write(self.script.replace('###PARAMETRIC_INPUTDATA###',
                                       str([['a'], ['b']])))
        file.close()
        self.returnObject = {}
#        class errorserver:
#            def execute(this, dirac_cmd):
#                return {}

        self.db.id = 1234
        self.db.actualCE = 'test'
        self.db.status = 'test'
        self.assertRaises(BackendError,
                          self.db._common_submit,
                          name)
#                          errorserver())

        self.assertEqual(self.db.id, None, 'id not None')
        self.assertEqual(self.db.actualCE, None, 'actualCE not None')
        self.assertEqual(self.db.status, None, 'status not None')

#        class server:
#            def __init__(this, list_return=False):
#                this.list_return = list_return
#            def execute(this, dirac_cmd):
#                self.assertEqual(dirac_cmd,"execfile('%s')"%name,'cmd wrong')
#                if this.list_return is True:
#                    return {'OK':True, 'Value': [123,456]}
#                return {'OK':True, 'Value': 12345}

        self.toCheck = {'command': "execfile('%s')" % name}
        self.returnObject = {'OK': True, 'Value': 12345}
        self.assertTrue(self.db._common_submit(name))
        self.assertEqual(self.db.id, 12345, 'id not set')

        def _setup_bulk_subjobs(dirac_ids, dirac_script):
            self.assertEqual(dirac_ids, [123, 456], 'ids not equal')
            self.assertEqual(dirac_script, name, 'dirac script not equal')
            return True

        setattr(self.db, '_setup_bulk_subjobs', _setup_bulk_subjobs)
        self.returnObject = {'OK': True, 'Value': [123, 456]}
        self.assertTrue(self.db._common_submit(name))

        os.remove(name)

    def test_submit(self):
        j = Job(backend=self.db)
        self.db._parent = j._impl

        file1 = tempfile.NamedTemporaryFile('w')
        file2 = tempfile.NamedTemporaryFile('w')
        file3 = tempfile.NamedTemporaryFile('w')
        sjc = StandardJobConfig(exe=self.script,
                                inputbox=[File(file1.name)._impl,
                                          File(file2.name)._impl,
                                          File(file3.name)._impl],
                                outputbox=['d', 'e', 'f'])

        def _addition_sandbox_content(subjobconfig):
            self.assertEqual(subjobconfig, sjc, 'config objects not equal')
            return ['g']

        def _common_submit(dirac_script):
            # this needs to change to workerpool
            #from GangaDirac.Lib.Server.DiracClient import DiracClient
            #self.assertTrue(isinstance(server, DiracClient),'not a dirac client')
            f = open(dirac_script, 'r')
            script = f.read()
            f.close()
            self.assertNotEqual(script, self.script, 'script not changed')
            self.assertEqual(self.script.replace('##INPUT_SANDBOX##',
                                                 str(['a', 'b', 'c'] +
                                                     [os.path.join(j._impl.getInputWorkspace().getPath(),
                                                                   '_input_sandbox_0.tgz')] +
                                                     ['g'])),
                             script, 'script not what it should be')

            return True

        setattr(
            self.db, '_addition_sandbox_content', _addition_sandbox_content)
        setattr(self.db, '_common_submit', _common_submit)

        self.assertTrue(self.db.submit(sjc, ['a', 'b', 'c']), 'didnt run')

        file1.close()
        file2.close()
        file3.close()

    def test_resubmit(self):
        def _resubmit():
            return '_resubmit run ok'
        setattr(self.db, '_resubmit', _resubmit)
        self.assertEqual(self.db.resubmit(), '_resubmit run ok')

    def test__resubmit(self):
        from Ganga.Core import BackendError
 #       class server:
 #           def execute(this, dirac_cmd):
 #               return {}

        def _common_submit(dirac_script):
            return '_comon_submit run ok'
        setattr(self.db, '_common_submit', _common_submit)

        masterj = Job(backend=self.db)
        j = Job(backend=self.db)
        j._impl.master = masterj._impl

        # problem as keeps finding job 0 in main repository which has this
        # 'dirac-script' file
        self.db._parent = masterj._impl
        self.returnObject = {}
        self.assertRaises(BackendError,
                          self.db._resubmit)
#                          server())

##         self.db._parent = j._impl
# self.assertRaises(BackendError,
# self.db._resubmit,
# server())

        # Come back to this

    def test_reset(self):
        j = Job(backend=self.db)._impl
        self.db._parent = j
        self.db.getJobObject().subjobs = [
            Job(backend=self.db)._impl, Job(backend=self.db)._impl]
        for j in self.db.getJobObject().subjobs:
            j.status = 'completing'

        disallowed_status = ['submitting', 'killed']
        for status in disallowed_status:
            self.db.getJobObject().status = status
            self.db.reset()
            self.assertEqual(
                self.db.getJobObject().status, status, 'status shouldn\'t have changed')

        self.db.getJobObject().status = 'completing'
        self.db.reset()
        self.assertEqual(
            self.db.getJobObject().status, 'submitted', 'didn\t reset job')
        self.assertNotEqual([j.status for j in self.db.getJobObject().subjobs], [
                            'submitted', 'submitted'], 'subjobs not reset properly')

        self.db.reset(doSubjobs=True)
        self.assertEqual([j.status for j in self.db.getJobObject().subjobs], [
                         'submitted', 'submitted'], 'subjobs not reset properly')

        for j in self.db.getJobObject().subjobs:
            j.status = 'completed'
        self.db.reset(doSubjobs=True)
        self.assertNotEqual([j.status for j in self.db.getJobObject().subjobs], [
                            'submitted', 'submitted'], 'subjobs not supposed to reset')

    def test_kill(self):
        #        class errorserver:
        #            def execute(this, dirac_cmd):
        #                return {}
        #        setattr(DiracBase,'dirac_ganga_server',errorserver())
        self.returnObject = {}
        self.db.id = 1234

        from Ganga.Core import BackendError
        self.assertRaises(BackendError,
                          self.db.kill)

#        class server:
#            def execute(this, dirac_cmd):
#                self.assertEqual(dirac_cmd,'kill(1234)','command not right')
#                return {'OK': True}
#        setattr(DiracBase,'dirac_ganga_server',server())
        self.toCheck = {'command': 'kill(1234)'}
        self.returnObject = {'OK': True}
        self.assertTrue(self.db.kill(), 'didn\'t run properly')

    def test_peek(self):
        #        class server:
        #            def execute(this, dirac_cmd):
        #                self.assertEqual(dirac_cmd,'peek(1234)')
        #                return {'OK':True,'Value':True}
        #        setattr(DiracBase,'dirac_ganga_server',server())

        self.db.id = 1234
        self.toCheck = {'command': 'peek(1234)'}
        self.returnObject = {'OK': True, 'Value': True}
        self.db.peek()

    def test_getOutputSandbox(self):
        j = Job(backend=self.db)
        self.db._parent = j._impl
        self.db.id = 1234

        tempDir = j._impl.getOutputWorkspace().getPath()
#        class server:
#            def execute(this, dirac_cmd):
#                self.assertEqual(dirac_cmd,
#                                 "getOutputSandbox(1234,'%s')"%dir,
#                                 'command not right')
#                return {'OK':True}
#        setattr(DiracBase,'dirac_ganga_server',server())
        self.toCheck = {'command': "getOutputSandbox(1234,'%s')" % tempDir}
        self.returnObject = {'OK': True}
        self.assertTrue(self.db.getOutputSandbox(), 'didn\'t run')

        testDir = 'test_dir'
        self.toCheck = {'command': "getOutputSandbox(1234,'%s')" % testDir}
        self.assertTrue(self.db.getOutputSandbox(testDir), 'didn\'t run with modified dir')

#        class errorserver:
#            def execute(this, dirac_cmd):
#                return {}
#        setattr(DiracBase,'dirac_ganga_server',errorserver())

        self.toCheck = {}
        self.returnObject = {}
        self.assertFalse(self.db.getOutputSandbox(testDir)), 'didn\'t fail gracefully'

    def test_removeOutputData(self):
        j = Job(backend=self.db)
        self.db._parent = j._impl
        #######################

        class testfile:

            def __init__(this): pass

            def remove(this):
                return 27

        #######################

        import GangaDirac.Lib.Backends.DiracBase

        def outputfiles_foreach(job, file_type, func):
            import types
            self.assertTrue(isinstance(job, Job._impl))
            if subjob:
                self.assertNotEqual(job.master, None)
            else:
                self.assertEqual(job.master, None)
            self.assertEqual(file_type, DiracFile._impl)
            self.assertEqual(type(func), types.FunctionType)
            self.assertEqual(
                func(testfile()), 27, 'Didn\'t call remove function')
        setattr(GangaDirac.Lib.Backends.DiracBase,
                'outputfiles_foreach', outputfiles_foreach)

        subjob = False
        self.assertEqual(self.db.removeOutputData(), None)

        j._impl.subjobs = [Job()._impl, Job()._impl, Job()._impl]
        for sj in j._impl.subjobs:
            sj.master = j._impl

        subjob = True
        self.assertEqual(self.db.removeOutputData(), None)

    def test_getOutputData(self):
        j = Job(backend=self.db)
        self.db._parent = j._impl

        self.assertRaises(GangaException, self.db.getOutputData, '/false/dir')

        #######################
        class testfile:

            def __init__(this, lfn, namePattern):
                this.lfn = lfn
                this.namePattern = namePattern

            def get(this):
                this.check = 42
        test_files = [testfile('a', 'alpha'), testfile('', 'delta'),
                      testfile('b', 'beta'), testfile('', 'bravo'),
                      testfile('c', 'charlie'), testfile('', 'foxtrot')]
        #######################

        import GangaDirac.Lib.Backends.DiracBase

        def outputfiles_iterator(job, file_type):
            import types
            self.assertTrue(isinstance(job, Job._impl))
            if subjob:
                self.assertNotEqual(job.master, None)
            else:
                self.assertEqual(job.master, None)
            self.assertEqual(file_type, DiracFile._impl)
            return test_files
        setattr(GangaDirac.Lib.Backends.DiracBase,
                'outputfiles_iterator', outputfiles_iterator)

        # master jobs
        #######################
        subjob = False
        self.assertEqual(self.db.getOutputData(), ['a', 'b', 'c'])
        for f in test_files:
            if f.lfn in ['a', 'b', 'c']:
                self.assertEqual(
                    f.localDir, j._impl.getOutputWorkspace().getPath())
                self.assertEqual(f.check, 42, "didn't call get")
                #delattr(f, 'localDir')
            else:
                self.assertFalse(hasattr(f, 'localDir'))
                self.assertFalse(hasattr(f, 'check'))
        self.assertEqual(
            self.db.getOutputData(None, ['alpha', 'charlie']), ['a', 'c'])
        self.assertEqual(self.db.getOutputData(
            os.path.expanduser('~/gangadir_testing'), ['alpha', 'charlie']), ['a', 'c'])

        # subjobs
        ########################
        j._impl.subjobs = [Job()._impl, Job()._impl, Job()._impl]
        i = 0
        for sj in j._impl.subjobs:
            sj.master = j._impl
            sj.id = i
            i += 1

        subjob = True
        self.assertEqual(self.db.getOutputData(), ['a', 'b', 'c'] * 3)
        self.assertEqual(self.db.getOutputData(None, ['beta']), ['b'] * 3)
        self.assertEqual(self.db.getOutputData(
            os.path.expanduser('~/gangadir_testing'), ['alpha', 'charlie']), ['a', 'c'] * 3)
        for i in range(3):
            self.assertTrue(os.path.isdir(
                os.path.join(os.path.expanduser('~/gangadir_testing'), '0.%d' % i)))
            os.rmdir(
                os.path.join(os.path.expanduser('~/gangadir_testing'), '0.%d' % i))

    def test_getOutputDataLFNs(self):
        j = Job(backend=self.db)
        self.db._parent = j._impl

        #######################
        class testfile:

            def __init__(this, lfn):
                this.lfn = lfn
        #######################

        import GangaDirac.Lib.Backends.DiracBase

        def outputfiles_iterator(job, file_type):
            import types
            self.assertTrue(isinstance(job, Job._impl))
            if subjob:
                self.assertNotEqual(job.master, None)
            else:
                self.assertEqual(job.master, None)
            self.assertEqual(file_type, DiracFile._impl)
            return [testfile('a'), testfile(''),
                    testfile('b'), testfile(''),
                    testfile('c'), testfile('')]
        setattr(GangaDirac.Lib.Backends.DiracBase,
                'outputfiles_iterator', outputfiles_iterator)

        subjob = False
        self.assertEqual(self.db.getOutputDataLFNs(), ['a', 'b', 'c'])

        j._impl.subjobs = [Job()._impl, Job()._impl, Job()._impl]
        for sj in j._impl.subjobs:
            sj.master = j._impl

        subjob = True
        self.assertEqual(self.db.getOutputDataLFNs(), ['a', 'b', 'c'] * 3)
