import tempfile
import os
import pytest
from functools import wraps
try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock

from GangaCore.GPIDev.Lib.Job import Job
from GangaCore.GPIDev.Lib.File import File
from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GangaCore.Lib.Splitters import ArgSplitter
from GangaCore.Lib.Executable import Executable
from GangaDirac.Lib.Utilities.DiracUtilities import GangaDiracError
from GangaCore.testlib.GangaUnitTest import load_config_files, clear_config
from GangaCore.Core.exceptions import GangaDiskSpaceError

script_template = """
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
result = dirac.submitJob(j)

output(result)
"""


@pytest.yield_fixture(scope='function')
def db():
    """Provides a DiracBase object per test function"""
    load_config_files()
    from GangaDirac.Lib.Backends.DiracBase import DiracBase
    yield DiracBase()
    clear_config()

@pytest.yield_fixture(scope='function')
def db_full():
    def mock_disk_space_full_decorator(f):
        @wraps(f)
        def decorated_function(self, *args, **kwargs):
            raise GangaDiskSpaceError("Mock no disk space!")
        
            return f(self, *args, **kwargs)
        return decorated_function
    import GangaCore.Utility.util
    GangaCore.Utility.util.require_disk_space = mock_disk_space_full_decorator
    load_config_files()
    from GangaDirac.Lib.Backends import DiracBase
    import importlib
    importlib.reload(DiracBase)
    yield DiracBase.DiracBase()
    clear_config()


def test__setup_subjob_dataset(db):
    assert db._setup_subjob_dataset([]) is None


def test__addition_sandbox_content(db):
    assert db._addition_sandbox_content(None) == [], 'Not empty list'


def test__setup_bulk_subjobs(tmpdir, db):
    from GangaCore.Core.exceptions import BackendError
    from GangaCore.GPIDev.Lib.Dataset.Dataset import Dataset
    from GangaDirac.Lib.Backends.Dirac import Dirac

    name = str(tmpdir.join('submit_script'))
    with open(name, 'w') as fd:
        fd.write(script_template.replace('###PARAMETRIC_INPUTDATA###', str([['a'], ['b']])))

    with pytest.raises(BackendError):
        db._setup_bulk_subjobs([], name)

    d = Dirac()
    j = Job()
    j.id = 0  # This would normally be set by the registry if this was a proxy job
    j.application = Executable()
    j.splitter = ArgSplitter()
    j.splitter.args = [['a'], ['b'], ['c'], ['d'], ['e']]
    j.inputdata = Dataset()
    j.backend = d
    d._parent = j

    dirac_ids = [123, 456]

    def fake_setup_subjob_dataset(dataset):
        assert dataset in [['a'], ['b']], 'dataset not passed properly'

    with patch.object(d, '_setup_subjob_dataset', fake_setup_subjob_dataset):
        assert d._setup_bulk_subjobs(dirac_ids, name), 'didnt run'

    assert len(j.subjobs) == len(dirac_ids), 'didnt work'
    for id_, backend_id, subjob in zip(list(range(len(dirac_ids))), dirac_ids, j.subjobs):
        assert id_ == subjob.id, 'ids dont match'
        assert backend_id == subjob.backend.id, 'backend.ids dont match'
        assert isinstance(subjob.application, j.application.__class__), 'apps dont match'
        assert subjob.splitter is None, 'splitter not done'
        assert isinstance(subjob.backend, j.backend.__class__), 'backend dont match'


def test__common_submit(tmpdir, db, mocker):
    mocker.patch('GangaCore.GPIDev.Credentials.credential_store')

    from GangaCore.Core.exceptions import BackendError

    j = Job()
    j.id = 0
    j.backend = db
    db._parent = j

    name = str(tmpdir.join('submit_script'))
    with open(name, 'w') as fd:
        fd.write(script_template.replace('###PARAMETRIC_INPUTDATA###', str([['a'], ['b']])))

    with patch('GangaDirac.Lib.Backends.DiracBase.execute', side_effect=GangaDiracError('test Exception')):
        db.id = 1234
        db.actualCE = 'test'
        db.status = 'test'
        with pytest.raises(BackendError):
            db._common_submit(name)

        assert db.id is None, 'id not None'
        assert db.actualCE is None, 'actualCE not None'
        assert db.status is None, 'status not None'

    with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=12345) as execute:
        assert db._common_submit(name)

        execute.assert_called_once_with("execfile('%s')" % name, cred_req=mocker.ANY)

        assert db.id == 12345, 'id not set'

    with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=[123, 456]):
        with patch.object(db, '_setup_bulk_subjobs') as _setup_bulk_subjobs:
            db._common_submit(name)
            _setup_bulk_subjobs.assert_called_once_with([123, 456], name)


def test_submit(db):
    j = Job()
    j.id = 0
    j.backend = db
    db._parent = j

    file1 = tempfile.NamedTemporaryFile('w')
    file2 = tempfile.NamedTemporaryFile('w')
    file3 = tempfile.NamedTemporaryFile('w')
    sjc = StandardJobConfig(exe=script_template,
                            inputbox=[File(file1.name),
                                      File(file2.name),
                                      File(file3.name)],
                            outputbox=['d', 'e', 'f'])

    def fake_common_submit(dirac_script):
        with open(dirac_script, 'r') as f:
            script = f.read()
            assert script != script_template, 'script not changed'
            assert script_template.replace('##INPUT_SANDBOX##',
                                           str(['a', 'b', 'c'] +
                                               [os.path.join(j.getInputWorkspace().getPath(),
                                                             '_input_sandbox_0.tgz')] +
                                               ['g'])) == script, 'script not what it should be'

        return True

    with patch.object(db, '_addition_sandbox_content', return_value=['g']) as _addition_sandbox_content:
        with patch.object(db, '_common_submit', Mock(fake_common_submit)) as _common_submit:
            assert db.submit(sjc, ['a', 'b', 'c'])

            _addition_sandbox_content.assert_called_once_with(sjc)
            _common_submit.assert_called_once()

    file1.close()
    file2.close()
    file3.close()


def test_resubmit(db):
    with patch.object(db, '_blockResubmit', return_value='_resubmit run ok'):
        assert db.resubmit() == '_resubmit run ok'


def test__resubmit(db):
    from GangaCore.Core.exceptions import BackendError

    def _common_submit(dirac_script):
        return '_common_submit run ok'

    masterj = Job()
    masterj.id = 0
    masterj.backend = db
    j = Job()
    j.id = 1
    j.backend = db
    db._setParent(masterj)

    with patch.object(db, '_common_submit', return_value='_common_submit run ok'):
        with pytest.raises(BackendError):
            db._resubmit()


def test_reset(db):
    j = Job()
    j.id = 0
    j.backend = db
    db._parent = j
    db.getJobObject().subjobs = [Job(), Job()]
    for subjob in db.getJobObject().subjobs:
        subjob.backend = db
    for j in db.getJobObject().subjobs:
        j.status = 'completing'

    disallowed_status = ['submitting', 'killed']
    for status in disallowed_status:
        db.getJobObject().status = status
        db.reset()
        assert db.getJobObject().status == status, 'status shouldn\'t have changed'

    db.getJobObject().status = 'completing'
    db.reset()
    assert db.getJobObject().status == 'submitted', 'didn\t reset job'
    assert [j.status for j in db.getJobObject().subjobs] != ['submitted', 'submitted'], 'subjobs not reset properly'

    db.reset(doSubjobs=True)
    assert [j.status for j in db.getJobObject().subjobs] == ['submitted', 'submitted'], 'subjobs not reset properly'

    for j in db.getJobObject().subjobs:
        j.status = 'completed'
    db.reset(doSubjobs=True)
    assert [j.status for j in db.getJobObject().subjobs] != ['submitted', 'submitted'], 'subjobs not supposed to reset'


def test_kill(db, mocker):
    mocker.patch('GangaCore.GPIDev.Credentials.credential_store')

    db.id = 1234

    with patch('GangaDirac.Lib.Backends.DiracBase.execute', side_effect=GangaDiracError('test Exception')):
        from GangaCore.Core.exceptions import BackendError
        with pytest.raises(BackendError):
            db.kill()

    with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=True) as execute:
        assert db.kill()
        execute.assert_called_once_with('kill(1234)', cred_req=mocker.ANY)


def test_peek(db, mocker):
    mocker.patch('GangaCore.GPIDev.Credentials.credential_store')

    db.id = 1234
    with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=True) as execute:
        db.peek()
        execute.assert_called_once_with('peek(1234)', cred_req=mocker.ANY)


def test_getOutputSandbox(db, mocker):
    mocker.patch('GangaCore.GPIDev.Credentials.credential_store')

    j = Job()
    j.id = 0
    j.backend = db
    db._parent = j
    db.id = 1234

    temp_dir = j.getOutputWorkspace().getPath()
    with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=True) as execute:
        assert db.getOutputSandbox(), 'didn\'t run'
        execute.assert_called_once_with("getOutputSandbox(1234,'%s', True)" % temp_dir, cred_req=mocker.ANY)

    test_dir = 'test_dir'
    with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=True) as execute:
        assert db.getOutputSandbox(test_dir), 'didn\'t run with modified dir'
        execute.assert_called_once_with("getOutputSandbox(1234,'%s', True)" % test_dir, cred_req=mocker.ANY)

    with patch('GangaDirac.Lib.Backends.DiracBase.execute', side_effect=GangaDiracError('test Exception')) as execute:
        assert not db.getOutputSandbox(test_dir), 'didn\'t fail gracefully'
        execute.assert_called_once()

def test_removeOutputData(db):
    from GangaDirac.Lib.Files.DiracFile import DiracFile

    j = Job()
    j.id = 0
    j.backend = db
    db._parent = j

    #######################

    class TestFile(object):
        def __init__(self):
            self.lfn = 27

        def remove(self):
            return 27

    #######################

    def fake_outputfiles_foreach(job, file_type, func):
        import types
        assert isinstance(job, Job)
        if subjob:
            assert job.master is not None
        else:
            assert job.master is None
            assert file_type == DiracFile
            assert isinstance(func, types.FunctionType)

    with patch('GangaDirac.Lib.Backends.DiracBase.outputfiles_foreach', fake_outputfiles_foreach):
        with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=True):
            subjob = False
            assert db.removeOutputData() is None

            j.subjobs = [Job(), Job(), Job()]
            for sj in j.subjobs:
                sj._setParent(j)

            subjob = True
            assert db.removeOutputData() is None


def test_getOutputData(db, tmpdir):
    from GangaDirac.Lib.Files.DiracFile import DiracFile

    j = Job()
    j.id = 0
    j.backend = db
    db._parent = j

    with pytest.raises(GangaDiracError):
        db.getOutputData('/false/dir')

    #######################
    class TestFile(object):
        def __init__(self, lfn, namePattern):
            self.lfn = lfn
            self.namePattern = namePattern

        def get(self):
            self.check = 42

    test_files = [TestFile('a', 'alpha'), TestFile('', 'delta'),
                  TestFile('b', 'beta'), TestFile('', 'bravo'),
                  TestFile('c', 'charlie'), TestFile('', 'foxtrot')]

    #######################

    def fake_outputfiles_iterator(job, file_type):
        assert isinstance(job, Job)
        if subjob:
            assert job.master is not None
        else:
            assert job.master is None
            assert file_type == DiracFile
        return test_files

    with patch('GangaDirac.Lib.Backends.DiracBase.outputfiles_iterator', fake_outputfiles_iterator):

        # master jobs
        #######################
        subjob = False
        assert db.getOutputData() == ['a', 'b', 'c']
        for f in test_files:
            if f.lfn in ['a', 'b', 'c']:
                assert f.localDir == j.getOutputWorkspace().getPath()
                assert f.check, 42 == "didn't call get"
            else:
                assert not hasattr(f, 'localDir')
                assert not hasattr(f, 'check')
        assert db.getOutputData(None, ['alpha', 'charlie']) == ['a', 'c']
        assert db.getOutputData(tmpdir.dirname, ['alpha', 'charlie']) == ['a', 'c']

        # subjobs
        ########################
        j.subjobs = [Job(), Job(), Job()]
        i = 0
        for sj in j.subjobs:
            sj._setParent(j)
            sj.id = i
            i += 1

        subjob = True
        assert db.getOutputData() == ['a', 'b', 'c'] * 3
        assert db.getOutputData(None, ['beta']) == ['b'] * 3
        assert db.getOutputData(tmpdir.dirname, ['alpha', 'charlie']) == ['a', 'c'] * 3
        for i in range(3):
            assert os.path.isdir(os.path.join(tmpdir.dirname, '0.%d' % i))
            os.rmdir(os.path.join(tmpdir.dirname, '0.%d' % i))


def test_getOutputDataLFNs(db):
    from GangaDirac.Lib.Files.DiracFile import DiracFile

    j = Job()
    j.id = 0
    j.backend = db
    db._parent = j

    #######################
    class TestFile(object):
        def __init__(self, lfn):
            self.lfn = lfn
    #######################

    def fake_outputfiles_iterator(job, file_type):
        assert isinstance(job, Job)
        if subjob:
            assert job.master is not None
        else:
            assert job.master is None
            assert file_type == DiracFile
        return [TestFile('a'), TestFile(''),
                TestFile('b'), TestFile(''),
                TestFile('c'), TestFile('')]

    with patch('GangaDirac.Lib.Backends.DiracBase.outputfiles_iterator', fake_outputfiles_iterator):
        subjob = False
        assert db.getOutputDataLFNs() == ['a', 'b', 'c']

        j.subjobs = [Job(), Job(), Job()]
        for sj in j.subjobs:
            sj._setParent(j)

        subjob = True
        assert db.getOutputDataLFNs() == ['a', 'b', 'c'] * 3

def test_getOutputSandbox_diskFull(db_full, mocker):
    mocker.patch('GangaCore.GPIDev.Credentials.credential_store')

    j = Job()
    j.id = 0
    j.backend = db_full
    db_full._parent = j
    db_full.id = 1234

    temp_dir = j.getOutputWorkspace().getPath()
    with patch('GangaDirac.Lib.Backends.DiracBase.execute', return_value=True) as execute:
        with pytest.raises(GangaDiskSpaceError) as execinfo:
            assert not  db_full.getOutputSandbox(), 'didn\'t run'
            assert str(execinfo.value)=='Mock no disk space!'

def test_getOutputData_diskFull(db_full, tmpdir):
    from GangaDirac.Lib.Files.DiracFile import DiracFile

    j = Job()
    j.id = 0
    j.backend = db_full
    db_full._parent = j

    with pytest.raises(GangaDiskSpaceError) as execinfo:
        assert not db_full.getOutputData('/false/dir')
        assert str(execinfo.value)=='Mock no disk space!'

    #######################
    class TestFile(object):
        def __init__(self, lfn, namePattern):
            self.lfn = lfn
            self.namePattern = namePattern

        def get(self):
            self.check = 42

    test_files = [TestFile('a', 'alpha'), TestFile('', 'delta'),
                  TestFile('b', 'beta'), TestFile('', 'bravo'),
                  TestFile('c', 'charlie'), TestFile('', 'foxtrot')]

    #######################

    def fake_outputfiles_iterator(job, file_type):
        assert isinstance(job, Job)
        if subjob:
            assert job.master is not None
        else:
            assert job.master is None
            assert file_type == DiracFile
        return test_files

    with patch('GangaDirac.Lib.Backends.DiracBase.outputfiles_iterator', fake_outputfiles_iterator):

        # master jobs
        #######################
        subjob = False
        with pytest.raises(GangaDiskSpaceError) as execinfo:
            assert not db_full.getOutputData() == ['a', 'b', 'c']
            assert str(execinfo.value)=='Mock no disk space!'

