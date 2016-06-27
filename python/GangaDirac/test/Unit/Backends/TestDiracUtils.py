import pytest

from Ganga.Core.exceptions import BackendError
from GangaDirac.Lib.Backends.DiracUtils import result_ok, get_job_ident, get_parametric_datasets, outputfiles_iterator


def test_result_ok():

    assert not result_ok(None), "Didn't return False with None arg"
    assert not result_ok(''), "Didn't return False with non-dict arg"
    assert not result_ok({}), "Didn't return False as default dict extraction"
    assert not result_ok({'OK': False}), "OK not handled properly"
    assert result_ok({'OK': True}), "Didn't return True"


def test_get_job_ident():

    error_script = """
from DIRAC import Job
"""
    script = """
from DIRAC import Job
j=Job()
j.outputsomething('output.root')
"""

    with pytest.raises(BackendError):
        get_job_ident(error_script.splitlines())
    assert 'j' == get_job_ident(script.splitlines()), "Didn't get the right job ident"


def test_get_parametric_dataset():
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

    with pytest.raises(BackendError):
        get_parametric_datasets(error_script1.splitlines())
    with pytest.raises(BackendError):
        get_parametric_datasets(error_script2.splitlines())
    assert get_parametric_datasets(script.splitlines()) == [['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h', 'i']], 'parametric dataset not correctly extracted'
    assert isinstance(get_parametric_datasets(script.splitlines()), list)


def test_outputfiles_iterator():

    ########################################################
    class TestFile(object):
        def __init__(self, name, subfiles=[]):
            self.name = name
            self.subfiles = subfiles

    class TestFileA(TestFile):
        def __init__(self, name, subfiles=[]):
            super(TestFileA, self).__init__(name, subfiles)

    class TestFileB(TestFile):
        def __init__(self, name, subfiles=[]):
            super(TestFileB, self).__init__(name, subfiles)

    class TestJob(object):
        def __init__(self, outputfiles=[], nc_outputfiles=[]):
            self.outputfiles = outputfiles
            self.non_copyable_outputfiles = nc_outputfiles

    def pred_a(f):
        return f.name == 'A2'

    def pred_b(f):
        return f.name == 'BS2'
    ########################################################

    test_job = TestJob(outputfiles=[TestFileA('A1', subfiles=[TestFileA('AS1')]), TestFileA('A2'),
                                    TestFileB('B1', subfiles=[TestFileB('BS1')]), TestFileA('A3')],
                       nc_outputfiles=[TestFileB('B2'), TestFileA('A4'),
                                       TestFileB('B3', subfiles=[TestFileB('BS2'), TestFileB('BS3')]), TestFileB('B4')])

    assert [f.name for f in outputfiles_iterator(test_job, TestFile)] == ['AS1', 'A2', 'BS1', 'A3', 'B2', 'A4', 'BS2', 'BS3', 'B4']
    assert [f.name for f in outputfiles_iterator(test_job, TestFileA)] == ['AS1', 'A2', 'A3', 'A4']
    assert [f.name for f in outputfiles_iterator(test_job, TestFileB)] == ['BS1', 'B2', 'BS2', 'BS3', 'B4']

    assert [f.name for f in outputfiles_iterator(test_job, TestFile, include_subfiles=False)] == ['A1', 'A2', 'B1', 'A3', 'B2', 'A4', 'B3', 'B4']
    assert [f.name for f in outputfiles_iterator(test_job, TestFileA, include_subfiles=False)] == ['A1', 'A2', 'A3', 'A4']
    assert [f.name for f in outputfiles_iterator(test_job, TestFileB, include_subfiles=False)] == ['B1', 'B2', 'B3', 'B4']

    assert [f.name for f in outputfiles_iterator(test_job, TestFile, selection_pred=pred_a)] == ['A2']
    assert [f.name for f in outputfiles_iterator(test_job, TestFile, selection_pred=pred_b)] == ['BS2']
    assert [f.name for f in outputfiles_iterator(test_job, TestFile, selection_pred=pred_b, include_subfiles=False)] == []
