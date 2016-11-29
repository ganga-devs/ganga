from __future__ import absolute_import

from Ganga.Utility.GridShell import Shell

from ..Credentials.TestCredentialStore import FakeCred


def test_submit_bad_output(mocker):
    """
    Test that the external command returning bad data causes the job to fail
    """
    __set_submit_option__ = mocker.patch('Ganga.Lib.LCG.Grid.__set_submit_option__', return_value='  ')
    mocker.patch('Ganga.Lib.LCG.Grid.getShell', return_value=Shell)
    cmd1 = mocker.patch('Ganga.Utility.GridShell.Shell.cmd1', return_value=(0, 'some bad output', False))

    from Ganga.Lib.LCG import Grid
    job_url = Grid.submit('/some/path', cred_req=FakeCred())

    assert __set_submit_option__.call_count == 1
    assert cmd1.call_count == 1

    assert job_url is None


def test_submit(mocker):
    """
    Test that a job submit succeeds with valid input
    """
    __set_submit_option__ = mocker.patch('Ganga.Lib.LCG.Grid.__set_submit_option__', return_value='  ')
    mocker.patch('Ganga.Lib.LCG.Grid.getShell', return_value=Shell)
    cmd1 = mocker.patch('Ganga.Utility.GridShell.Shell.cmd1', return_value=(0, 'https://example.com:9000/some_url', False))

    from Ganga.Lib.LCG import Grid
    job_url = Grid.submit('/some/path', cred_req=FakeCred())

    assert __set_submit_option__.call_count == 1
    assert cmd1.call_count == 1

    assert '/some/path' in cmd1.call_args[0][0], 'JDL path was not passed correctly'
    assert job_url == 'https://example.com:9000/some_url'
