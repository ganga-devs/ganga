def test_submit_no_proxy(mocker):
    """
    Test that the lack of a proxy object causes the submit to fail
    """
    check_proxy = mocker.patch('Ganga.Lib.LCG.Grid.check_proxy', return_value=False)

    from Ganga.Lib.LCG import Grid
    job_url = Grid.submit('/some/path')

    assert check_proxy.call_count == 1

    assert job_url is None


def test_submit_expired_proxy(mocker):
    """
    Test that an invalid proxy object causes the submit to fail
    """
    check_proxy = mocker.patch('Ganga.Lib.LCG.Grid.check_proxy', return_value=True)
    credential = mocker.patch('Ganga.Lib.LCG.Grid.credential', return_value=mocker.MagicMock())
    credential.return_value.isValid.return_value = False

    from Ganga.Lib.LCG import Grid
    job_url = Grid.submit('/some/path')

    assert check_proxy.call_count == 1
    assert credential.call_count == 1

    assert job_url is None


def test_submit_bad_output(mocker):
    """
    Test that the external command returning bad data causes the job to fail
    """
    check_proxy = mocker.patch('Ganga.Lib.LCG.Grid.check_proxy', return_value=True)
    credential = mocker.patch('Ganga.Lib.LCG.Grid.credential', return_value=mocker.MagicMock())
    credential.return_value.isValid.return_value = True
    __set_submit_option__ = mocker.patch('Ganga.Lib.LCG.Grid.__set_submit_option__', return_value='  ')
    cmd1 = mocker.patch('Ganga.Utility.GridShell.Shell.cmd1', return_value=(0, 'some bad output', False))

    from Ganga.Lib.LCG import Grid
    job_url = Grid.submit('/some/path')

    assert check_proxy.call_count == 1
    assert credential.call_count == 1
    assert __set_submit_option__.call_count == 1
    assert cmd1.call_count == 1

    assert job_url is None


def test_submit(mocker):
    """
    Test that a job submit succeeds with valid input
    """
    check_proxy = mocker.patch('Ganga.Lib.LCG.Grid.check_proxy', return_value=True)
    credential = mocker.patch('Ganga.Lib.LCG.Grid.credential', return_value=mocker.MagicMock())
    credential.return_value.isValid.return_value = True
    __set_submit_option__ = mocker.patch('Ganga.Lib.LCG.Grid.__set_submit_option__', return_value='  ')
    cmd1 = mocker.patch('Ganga.Utility.GridShell.Shell.cmd1', return_value=(0, 'https://example.com:9000/some_url', False))

    from Ganga.Lib.LCG import Grid
    job_url = Grid.submit('/some/path')

    assert check_proxy.call_count == 1
    assert credential.call_count == 1
    assert __set_submit_option__.call_count == 1
    assert cmd1.call_count == 1

    assert '/some/path' in cmd1.call_args[0][0], 'JDL path was not passed correctly'
    assert job_url == 'https://example.com:9000/some_url'
