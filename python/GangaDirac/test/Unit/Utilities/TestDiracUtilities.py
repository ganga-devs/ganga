import os
import timeit

import pytest
import mock

from GangaDirac.Lib.Utilities.DiracUtilities import execute, getDiracEnv

from Ganga.testlib.GangaUnitTest import load_config_files, clear_config

from Ganga.testlib.mark import external

from GangaDirac.Lib.Utilities import DiracUtilities
DiracUtilities.last_modified_valid = True
DiracUtilities._checkProxy = mock.Mock()

@pytest.yield_fixture(scope='module', autouse=True)
def config_files():
    """
    Load the config files in a way similar to a full Ganga session
    NB: This is taken to be only within the context of __this__ file!
    """
    load_config_files()
    yield
    clear_config()


def test_dirac_env():
    env = getDiracEnv()
    assert any(key.startswith('DIRAC') for key in env)


def test_output():
    """Test that ``output`` works as expected"""
    assert execute('output("foo")').strip() == 'foo'


def test_execute():
    # Test shell vs python mode
    assert execute('import os\noutput(os.getcwd())', cwd=os.getcwd()).strip() == os.getcwd()


def test_execute_timeouts():
    # Test timeouts
    assert execute('while true; do sleep 1; done', shell=True, timeout=1) == 'Command timed out!'
    assert execute('while True: pass', timeout=1) == 'Command timed out!'

    # Test timeout doesn't hinder a normal command
    assert execute('import os\noutput(os.getcwd())', timeout=10, cwd=os.getcwd()).strip() == os.getcwd()

    # Test timeout doesn't delay normal command
    assert timeit.timeit(
        '''
        import os
        from GangaDirac.Lib.Utilities.DiracUtilities import execute
        execute('import os\\noutput(os.getcwd())',timeout=10, cwd=os.getcwd())
        ''', number=1) < 11


def test_execute_cwd():
    # Test changing dir
    assert execute('import os\noutput(os.getcwd())', cwd='/').strip() == '/'


def test_execute_env():

    # Test correctly picking up env
    env = {'ALEX': '/hello/world',
           'PATH': os.environ.get('PATH', ''),
           'PYTHONPATH': os.environ.get('PYTHONPATH', '')}
    # env.update(os.environ)
    assert execute('echo $ALEX', shell=True, env=env).strip() == '/hello/world'
    assert execute('import os\noutput(os.environ.get("ALEX","BROKEN"))', env=env, python_setup='#').strip() == '/hello/world'

    # Test env not updated by default
    execute('export NEWTEST=/new/test', shell=True, env=env)
    assert 'NEWTEST' not in env
    execute('import os\nos.environ["NEWTEST"]="/new/test"', env=env, python_setup='#')
    assert 'NEWTEST' not in env

    # Test updating of env
    execute('export NEWTEST=/new/test', shell=True, env=env, update_env=True)
    assert 'NEWTEST' in env
    del env['NEWTEST']
    assert 'NEWTEST' not in env
    execute('import os\nos.environ["NEWTEST"]="/new/test"', env=env, python_setup='#', update_env=True)
    assert 'NEWTEST' in env


def test_execute_output():

    # Test pkl pipe output is interpreted properly
    d = execute('import datetime\noutput(datetime.datetime(2013,12,12))')
    assert hasattr(d, 'month')
    assert d.month == 12

