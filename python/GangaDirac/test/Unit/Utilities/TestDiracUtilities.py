import os
import timeit

import pytest

from GangaDirac.Lib.Utilities.DiracUtilities import execute

from Ganga.testlib.GangaUnitTest import load_config_files, clear_config


@pytest.yield_fixture(scope='module', autouse=True)
def config_files():
    """
    Load the config files in a way similar to a full Ganga session
    """
    load_config_files()
    yield
    clear_config()


def test_execute():

    # Test shell vs python mode
    assert execute('import os\nprint(os.getcwd())').strip() == os.getcwd()
    assert execute('cd {0}; pwd'.format(os.getcwd()), shell=True).strip() == os.getcwd()


def test_execute_timeouts():

    # Test timeouts
    assert execute('while true; do sleep 1; done', shell=True, timeout=1) == 'Command timed out!'
    assert execute('while True: pass', timeout=1) == 'Command timed out!'

    # Test timeout doesn't hinder a normal command
    assert execute('import os\nprint(os.getcwd())', timeout=10).strip() == os.getcwd()
    assert execute('cd {0}; pwd'.format(os.getcwd()), shell=True, timeout=10).strip() == os.getcwd()

    # Test timeout doesn't delay normal command
    assert timeit.timeit('''from GangaDirac.Lib.Utilities.DiracUtilities import execute\nexecute('import os\\nprint(os.getcwd())',timeout=10)''', number=1) < 10
    assert timeit.timeit('''from GangaDirac.Lib.Utilities.DiracUtilities import execute\nexecute('cd {0}; pwd',shell=True, timeout=10)'''.format(os.getcwd()), number=1) < 10


def test_execute_cwd():

    # Test changing dir
    assert execute('import os\nprint(os.getcwd())', cwd='/').strip() == '/'
    assert execute('pwd', shell=True, cwd='/').strip() == '/'


def test_execute_env():

    # Test correctly picking up env
    env = {'ALEX': '/hello/world',
           'PATH': os.environ.get('PATH', ''),
           'PYTHONPATH': os.environ.get('PYTHONPATH', '')}
    # env.update(os.environ)
    assert execute('echo $ALEX', shell=True, env=env).strip() == '/hello/world'
    assert execute('import os\nprint(os.environ.get("ALEX","BROKEN"))', env=env, python_setup='#').strip() == '/hello/world'

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

    # Test printout of pickle dump interpreted correctly
    d = execute('import pickle, datetime\nprint(pickle.dumps(datetime.datetime(2013,12,12)))')
    assert hasattr(d, 'month')
    assert d.month == 12

    # Test straight printout doesn't work without includes, stdout as str
    # returned by default
    d1 = execute('import datetime\nprint(datetime.datetime(2013,12,12))')
    d2 = execute('import datetime\nrepr(datetime.datetime(2013,12,12))')
    assert not hasattr(d1, 'month')
    assert isinstance(d1, str)
    assert not hasattr(d2, 'month')
    assert isinstance(d2, str)

    # Test printout works with the right includes
    d1 = execute('print(datetime.datetime(2013,12,12))', eval_includes='import datetime')
    d2 = execute('print(repr(datetime.datetime(2013,12,12)))', eval_includes='import datetime')
    assert not hasattr(d1, 'month')
    assert isinstance(d1, str)
    assert hasattr(d2, 'month')
    assert d2.month == 12
