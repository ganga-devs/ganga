import os
import timeit

import pytest
import mock

from GangaDirac.Lib.Utilities.DiracUtilities import execute, getDiracEnv

from Ganga.testlib.GangaUnitTest import load_config_files, clear_config

from Ganga.testlib.mark import external

@pytest.yield_fixture(scope='module', autouse=True)
def config_files():
    """
    Load the config files in a way similar to a full Ganga session
    NB: This is taken to be only within the context of __this__ file!
    """
    load_config_files()
    yield
    clear_config()

@external
def test_print():
    """Test that ``print`` works as expected"""
    assert execute('print("foo")').strip() == 'foo'


@external
def test_execute():
    assert execute('cd "{0}"; pwd'.format(os.getcwd()), shell=True, cwd=os.getcwd()).strip() == os.getcwd()


@external
def test_execute_timeouts():
    # Test timeouts
    assert execute('while true; do sleep 1; done', shell=True, timeout=1) == 'Command timed out!'
    assert execute('while True: pass', timeout=1) == 'Command timed out!'

    # Test timeout doesn't hinder a normal command
    assert execute('cd "{0}"; pwd'.format(os.getcwd()), shell=True, timeout=10, cwd=os.getcwd()).strip() == os.getcwd()

    assert timeit.timeit(
        '''
        from GangaDirac.Lib.Utilities.DiracUtilities import execute
        execute('cd "{0}"; pwd',shell=True, timeout=10)
        '''.format(os.getcwd()), number=1) < 11

@external
def test_execute_cwd():
    assert execute('pwd', shell=True, cwd='/').strip() == '/'


@external
def test_execute_output():

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
 
