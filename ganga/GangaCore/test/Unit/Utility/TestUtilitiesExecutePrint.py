import os
import timeit

from GangaCore.Utility.execute import execute

# This file tests for things being returned via the stdout which is parsed by execute


def test_print():
    """Test that ``print`` works as expected"""
    assert execute('print("foo")', shell=False).strip() == 'foo'


def test_execute():
    ''' This tests that the script correctly changes the dir as requested via the script '''
    assert execute('cd "{0}"; pwd'.format(os.getcwd()), shell=True, cwd=os.getcwd()).strip() == os.getcwd()


def test_execute_timeouts():
    ''' This tests that the timeouts on scripts cause them to stop as expected '''
    # Test timeouts
    assert execute('while true; do sleep 1; done', shell=True, timeout=1) == 'Command timed out!'
    assert execute('while True: pass', timeout=1, shell=False) == 'Command timed out!'

    # Test timeout doesn't hinder a normal command
    assert execute('cd "{0}"; pwd'.format(os.getcwd()), shell=True, timeout=10, cwd=os.getcwd()).strip() == os.getcwd()

    assert timeit.timeit(
        '''
from GangaCore.Utility.execute import execute
execute('cd "{0}"; pwd', shell=True, timeout=10)
        '''.format(os.getcwd()), number=1) < 11


def test_execute_cwd():
    ''' This tests that teh cwd is set correctly as requeted in the arguments '''
    assert execute('pwd', shell=True, cwd='/').strip() == '/'


def test_execute_output():
    ''' This tests the various levels when an import can occur and that complex objects can be returned via stdout '''

    # Test printout of pickle dump interpreted correctly
    d = execute('import pickle, datetime\nprint(pickle.dumps(datetime.datetime(2013,12,12)))', shell=False)
    assert hasattr(d, 'month')
    assert d.month == 12

    # Test straight printout doesn't work without includes, stdout as str
    # returned by default
    d1 = execute('import datetime\nprint(datetime.datetime(2013,12,12))', shell=False)
    d2 = execute('import datetime\nrepr(datetime.datetime(2013,12,12))', shell=False)
    assert not hasattr(d1, 'month')
    assert isinstance(d1, str)
    assert not hasattr(d2, 'month')
    assert isinstance(d2, str)

    # Test printout works with the right includes
    d1 = execute('print(datetime.datetime(2013,12,12))', python_setup='import datetime', eval_includes='import datetime', shell=False)
    d2 = execute('print(repr(datetime.datetime(2013,12,12)))', python_setup='import datetime', eval_includes='import datetime', shell=False)
    assert not hasattr(d1, 'month')
    assert isinstance(d1, str)
    assert hasattr(d2, 'month')
    assert d2.month == 12
 
