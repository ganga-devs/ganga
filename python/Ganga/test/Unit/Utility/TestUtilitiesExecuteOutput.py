import os
import timeit

from Ganga.Utility.execute import execute

# This file tests for outputs returned via the output stream within execute


def test_output():
    """Test that ``output`` works as expected"""
    assert execute('output("foo")', shell=False).strip() == 'foo'


def test_execute():
    ''' This tests that the cwd returned from the output is what is requested in the script '''
    # Test shell vs python mode
    assert execute('import os\noutput(os.getcwd())', cwd=os.getcwd(), shell=False).strip() == os.getcwd()


def test_execute_timeouts():
    ''' This tests the timeouts cause the scripts to fail as expected '''
    # Test timeouts
    assert execute('while true; do sleep 1; done', shell=True, timeout=1) == 'Command timed out!'
    assert execute('while True: pass', timeout=1, shell=False) == 'Command timed out!'

    # Test timeout doesn't hinder a normal command
    assert execute('import os\noutput(os.getcwd())', timeout=10, cwd=os.getcwd(), shell=False).strip() == os.getcwd()

    # Test timeout doesn't delay normal command
    assert timeit.timeit(
        """
import os
from Ganga.Utility.execute import execute
execute('import os\\noutput(os.getcwd())',timeout=10, cwd=os.getcwd(), shell=False)
        """, number=1) < 11


def test_execute_cwd():
    ''' This tests that the cwd of the script is changed as per the arguments to execute '''
    # Test changing dir
    assert execute('import os\noutput(os.getcwd())', cwd='/', shell=False).strip() == '/'


def test_execute_env():
    ''' This tests that the scripts can update an env based upon input and handle an appropriate python_setup sent in execute args '''

    # Test correctly picking up env
    env = {'ALEX': '/hello/world',
           'PATH': os.environ.get('PATH', ''),
           'PYTHONPATH': os.environ.get('PYTHONPATH', '')}
    # env.update(os.environ)
    assert execute('echo $ALEX', shell=True, env=env).strip() == '/hello/world'
#    assert execute('import os\noutput(os.environ.get("ALEX","BROKEN"))', env=env, python_setup='#', shell=False).strip() == '/hello/world'

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
    execute('import os\nos.environ["NEWTEST"]="/new/test"', env=env, python_setup='#', update_env=True, shell=False)
    assert 'NEWTEST' in env


def test_execute_output():
    ''' This tests the abilty to send complex objects back through the output stream to Ganga '''

    # Test pkl pipe output is interpreted properly
    d = execute('import datetime\noutput(datetime.datetime(2013,12,12))', shell=False)
    assert hasattr(d, 'month')
    assert d.month == 12

