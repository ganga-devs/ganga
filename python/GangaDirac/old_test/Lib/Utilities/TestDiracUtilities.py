from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.Lib.Utilities.DiracUtilities import execute
from Ganga.GPI import *
# GangaTest.Framework.utils defines some utility methods
# from GangaTest.Framework.utils import file_contains#,
# sleep_until_completed,sleep_until_state
import unittest
import os
from Ganga.Utility.Config import getConfig


class TestDiracUtilities(GangaGPITestCase):

    def test_execute(self):

        # Test shell vs python mode
        self.assertEqual(execute('import os\nprint os.getcwd()').strip(), os.getcwd())
        self.assertEqual(execute('pwd', shell=True).strip(), os.getcwd())

    def test_execute_timeouts(self):

        # Test timeouts
        self.assertEqual(execute('while true; do sleep 1; done', shell=True, timeout=10), 'Command timed out!')
        self.assertEqual(execute('while True: pass', timeout=10), 'Command timed out!')

        # Test timeout doesn't hinder a normal command
        self.assertEqual(execute('import os\nprint os.getcwd()', timeout=10).strip(), os.getcwd())
        self.assertEqual(execute('pwd', shell=True, timeout=10).strip(), os.getcwd())

        # Test timeout doesn't delay normal command
        import timeit
#        self.assertTrue(timeit.timeit('''from GangaDirac.Lib.Server.WorkerThreadPool import execute\nexecute('import os\\nprint os.getcwd()',timeout=10)''',
#                                      number=1) < 5)
#        self.assertTrue(timeit.timeit('''from GangaDirac.Lib.Server.WorkerThreadPool import execute\nexecute('pwd',shell=True, timeout=10)''',
#                                      number=1) < 5)
        self.assertTrue(timeit.timeit('''from GangaDirac.Lib.Utilities.DiracUtilities import execute\nexecute('import os\\nprint os.getcwd()',timeout=10)''',
                                      number=1) < 10)
        self.assertTrue(timeit.timeit('''from GangaDirac.Lib.Utilities.DiracUtilities import execute\nexecute('pwd',shell=True, timeout=10)''',
                                      number=1) < 10)

    def test_execute_cwd(self):

        # Test changing dir
        self.assertEqual(execute('import os\nprint os.getcwd()', cwd='/').strip(), '/')
        self.assertEqual(execute('pwd', shell=True, cwd='/').strip(), '/')

    def test_execute_env(self):

        # Test correctly picking up env
        env = {'ALEX': '/hello/world', 'PATH':
               os.environ['PATH'], 'PYTHONPATH': os.environ['PYTHONPATH']}
        # env.update(os.environ)
        self.assertEqual(execute('echo $ALEX', shell=True, env=env).strip(), '/hello/world')
        self.assertEqual(execute('import os\nprint os.environ.get("ALEX","BROKEN")',
                                 env=env, python_setup='#').strip(), '/hello/world')

        # Test env not updated by default
        execute('export NEWTEST=/new/test', shell=True, env=env)
        self.assertFalse('NEWTEST' in env)
        execute('import os\nos.environ["NEWTEST"]="/new/test"', env=env,  python_setup='#')
        self.assertFalse('NEWTEST' in env)

        # Test updating of env
        execute('export NEWTEST=/new/test', shell=True, env=env, update_env=True)
        self.assertTrue('NEWTEST' in env)
        del env['NEWTEST']
        self.assertFalse('NEWTEST' in env)
        execute('import os\nos.environ["NEWTEST"]="/new/test"', env=env,  python_setup='#', update_env=True)
        self.assertTrue('NEWTEST' in env)

    def test_execute_output(self):

        # Test pkl pipe output is interpreted properly
        d = execute('import datetime\noutput(datetime.datetime(2013,12,12))')
        self.assertTrue(hasattr(d, 'month'))
        self.assertEqual(d.month, 12)

        # Test printout of pickle dump interpreted correctly
        d = execute('import pickle, datetime\nprint pickle.dumps(datetime.datetime(2013,12,12))')
        self.assertTrue(hasattr(d, 'month'))
        self.assertEqual(d.month, 12)

        # Test straight printout doesn't work without includes, stdout as str
        # returned by default
        d1 = execute('import datetime\nprint datetime.datetime(2013,12,12)')
        d2 = execute('import datetime\nrepr(datetime.datetime(2013,12,12))')
        self.assertFalse(hasattr(d1, 'month'))
        self.assertTrue(type(d1) == str)
        self.assertFalse(hasattr(d2, 'month'))
        self.assertTrue(type(d2) == str)

        # Test printout works with the right includes
        d1 = execute('import datetime\nprint datetime.datetime(2013,12,12)', eval_includes='import datetime')
        d2 = execute('import datetime\nprint repr(datetime.datetime(2013,12,12))', eval_includes='import datetime')
        self.assertFalse(hasattr(d1, 'month'))
        self.assertTrue(type(d1) == str)
        self.assertTrue(hasattr(d2, 'month'))
        self.assertEqual(d2.month, 12)
