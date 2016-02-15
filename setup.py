from setuptools import setup
from setuptools import find_packages
from setuptools import Command
import subprocess
import os
import sys

file_path = os.path.dirname(os.path.realpath(__file__))
ganga_python_dir = os.path.join(file_path, 'python')
sys.path.insert(0, ganga_python_dir)
from Ganga import _gangaVersion

def readme():
    import os.path
    filename = 'README.rst'
    if not os.path.exists(filename):
        filename = os.path.abspath(os.path.join(file_path, filename))
    with open(filename) as f:
        return f.read()


class RunTestsCommand(Command):
    """
    A custom setuptools command to run the Ganga test suite.
    """
    description = 'run the Ganga test suite'
    all_types = ['unit', 'integration', 'all']
    user_options = [
        ('type=', 't', 'the type of tests: [{0}]'.format(', '.join(all_types))),
        ('coverage', None, 'should coverage be generated'),
        ('xunit', None, 'should xunit-compatible files be produced'),
    ]

    def initialize_options(self):
        self.type = 'unit'
        self.coverage = False
        self.xunit = False

    def finalize_options(self):
        if not self.type in self.all_types:
            raise Exception('Test type must be [{0}]'.format(', '.join(self.all_types)))

    @staticmethod
    def _getTestEnv():

        test_env = os.environ.copy()
        ## Make sure that the PYTHONPATH is correct for the tests to pick up 'import Ganga'
        test_sys_path = ''
        for _dir in sys.path:
            test_sys_path = test_sys_path + _dir + ':'
        test_env['PYTHONPATH'] = test_sys_path

        return test_env

    def run(self):

        cmd = ['nosetests']

        if self.type in ['unit', 'all']:
            cmd.append('Ganga/new_tests/*.py')
        if self.type in ['integration', 'all']:
            cmd.append('Ganga/new_tests/GPI --testmatch="(?:\\b|_)([tT]est)""')

        if self.coverage:
            cmd.append('--with-coverage --cover-erase --cover-xml --cover-package=.')
        if self.xunit:
            cmd.append('--with-xunit')

        subprocess.check_call(' '.join(cmd), cwd=ganga_python_dir, shell=True, env=self._getTestEnv())


setup(name='ganga',
      description='Job management tool',
      long_description=readme(),
      url='https://github.com/ganga-devs/ganga',
      version=_gangaVersion,
      author='Ganga Developers',
      author_email='project-ganga-developers@cern.ch',
      license='GPL v2',
      scripts=['bin/ganga'],
      package_dir={'': 'python'},
      packages=find_packages('python'),
      install_requires=[
          'ipython==1.2.1',
          'httplib2>=0.8',
          'python-gflags>=2.0',
          'google-api-python-client>=1.1',
          'stomp.py>=3.1.7',
      ],
      classifiers=[
          'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
          'Programming Language :: Python :: 2.6',
          'Programming Language :: Python :: 2.7',
      ],
      package_data={'Ganga': ['Runtime/HEAD_CONFIG.INI']},
      cmdclass={
          'tests': RunTestsCommand,
      },
      )
