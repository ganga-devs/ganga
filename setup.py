from setuptools import setup
from setuptools import find_packages
from setuptools import Command
import subprocess
import os
import sys

file_path = os.path.dirname(os.path.realpath(__file__))


def version():
    ganga_python_dir = os.path.join(file_path, 'python')
    sys.path.insert(0, ganga_python_dir)
    from Ganga import _gangaVersion
    return _gangaVersion


def readme():
    filename = os.path.abspath(os.path.join(file_path, 'README.rst'))
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
        if self.type not in self.all_types:
            raise Exception('Test type must be [{0}]'.format(', '.join(self.all_types)))

    @staticmethod
    def _get_test_env():
        ganga_python_dir = os.path.join(file_path, 'python')

        test_env = os.environ.copy()
        path = ':'.join(s for s in [ganga_python_dir, test_env.get('PYTHONPATH', None)] if s)
        test_env['PYTHONPATH'] = path

        return test_env

    def run(self):

        cmd = ['py.test']

        if self.type in ['unit', 'all']:
            cmd.append('python/Ganga/test/Unit')
            cmd.append('python/Ganga/Core')
            cmd.append('python/Ganga/Runtime')
            cmd.append('python/Ganga/Utility')
        if self.type in ['integration', 'all']:
            cmd.append('python/Ganga/test/GPI')

        if self.coverage:
            cmd.append('--cov-report xml --cov .')
        if self.xunit:
            cmd.append('--junitxml tests.xml')

        subprocess.check_call(' '.join(cmd), cwd=file_path, shell=True, env=self._get_test_env())


setup(name='ganga',
      description='Job management tool',
      long_description=readme(),
      url='https://github.com/ganga-devs/ganga',
      version=version(),
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
