from setuptools import setup
from setuptools import find_packages
from setuptools import Command
import subprocess
import os
import sys

file_path = os.path.dirname(os.path.realpath(__file__))

_gangaVersion = '8.3.4'

def version():
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
        ganga_python_dir = os.path.join(file_path, 'ganga')

        test_env = os.environ.copy()
        path = ':'.join(s for s in [ganga_python_dir, test_env.get('PYTHONPATH', None)] if s)
        test_env['PYTHONPATH'] = path

        return test_env

    def run(self):

        cmd = ['py.test']

        if self.type in ['unit', 'all']:
            cmd.append('ganga/GangaCore/test/Unit')
            cmd.append('ganga/GangaCore/Core')
            cmd.append('ganga/GangaCore/Runtime')
            cmd.append('ganga/GangaCore/Utility')
        if self.type in ['integration', 'all']:
            cmd.append('ganga/GangaCore/test/GPI')

        if self.coverage:
            cmd.append('--cov-report xml --cov .')
        if self.xunit:
            cmd.append('--junitxml tests.xml')

        subprocess.check_call(' '.join(cmd), cwd=file_path, shell=True, env=self._get_test_env())


pythonPackages = find_packages('./')
pythonPackages.append('ganga/GangaRelease')

setup(name='ganga',
      description='Job management tool',
      long_description=readme(),
      url='https://github.com/ganga-devs/ganga',
      version=version(),
      author='Ganga Developers',
      author_email='project-ganga-developers@cern.ch',
      license='GPL v2',
      scripts=['bin/ganga'],
      package_dir={'ganga':'ganga', 'GangaRelease':'ganga/GangaRelease'},
      packages=pythonPackages,
      install_requires=[
          'ipython>=5.0.0',
          'httplib2>=0.8',
          'absl-py>=0.1.2',
          'google-api-python-client',
          'google-auth-httplib2',
          'google-auth-oauthlib',
          'requests>=2.23.0',
          'Flask~=1.1.2',
          'PyJWT~=1.7.1',
          'Flask-SQLAlchemy~=2.4.3',
      ],
      extras_require={
          'dev': ['coverage', 'pytest', 'pytest-cov', 'pytest-pylint', 'pytest-mock'],
          'profiler' : ['memory_profiler'],
          'LHCb' : ['LbDevTools']},
      classifiers=[
          'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
          'Programming Language :: Python :: 3.6',
      ],
      include_package_data=True,
      package_data={'GangaCore': ['Runtime/HEAD_CONFIG.INI'], 'GangaRelease':['ReleaseNotes-*', 'tools/check-new-ganga.py', 'tools/ganga-cvmfs-install.sh', 'tools/ganga-cvmfs-install-dev.sh']},
      cmdclass={
          'tests': RunTestsCommand,
      },
      )
