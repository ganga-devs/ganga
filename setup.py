from setuptools import setup
from setuptools import find_packages
from setuptools import Command
import subprocess
import os


def readme():
    with open('README.rst') as f:
        return f.read()


class RunTestsCommand(Command):
    """
    A custom setuptools command to run the Ganga test suite.
    """
    description = 'run the Ganga test suite'
    all_types = ['unit', 'integration', 'all']
    user_options = [
        ('type=', 't', 'the type of tests: [{0}]'.format(', '.join(all_types))),
    ]

    def initialize_options(self):
        self.type = 'unit'

    def finalize_options(self):
        if not self.type in self.all_types:
            raise Exception('Test type must be [{0}]'.format(', '.join(self.all_types)))

    def run(self):
        os.environ['GANGASYSROOT'] = os.path.dirname(os.path.realpath(__file__))
        if self.type == 'unit':
            print('Running unit tests')
            subprocess.check_call('nosetests Ganga/new_tests/*.py', cwd='python', shell=True)
        elif self.type == 'integration':
            print('Running integration tests')
            subprocess.check_call('nosetests Ganga/new_tests/GPI --testmatch="(?:\\b|_)([Tt]est|Savannah|JIRA)"', cwd='python', shell=True)
        elif self.type == 'all':
            print('Running all tests')
            subprocess.check_call('nosetests Ganga/new_tests/*.py Ganga/new_tests/GPI --testmatch="(?:\\b|_)([Tt]est|Savannah|JIRA)"', cwd='python', shell=True)


setup(name='ganga',
      description='Job management tool',
      long_description=readme(),
      url='https://github.com/ganga-devs/ganga',
      version='6.1.14',
      author='Ganga Developers',
      author_email='project-ganga-developers@cern.ch',
      license='GPL v2',
      scripts=['bin/ganga'],
      package_dir={'': 'python'},
      packages=find_packages('python'),
      install_requires=[
          'ipython>=1.2.1',
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
