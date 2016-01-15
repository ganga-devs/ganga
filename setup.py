from setuptools import setup
from setuptools import find_packages

def readme():
    with open('README.rst') as f:
        return f.read()


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
      )
