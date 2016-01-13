from distutils.core import setup
from setuptools import find_packages
import subprocess
from distutils.sysconfig import get_python_lib
from distutils.command.install import install as _install


def _post_install(dir):
    # Install stomputil
    cmd = 'curl http://ganga.web.cern.ch/ganga/download/stomputil-2.4-noarch-ganga.tar.gz | tar xz --strip-components=4 -C %s' % get_python_lib()
    subprocess.check_call([cmd], shell=True)


class install(_install):
    def run(self):
        _install.run(self)
        self.execute(_post_install, (self.install_lib,), msg="Running post install task")


setup(name='ganga',
      description='Job management tool',
      url='https://github.com/ganga-devs/ganga',
      version='6.1.14',
      scripts=['bin/ganga'],
      package_dir={'': 'python'},
      packages=find_packages('python'),
      install_requires=[
          'ipython>=3.2.1',
          'paramiko>=1.7.3',
          'pycrypto>=2.0.1',
          'httplib2>=0.8',
          'python-gflags>=2.0',
          'google-api-python-client>=1.1',
      ],
      cmdclass={'install': install},
      )
