#!/usr/bin/env python
import glob
import sys
import os
sys.path.append('/home/mkenyon/ganga/BuildBranch/ganga/python/GangaBuild/lib')

from ganga.distutils.config import setup

"""
Default distutils 'setup' method overwritten.
"""

packages = []
data_files = []
for root, dirs, files in os.walk('./'):
    #print root, dirs, files
    if '__init__.py' in files:
        packages.append(root.replace('/','.'))
    for file in files:
    #    print os.path.join(root,file)
        data_files.append(file)

setup(
    packages=packages,
    
    data_files=[]
      
)

