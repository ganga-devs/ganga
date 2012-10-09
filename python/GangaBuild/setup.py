#!/usr/bin/env python
import glob
import sys
sys.path.append('/home/mkenyon/ganga/BuildBranch/ganga/python/GangaBuild/lib')

from ganga.distutils.config import setup

"""
Default distutils 'setup' method overwritten.
"""
setup(
    packages=[
        'ganga', 'ganga.distutils'
    ],
    
    package_dir={'': 'lib'},
      
    data_files=[]
)
