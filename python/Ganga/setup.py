#!/usr/bin/env python
import glob
import sys
import os
sys.path.append('../stage/lib')

from ganga.distutils.config import setup

"""
Default distutils 'setup' method overwritten.
"""

setup(
    packages=[
        'ganga'
    ],
    
    package_dir={'': 'lib'},
      
    data_files=[]
)
