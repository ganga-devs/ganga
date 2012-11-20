#!/usr/bin/env python
import glob
import sys
import os
sys.path.append('/home/mkenyon/ganga/GangaBuild/python/GangaBuild/lib')

from ganga.distutils.config import setup

"""
Default distutils 'setup' method overwritten.
"""

setup(
    packages=[
        'ganga',
        'ganga.distutils', 'ganga.distutils.xmlutils'
    ],
    
    package_dir={'': 'lib'},
      
    data_files=[]
)
