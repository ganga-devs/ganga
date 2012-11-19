#!/usr/bin/env python
import glob
import sys
import os
sys.path.append('../../../stage/lib')
print os.listdir('./')
print os.getcwd()

from ganga.distutils.config import setup

"""
Default distutils 'setup' method overwritten.
"""

setup(
    packages=[
         'ganga'
    ],

    data_files=[]
)
#setup(
#    packages=[
#        'ganga'
#    ],
#    
#    package_dir={'': 'lib'},
#      
#    data_files=[]
#)
