#!/usr/bin/env python
import glob
import sys
import os
sys.path.append('../stage/lib')
print os.listdir('./')
print os.getcwd()

from ganga.distutils.config import setup

"""
Default distutils 'setup' method overwritten.
"""

data_files=[]
for root, dirs, files in os.walk('./'):
  if '.svn' in dirs:
      dirs.remove('.svn')
  for filename in files:
      print os.path.join(root, filename)
      data_files.append(os.path.join(root,filename))

setup(
    name='mjk',
    version='1.0',
    
#    packages=[
#         'ganga'
#    ],

    data_files=data_files
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
