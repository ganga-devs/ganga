#!/usr/bin/env python

# Helper script to return the environment changes required to setup Ganga external packages

from __future__ import print_function

import sys
import os
import os.path

python_path = os.path.join(sys.argv[1], 'python')

sys.path.insert(0, python_path)

# Loop over modules
syspath_list = []
for mod in ['Ganga', 'GangaAtlas']:#os.listdir(python_path):

    # import parent package
    mod_imp = __import__(mod + ".PACKAGE")

    # loop over all the required packages
    for name in mod_imp.PACKAGE.setup.packages:

        # ASSUME that if we have a syspath, this is a python package and can be imported
        path = mod_imp.PACKAGE.setup.getPackagePath2(name, 'syspath')

        if not path:
            continue

        # attempt the import - add the external path if the import fails for any reason
        try:
            test_imp = __import__(name)
        except:
            syspath_list.append(path)


# print out the required env line
print(":".join(syspath_list))
