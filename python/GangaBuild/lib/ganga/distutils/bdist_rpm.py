"""
Contains the Ganga changes to the bdist_rpm command.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

import re
import sys
from distutils.command.bdist_rpm import bdist_rpm as _bdist_rpm

class bdist_rpm(_bdist_rpm):
    def initialize_options(self):
        self.user_options.append(('requires26', None, 'Requirements in python 2.6'))
        self.requires26=None
        _bdist_rpm.initialize_options(self)

    def run(self):
        version = self.distribution.get_version()
        new_version = adjustVersionNumber(version)
        if (self.requires26 and sys.version_info > (2, 6,0)):
            print "  WE HAVE REQUIREMENTS for 2.6, and we are running 2.6"
            self.requires= self.requires26.split()

        version = self.distribution.get_version()
        new_version = adjustVersionNumber(version)
        self.distribution.metadata.version = new_version
        _bdist_rpm.run(self)

        self.distribution.metadata.version = version


def adjustVersionNumber(version):
    """
    Append "_stable" to stable versions. (Stable versions in the Ganga
    are those that do not terminate in '_rc<number>'.)
    """

    if not re.match(".*_rc\d+$", version) and not \
           re.match(".*_stable$", version):
        version = version + "_stable"
    return version




