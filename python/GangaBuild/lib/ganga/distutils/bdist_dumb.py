"""
Contains the Ganga changes to the bdist_dumb command.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from distutils.command.bdist_dumb import bdist_dumb as _bdist_dumb
from ganga.distutils.bdist_rpm import adjustVersionNumber

class bdist_dumb(_bdist_dumb):
    def run(self):
        version = self.distribution.get_version()
        new_version = adjustVersionNumber(version)
        self.distribution.metadata.version = new_version
        _bdist_dumb.run(self)
        self.distribution.metadata.version = version




