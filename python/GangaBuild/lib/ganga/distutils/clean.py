"""
Contains the distutils clean command customization for the Ganga build.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
import os

from distutils.cmd import Command
from distutils.dir_util import remove_tree

class clean (Command):
    """
    Custom implementation of distutils clean command.
    """

    description = "cleanup the module build directory"

    user_options = [("dist", "d", "also clean dist directory")]

    boolean_options = ['dist']

    help_options = []

    def initialize_options(self):
        self.all = None
        self.bdist_base = None
        self.build_base = None
        
    def finalize_options(self):
        self.set_undefined_options('build', ('build_base', 'build_base'))
        self.set_undefined_options('bdist', ('dist_dir', 'bdist_base'))
        
    def run(self):
        if os.path.exists(self.build_base):
            remove_tree(self.build_base, dry_run=self.dry_run)
        if os.path.exists(self.bdist_base):
            remove_tree(self.bdist_base, dry_run=self.dry_run)
        if os.path.exists("MANIFEST"):
            os.unlink("MANIFEST")
