"""
Contains the distutils stage command customization for the dashboard build.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
import commands
import os
import sys

from distutils.cmd import Command

class stage(Command):
    """
    Implements the distutils stage command for the dashboard.
    
    This is a wrapper around the install command, with specific 'install-purelib',
    'install-data' option parameters.

    @author: Ricardo Rocha <ricardo.rocha@cern.ch>
    @version: $Id: stage.py,v 1.21 2007/12/11 11:16:58 bgaidioz Exp $
    """

    description = "put module data/libraries in the stage directory"

    user_options = [
        ('stage-base=', 's', 
         'alternative stage directory (default is ../stage)'),
        ('include-tests', 't', 'stage test modules from the test directory'),
        ('all', 'a', 'stage all modules in the current workspace'),
    ]

    boolean_options = []

    help_options = []

    def initialize_options(self):
        self.all = 0
        self.include_tests = 0
        self.install_purelib = None
        self.install_data = None
        self.stage_base = None

    def finalize_options(self):
        if self.stage_base is None:
            self.install_purelib = os.path.join("..", "stage", "lib")
            self.install_data = os.path.join("..", "stage")
        else:
            self.install_purelib = os.path.join(self.stage_base, "lib")
            self.install_data = os.path.join(self.stage_base)

    def run(self):
        """
        Run the stage command.
        """
        if self.all:
            currentDir = os.getcwd()
            modDirs = os.listdir("..")
            # sort module directories alphabetically so arda.dashboard comes first
            modDirs.sort()
            for modDir in modDirs:
                if os.path.exists(os.path.join("..", modDir, "setup.py")):
                    os.chdir(os.path.join("..", modDir))
                    sys.stdout.write("Staging '%s' ... " % modDir)
                    sys.stdout.flush()
                    resCode, output = commands.getstatusoutput("python setup.py stage")
                    if resCode != 0:
                        sys.stdout.write("failed\n")
                    else:
                        sys.stdout.write("ok\n")
            os.chdir(currentDir)
        else:
            installCmd = self.distribution.get_command_obj("install")
            # TODO: Did not work using 
            # installCmd.set_undefined_options('stage',
            #                                 ('install_data', 'install_data'))
            # Might be worth to try it again later
            installCmd.install_purelib = self.install_purelib
            installCmd.install_lib = self.install_purelib
            installCmd.install_data = self.install_data
            installCmd.run()
        
            # Stage test modules if the proper param was given
            if self.include_tests:
                if os.path.exists("test"):
                    self.copy_tree("test", self.install_purelib)
