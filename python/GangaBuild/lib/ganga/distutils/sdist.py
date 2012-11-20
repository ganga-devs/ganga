"""
Contains the distutils sdist command extension for the Ganga build.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
import os.path

from distutils.command.sdist import sdist as _sdist
from distutils.dir_util import remove_tree
from ganga.distutils.bdist_rpm import adjustVersionNumber

class sdist(_sdist):
    """
    Implementation the distutils sdist command extension for the Ganga.
        
    @author: Ricardo Rocha <ricardo.rocha@cern.ch>
    @version: $Id: sdist.py,v 1.4 2007/01/31 09:42:01 rocha Exp $
    """

    description = "create a source distribution (tarball, zip file, etc.)"
    
    boolean_options = []

    help_options = []

    def initialize_options(self):
        _sdist.initialize_options(self)
    
    def finalize_options(self):
        _sdist.finalize_options(self)
        
    def run(self):
        self.cli_file = self.distribution.get_command_obj("build").cli_file

        toolsXSL = "config/cli/tools.xsl"        
        if os.path.exists(self.cli_file):
            if os.path.exists(toolsXSL):
                os.remove(toolsXSL)
            os.link("../arda.dashboard/config/cli/tools.xsl", toolsXSL)

        docbookDir = "doc/docbook-utils"
        if os.path.exists("doc"):
            if os.path.exists(docbookDir):
                remove_tree(docbookDir)
            self.copy_tree("../arda.dashboard/config/doc/docbook-utils", docbookDir)
                    
        commonManPages = "doc/man/common"        
        if os.path.exists(os.path.join("doc", "man")):
            if not os.path.exists(commonManPages):
                self.copy_tree("../arda.dashboard/config/doc/man/common", commonManPages)

        #Set the version according to the name we want on the resulting archive
        version = self.distribution.get_version()
        new_version = adjustVersionNumber(version)
        self.distribution.metadata.version = new_version

        _sdist.run(self)
        
        self.distribution.metadata.version = version

        if os.path.exists(toolsXSL):
            os.remove(toolsXSL)
        if os.path.exists(docbookDir):
            remove_tree(docbookDir)
        if os.path.exists(commonManPages):
            remove_tree(commonManPages)

