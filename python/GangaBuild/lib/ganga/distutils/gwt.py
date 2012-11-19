"""
Contains the distutils gwthost command of the dashboard build.

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

from ConfigParser import ConfigParser
from distutils.cmd import Command

class gwt(Command):
    """
    Implements the distutils gwthost command for the dashboard.
    
    It launches an existing GWT application in 'hosted mode'.

    @author: Ricardo Rocha <ricardo.rocha@cern.ch>
    @version: $Id: gwt.py,v 1.1 2008/07/23 13:16:55 rocha Exp $
    """

    """
    Description of the module.
    """
    description = "launch a GWT application in hosted mode"

    """
    Listing of the command options (including description).
    """
    user_options = [("host", "h", "Launches the GWT in the module in hosted mode"),
                    ("init", "i", "Creates the necessary dir/file structure for GWT in the module")]

    """
    Listing of help related command options.
    """
    help_options = []

    def initialize_options(self):
        """
        Initialize command options.
        """
        self.host = False
        self.init = False
        self.java_home = "/usr/lib/jvm/ia32-java-6-sun/jre"
        self.build_gwt_www = None
        self.build_gwt_bin = None
    
    def finalize_options(self):
        """
        Finalize command options.
        """
        pass
    
    def run(self):
        """
        Implementation of the gwthost command.
        """
        self.set_undefined_options('build', ('build_gwt_www', 'build_gwt_www'))
        self.set_undefined_options('build', ('build_gwt_bin', 'build_gwt_bin'))
        
        # Load the external module configuration
        config = ConfigParser()
        config.read(["module.cfg"])
        moduleName = str(config.get("module", "name"))
        (gwtAppPackage, gwtAppName) = gwt.getGWTApp()
        
        # Invoke the GWT app in hosted mode
        gwtHomeDir = "../arda.dashboard/config/gwt"
        gwtHostedApp = "com.google.gwt.dev.GWTShell"
        if self.host:
            classpath = [self.java_home,
                         os.path.join("gwt", "src"), self.build_gwt_bin,
                         os.path.join(gwtHomeDir, "gwt-user.jar"), 
                         os.path.join(gwtHomeDir, "gwt-dev-linux.jar"),]
            if self.java_home is not None:
                os.environ["PATH"] = "%s:%s" % (os.path.join(self.java_home, "bin"), 
                                                os.environ["PATH"])
            cmd = "java -Xmx256M -cp '%s' '%s' -out '%s' %s.%s/%s.html " \
                  % (":".join(classpath), gwtHostedApp, self.build_gwt_www,
                     gwtAppPackage, gwtAppName, gwtAppName)
            print cmd
            (code, output) = commands.getstatusoutput(cmd)
            print output
        elif self.init:
            print "gwtinit"

    def getGWTApp(cls):
        # Load the external module configuration
        config = ConfigParser()
        config.read(["module.cfg"])
        moduleName = str(config.get("module", "name"))
        gwtAppPackage = moduleName.replace("-", ".")
        gwtAppName = ""
        for part in moduleName.split("-"):
            gwtAppName += part[0].upper() + part[1:]
        return (gwtAppPackage, gwtAppName)
    getGWTApp = classmethod(getGWTApp)
            