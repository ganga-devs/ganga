"""
Contains the distutils build command extension for the Ganga build.

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
import os.path

from distutils.command.build import build as _build
from ganga.distutils.xmlutils import xpath

from xml.dom import minidom

from ganga.distutils.gwt import gwt

class build(_build):
    """
    Implementation the distutils build command extension for Ganga.
    """

    description = "build everything needed to install"

    user_options = _build.user_options
    user_options.extend([
                   ('cli-file=', 'f', 'dashboard cli tools config file to use'),
                   ('include-docs', 'd', 'build module documentation'),
                   ])
    
    boolean_options = ['include-docs']

    help_options = []

    def initialize_options(self):
        _build.initialize_options(self)
        self.gwt_base = "gwt"
        self.bdoc_base = None        
        self.build_bin = os.path.join(self.build_base, "bin")
        self.build_gwt = os.path.join(self.build_base, "gwt")
        self.build_gwt_www = os.path.join(self.build_gwt, "www")
        self.build_gwt_bin = os.path.join(self.build_gwt, "bin")
        self.cli_file = os.path.join('config', 'cli', 'tools.xml')
        self.include_docs = 0
        self.java_home = "/usr/lib/jvm/ia32-java-6-sun/jre"
        
    def finalize_options(self):
        _build.finalize_options(self)
                
    def run(self):        
        self.set_undefined_options('doc', ('bdoc_base', 'bdoc_base'))        
        if not os.path.exists(self.build_base):
            self.mkpath(self.build_base)
        
        if os.path.exists(self.cli_file):
            self.mkpath(self.build_bin)
            self.mkpath(os.path.join(self.build_base, "config"))
            toolsXSL = "config/cli/tools.xsl"
            if not os.path.exists(toolsXSL):
                toolsXSL = "../arda.dashboard/config/cli/tools.xsl"
            cod, output = commands.getstatusoutput("xsltproc --stringparam binPath %s " \
                                                   "%s %s" 
                                                   % (self.build_bin, toolsXSL, self.cli_file))
            if cod != 0:
                self.warn(output)
            # Update the binary file permissions where needed
            toolsFile = minidom.parse(self.cli_file)
            tools = xpath.find(toolsFile, '/dashboardCli/tool')
            for tool in tools:
                mode = 0755
                if tool.hasAttribute("mode"):
                    mode = int(tool.getAttribute("mode"), 8)
                os.chmod(os.path.join(self.build_bin, str(tool.getAttribute("name"))),
                         mode)
        if os.path.exists(self.gwt_base):
            self.mkpath(self.build_gwt)
            self.build_gwt_app()

        docCmd = self.distribution.get_command_obj("doc")
        docCmd.finalize_options()
        docCmd.run()
        
        _build.run(self)

    def build_gwt_app(self):
        (gwtAppPackage, gwtAppName) = gwt.getGWTApp()
        gwtHomeDir = os.path.join("..", "arda.dashboard", "config", "gwt")
        gwtCompileApp = "com.google.gwt.dev.GWTCompiler"
        classpath = [self.java_home, os.path.join(self.gwt_base, "src"),
                     os.path.join(gwtHomeDir, "gwt-user.jar"), 
                     os.path.join(gwtHomeDir, "gwt-dev-linux.jar"),]
        if self.java_home is not None:
            os.environ["PATH"] = "%s:%s" % (os.path.join(self.java_home, "bin"), os.environ["PATH"])
        cmd = "java -Xmx256M -cp '%s' '%s' -out '%s' -gen '%s' " \
              "%s.%s" \
              % (":".join(classpath), gwtCompileApp, self.build_gwt_www, 
            os.path.join(self.build_gwt, "bin"), gwtAppPackage, gwtAppName)
        print cmd
        (code, output) = commands.getstatusoutput(cmd)
        print output
        
