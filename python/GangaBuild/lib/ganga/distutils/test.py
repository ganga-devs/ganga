"""
Contains the distutils test command customization for the Ganga build.

@license: Apache License 2.0
"""
"""
Copyright (c) Members of the EGEE Collaboration. 2004.
See http://www.eu-egee.org/partners/ for details on the copyright holders.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""
import imp
import inspect
try:
    import libxml2
    import libxslt
except ImportError:
    pass
import os.path
import re
import sys
import time
import types
import unittest

from ConfigParser import ConfigParser
from distutils.cmd import Command
from xml.sax import saxutils

from ganga.test.TestRunner import TestRunner

class test(Command):
    """
    Implementation of the distutils 'test' command extension for the Ganga.
        
    @author: Ricardo Rocha <ricardo.rocha@cern.ch>
    @version: $Id: test.py,v 1.18 2008/04/18 17:07:57 rocha Exp $
    """

    description = "run all module unit tests"

    user_options = [('test-suite=', 't', "test suite to run (optional)"),
                    ('test-dir=', 'd', "directory where test results should be stored"),
                    ('xsl-file=', 'x', "xsl document used over the xml result")]

    boolean_options = []

    help_options = []

    def initialize_options(self):
        self.test_dir = None
        self.test_suite = None
        self.xsl_file = None
        
    def finalize_options(self):
        if self.test_dir is None:
            self.test_dir = os.path.join("build", "test")
        if self.xsl_file is None:
            self.xsl_file = os.path.join("..", "arda.dashboard", "config", "test", "results.xsl")
        
    def run(self):
        """
        Runs the distutils test command.
        """
        suites = []
        
        # Stage the module libraries (including the test modules)
        stageCmd = self.distribution.get_command_obj("stage")
        stageCmd.include_tests = 1
        stageCmd.finalize_options()
        stageCmd.run()        

        self.mkpath(self.test_dir)
        # Create the suites and add them to the list of suites
        if self.test_suite is not None:
            suites.append((self.test_suite, unittest.TestLoader().loadTestsFromName(self.test_suite)))
        else:
            # Nothing was passed in the command line, so
            # build a suite with all modules that are tests in the 'test' directory
            testRegexp = re.compile(".*Test.py$")
            for tDir, dirs, files in os.walk("test"):
                for dirFile in files:
                    if testRegexp.match(dirFile) != None:
                        package = tDir[tDir.find("/")+1:].replace("/", ".")              
                        moduleName = dirFile[:-3]
                        packagedModuleName = package + "." + moduleName
                        module = self._loadPackagedModule(packagedModuleName)
                        testClass = getattr(module, moduleName)
                        suites.append((moduleName, unittest.makeSuite(testClass)))

        # Get the package name
        config = ConfigParser()
        configFile = open('module.cfg')
        config.readfp(configFile)
        configFile.close()
        packageName = config.get("module", "name")
        
        # Run each of the test suites in the list and collect results
        numFailures = 0
        numTests = 0
        results = []
        start = time.time()        
        for name, suite in suites:
            result = TestRunner().run(name, suite)
            results.append(result)
            numFailures += result["numFailures"]
            numTests += result["numTests"]
        end = time.time()
        
        # Store the result in a XML format
        xmlResult = self.serialize({"packages": [{"name": packageName,
                                                  "testruns": [{"testsuites": results,
                                                                "time": "%.2f" % (end - start),
                                                                "numTests": numTests,
                                                                "numFailures": numFailures}
                                                               ]}
                                                 ]
                                    })
        xmlFile = open(os.path.join(self.test_dir, "results.xml"), "w")
        xmlFile.write(xmlResult)
        xmlFile.close()
        
        # Load the XML stylesheet
        try:
            styleDoc = libxml2.parseFile(self.xsl_file)
            style = libxslt.parseStylesheetDoc(styleDoc)
        except libxml2.parserError, exc:
            self.warn("Failed to parse xslt file: '%s'. Error: %s" % (self.xsl_file, exc))
            
        # Parse the XML using the stylesheet and store result
        xmlDoc = libxml2.parseDoc(xmlResult)
        xhtmlResult = style.applyStylesheet(xmlDoc, None).serialize()
        xhtmlFile = open(os.path.join(self.test_dir, "results.html"), "w")
        xhtmlFile.write(xhtmlResult)
        xhtmlFile.close()        
    
    def _loadPackagedModule(self, moduleName):
        """
        Loads a module starting from the fully qualified module name.
    
        Takes the dotted module name (i.e. 'dashboard.package1.ModuleName')
        and loads each package individually and finally the module also.
    
        The module is not reloaded if it was loaded before.
    
        @param moduleName: The dotted module name
        @return: A reference to the loaded module
        """
        modElements = moduleName.split('.')
        path = None
        tmpMod = None
        # Only load the module if it is not already loaded
        try:
            tmpMod = sys.modules[modElements[len(modElements)-1]]
        except KeyError:
            for index, elem in enumerate(modElements):
                (modFile, modPath, modDesc) = imp.find_module(elem, path)
                tmpMod = imp.load_module(elem, modFile, modPath, modDesc)
                if index != len(modElements)-1:
                    path = tmpMod.__path__
            return tmpMod    
        
    def serialize(self, obj):
        """
        """
        xmlStr = ""
    
        if type(obj) == types.ListType:
            for value in obj:
                xmlStr += "<item>" + self.serialize(value) + "</item>"
        elif type(obj) == types.DictType:
            for key in obj.keys():
                xmlStr += "<" + key + ">" + self.serialize(obj[key]) + "</" + key + ">"
        elif type(obj) == types.NoneType:
            xmlStr += ""
        else:
            xmlStr += saxutils.escape(str(obj))

        return xmlStr
