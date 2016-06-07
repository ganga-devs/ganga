###############################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: loader.py,v 1.2 2008-11-26 08:31:33 moscicki Exp $
#
# Copyright (C) 2003-2007 The Ganga Project
#
# This file is part of Ganga. 
#
# Ganga is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# Ganga is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
################################################################################

from __future__ import generators
import sys
import os
import os.path
import unittest
from pytf.lib import *
from GangaTest.Framework.tests import GangaGPIPTestCase

__version__ = "1.2"
__author__="Adrian.Muraru[at]cern[dot]ch"


#default timeout in seconds for a test
DEFAULT_TEST_TIMEOUT = 120

class GangaTestLoader:
    """
    Ganga specific TestLoader:
    Loads all the tests from project, filtering based on the given pattern (e.g. Ganga.test.Bugs.* .
    There are two types of tests that may be loaded :
        - *Release Tests* : tests distributed within Ganga distribution and located in a separate branch for each top-level package ( e.g. Ganga/old_test/)
        - *Local Tests*   : local test-cases loaded from arbitrary locations 
    """    
    def __init__(self,args,logger=None):        
        
        #optional logger
        self.logger=logger
        
        #test suite files
        self.config = loadArg(args,0, default="default.ini").split(":")
        self.configAlias=''
        
        # automatically append user and site specific configuration when running test-cases        
        from Ganga.Utility.Config import getConfig
        syscfg = getConfig('System')
        self.userConfig = ":".join([os.path.expanduser(syscfg['GANGA_CONFIG_FILE']),
                                    syscfg['GANGA_CONFIG_PATH']])
        
        self.testID = getConfig('Configuration')['user']
        #default path to search tests in
        self.testsTopDir = os.getcwd()
        
        #Ganga top dir
        self.releaseTopDir = os.path.abspath(loadArg(args,1))    
        
        #release testing mode
        self.releaseTesting = int(loadArg(args,2,default=False))
        
        #the path to save the tests' stdout        
        self.outputPrefix = os.path.abspath(loadArg(args,3,default='./'))
        
        self.enableLocalTests = int(loadArg(args,4,default=True))
        self.enableReleaseTests = int(loadArg(args,5,default=True))
        self.report_outputpath = loadArg(args,6,default='./')
        self.schema_test = loadArg(args,7,default='')
        self.current_report_name = None
        
        #set the verbosity based on GangaTest.Framework logging config           
        from Ganga.Utility.logging import getLogger
        tfLogger = getLogger(name='GangaTest.Framework')
        if tfLogger.level < 20 :#DEBUG
            self.verbose = True
        else:
            self.verbose = False
        print self.releaseTesting

    def loadTests(self,patterns):
        """
        pattern format should be: TopLevelPackage.test.A.B.C ... 
        Update 27.02.07:
        -pattern can also be a file/dir containing tests (load tests from arbitrary locations)
        """
        #TODO: support also multiple patterns to be specified ?
        patterns=patterns[:1]
        _log(self.logger,'info',"Test search pattern: %s" % patterns[0])
        
        # release tests take precedence
        if self.enableReleaseTests:
            #work on copy
            _patterns = patterns[:]            
            self.testsTopDir = "%s/python" % self.releaseTopDir
            #_log(self.logger,'info',"Searching release tests in %s " % self.testsTopDir)
            
            new_pattern = self.__convertSearchPattern(_patterns)
            if new_pattern:
                #change the pattern used to select release tests 
                _patterns[0]=new_pattern
            
            pattern = _patterns[0]
            _log(self.logger,'info',"Searching release tests matching [%s] in %s " % (pattern,self.testsTopDir))
            atoms = pattern.split(".")
            print str( patterns ) + " : " + str(atoms)
            if len(atoms) > 2:
                top_level_package = atoms[0]
                tests_type = atoms[1]
                if tests_type == "RT" or tests_type == "old_test" :
                    release_tests = self._loadReleaseTests(pattern)
                    if release_tests:
                        return release_tests
            #no release test found
            _log(self.logger,'info',"No release tests was found matching %s." % pattern)

        #search for local tests    
        if self.enableLocalTests:
            local_tests = None
            _log(self.logger,'info',"Searching local tests in: %s" % ' '.join(patterns))
            #change the default (release tests) top dir
            self.testsTopDir = os.getcwd()

            local_tests = self._loadLocalTests(patterns)
            if local_tests:
                _msg = "Running local tests from:"
                for p in patterns:
                    _msg = "%s\t %s" % (_msg,p)
                _log(self.logger,'info', _msg)
                return {'unittest':unittest.TestSuite(local_tests)}
            #else
            _log(self.logger,'info',"No local test found...")

        return {}        
    
    def filterTests(self,testcases,pattern):
        import fnmatch
        pattern = pattern.replace('.','/')
        filtered_tests = []        
        for key in testcases.keys():
            tests=testcases[key]            
            for t in tests:
                if key.startswith(pattern) or fnmatch.fnmatch(t.getDescription().split()[0],pattern):
                    filtered_tests.append(t)
        return filtered_tests
                
    def _loadReleaseTests(self,pattern):
        import os,re
        from os.path import join, getsize
        
        top_level_package = pattern.split(".")[0]
        dirname = os.path.abspath("%s/%s/%s"%(self.testsTopDir,top_level_package,"old_test"))          
        #print "Project RELEASE TOP_DIR: %s"%self.releaseTopDir
        #print "Project TESTS TOP_DIR: %s"%self.testsTopDir
        #print "Package: %s\nPattern: %s "%(top_level_package, pattern)
        print "Test configuration(s):",self.config 

        suites = {}
        tests_no=0
        for config in self.config:
            config_path = "%s/%s/old_test/config/%s"%(self.testsTopDir,top_level_package,config)
            config_path = "%s:%s" % (config_path,self.userConfig)            
            
            self.configAlias = os.path.splitext(config)[0]
            report_name = "%s__%s"%(pattern.replace("*","ALL"),self.configAlias)
            if self.schema_test is not '':
                report_name = "%s_%s_%s__%s"%(pattern.replace("*","ALL"),self.schema_test,self.testID,self.configAlias)
            print "Using configuration: %s" % config_path 
            print "Report ID: %s" % report_name

            self.current_report_name = report_name

            #the tests container
            tests = self.__walkTests(dirname,pattern,config_path)
            tests_no+=len(tests)
            if tests:
                suites[report_name]=unittest.TestSuite(tests)
        print "%s tests found" % tests_no
        return suites
                
    def __convertSearchPattern(self,patterns):
        """
         translate the input pattern into the internal, dot separated,representation 
        """
        sys_pattern = patterns[0]
        sys_tests_path= os.path.join(self.testsTopDir,sys_pattern)  
        if os.path.exists(sys_tests_path):
            if os.path.isdir(sys_tests_path):
                return os.path.normpath(sys_pattern).replace('/','.')+".*"
            elif os.path.isfile(sys_tests_path):
                if sys_pattern.endswith('.gpi'):
                    return sys_pattern[:-4].replace('/','.')
                elif sys_pattern.endswith('.gpim'):
                    return sys_pattern[:-5].replace('/','.')
                elif sys_pattern.endswith('.py'):
                    return sys_pattern[:-3].replace('/','.')+".*"
                elif sys_pattern.endswith('.gpip'):
                    return sys_pattern[:-5].replace('/','.')+".*"
        return sys_pattern.replace('/','.')
        
        
    def __walkTests(self,dirname,pattern,config):
        """
        Collects all tests from path specified by pattern 
        Parameters:
        pattern : specifies a pattern to select the set of tests to be loaded
        Individual tests can be defined either as separate files (.gpim,.gpi) grouped in directories
        or can be defined in "classic" pyunit test cases (.py)
        The pattern is interpreted as a path to a list of individual tests:
        Example:
        The following physical structure for tests:
            Ganga/old_test/
                  /Folders/
                      testA.gpi
                      testB.gpi
                      TestGroup.py {multiple tests embedded: testC, testD...}
                  /Folders.py {multiple tests embedded: testE, testF...}
        
        is transformed in the logical structure:
            Ganga/old_test/Folders
                           /{testA,testB,testE,testF}
                           /TestGroup{testC,testD}                     
        So, the pattern Ganga.test.Folders.* will match all the tests 
        while the pattern Ganga.test.Folders.TestGroup.* will match just the test belonging to TestGroup
        Invidual tests can be selected by specifying the complete path: 
                  Ganga.test.Folders.TestGroup.testE or Ganga.test.Folders.testA
        
        N.B.:
            It may be possible that a complete path to match multiple tests if a test with the same name is defined as a single test(.gpi,.gpim) in a directory
            and also in a PyUnit testcase class
            
        """

        tests={}
        #the tests container
        topdir = self.testsTopDir        
        for root,dirs,files in walk(os.path.join(dirname)):
            if len(files)>0:
                for file in files:
                    #if file.startswith('.'): print "Skipped", file; continue;
                    if file.endswith(".gpi") or file.endswith(".gpim"):
                        key=root[root.index(topdir)+len(topdir)+1:]
                        list = tests.get(key,[])
                        if file.endswith(".gpi"):
                            test = self.__generateTestCaseFromGPITest(os.path.join(root,file),config)
                            if test is not None: list.append(test)
                        elif file.endswith(".gpim"):
                            test = self.__generateTestCaseFromGMPTest(os.path.join(root,file),config)
                            if test is not None: list.append(test)
                        tests[key]=list
                    elif file.endswith(".py") and file.startswith('Test'):
                        newroot= os.path.join(root,get_name(file))
                        key=newroot[newroot.index(topdir)+len(topdir)+1:]
                        list = tests.get(newroot,[])
                        #list.append("%s tests"%file)
                        pys = self.__generateTestCasesFromPyUnitTest(os.path.join(root,file),config)
                        if type(pys)==type([]) and len(pys)>0:
                            list+=pys
                        tests[key]=list       
                    elif file.endswith(".gpip") and file.startswith('Test'):
                        newroot= os.path.join(root,get_name(file))
                        key=newroot[newroot.index(topdir)+len(topdir)+1:]
                        list = tests.get(newroot,[])
                        #list.append("%s tests"%file)
                        gpips = self.__generateTestCasesFromGPIPTest(os.path.join(root,file),config)
                        if type(gpips)==type([]) and len(gpips)>0:
                            list+=gpips
                        tests[key]=list       
        filtered = self.filterTests(tests,pattern)
        filtered.sort(lambda t1,t2: cmp(t1.getDescription(),t2.getDescription()))
        return filtered

    def _loadLocalTests(self,locations):
        """
        Load tests from *location* : can be a single file or a directory
        """
        import os,glob
        tests = []
        for location in locations:            
            if os.path.exists(location):
                paths = [location]
            paths = glob.glob(location)            
            if len(paths)==0:
                guess = [location+".gpi",location+".gpim",location+".py",location+".gpip"]                
                paths = [l for l in guess if os.path.exists(l) and os.path.isfile(l)]
                
            if len(paths)==0:
                continue
            
            #else, we found smth
            for l in paths:                
                #if the location is inside GANGA_PACKAGE/old_test/ dir then
                #use (as in release tests) the GANGA_PACKAGE/old_test/config dir to load
                #the configuration from
                tpath = fullpath(l)
                rpath = os.path.join(self.releaseTopDir,'python')
                idx = tpath.rfind(rpath)                
                if idx >= 0:
                    package = tpath[idx+len(rpath)+1:].split(os.sep)[0]
                else:
                    package = 'Ganga'                

                if os.path.isfile(l):
                    tests+=self.__loadLocalTest(l,gangaPackage=package)
                elif os.path.isdir(l):
                    for file in dirwalk(l):
                        tests+=self.__loadLocalTest(file,gangaPackage=package)
        if len(tests)==0:
            return None        
        tests.sort(lambda t1,t2: cmp(t1.getDescription(),t2.getDescription()))
        return tests
    
    def __loadLocalTest(self,file,gangaPackage='Ganga'):
        """
        Load test(s) from file
        Return:
         a list containing PYTF testcases
        """
        list = []
        
        # for each test configuration specified 
        for config in self.config:              
            self.configAlias = os.path.splitext(config)[0]            
            # construct the test configuration path
            # 1. User config : GANGA_CONFIG_FILE:GANGA_CONFIG_PATH
            config_path = "%s/python/%s/old_test/config/%s"%(self.releaseTopDir,gangaPackage,config)
            ganga_config_path = ":".join([config_path,self.userConfig])

            ext = get_ext(file)
            if ext in (".gpi",".gpim"):
                test = None
                if ext == ".gpi":
                    test = self.__generateTestCaseFromGPITest(file,ganga_config_path,test_relpath=os.path.dirname(file))
                elif ext == ".gpim":
                    test = self.__generateTestCaseFromGMPTest(file,ganga_config_path,test_relpath=os.path.dirname(file))
                if test is not None: list.append(test)
            elif ext.lower() == ".gpip" and get_name(file).startswith('Test'):
                print 'Load local GPIP test'
                gpips = []
                gpips = self.__generateTestCasesFromGPIPTest(file,ganga_config_path,test=None,test_relpath=os.path.dirname(file))
                if type(gpips)==type([]) and len(gpips)>0:
                    list+=gpips
            elif ext.lower() == ".py" and get_name(file).startswith('Test'):            
                pys=[]
                pys = self.__generateTestCasesFromPyUnitTest(file,ganga_config_path,test=None,test_relpath=os.path.dirname(file))
                if type(pys)==type([]) and len(pys)>0:
                    list+=pys
                
        return list


    def __generateTestCaseFromCmd(self, testCmd, testName, testAlias=None, 
                                setUpCmd=None, tearDownCmd=None, description=None, 
                                output_path=None, timeout=sys.maxsize):
        """
        Add a single ganga test-case from a gpi test (similar to run_external_test)
        Internally this method decorates the setUp->[testCmd]->tearDown in a TestCase
        Arguments:
         output_path: when specified, then the output is captured in the given file
         timeout: the testcase is reported as failed if it passes the specified time in seconds
        """
        if testAlias is None:
            testAlias = testName
        method_name = testName
        d = {}
        #todo: use a VARS dictionary for variable used to create the wrapper script
        VARS={}
        VARS['method_name']=method_name
        VARS['verbose'] = self.verbose
        VARS['timeout'] =timeout
        VARS['output_path']=output_path
        VARS['description']=description
        VARS['setUpCmd']=setUpCmd
        VARS['tearDownCmd']=tearDownCmd    
        
        code="""

def %(method_name)s(self):
    import unittest
    import time
    
    def read_file(filename):
        f = open(filename)
        try: return f.read()
        finally: f.close()

    def write_file(filename,content):
        f = open(filename,'w')
        try: return f.write(content+"\\n")
        finally: f.close()
   
    process = None # used as handler to the test process    
    pytf_paths = str.split(os.getenv('PYTF_TOP_DIR'),':')
    pytf_runner = str()
    if type(pytf_paths) is str:
        pytf_runner = os.path.join( pytf_paths, 'cmd_run.sh' )
    else:
        for i in pytf_paths:
            if os.path.isfile( os.path.join( i, 'cmd_run.sh') ):
                pytf_runner = os.path.join( i, 'cmd_run.sh')
                break;
    #pytf_runner = os.path.join(os.getenv('PYTF_TOP_DIR',''),'cmd_run.sh')    
    pytf_runner = os.path.join(os.getenv('PYTF_TOP_DIR','').split(':')[0],'cmd_run.sh')

    def getstatusouterr(cmd,output_path="%(output_path)s",out_mode='a',setTimeout=False):
        import os
        import os.path
        import time
        from subprocess import PIPE,STDOUT,Popen        
        info = ''
        if not os.path.exists(pytf_runner):
            print '\\n ERROR: Cannot find %%s\\n' %% pytf_runner
        script_runner = [pytf_runner]
        if setTimeout:
            script_runner.extend(['-t','%(timeout)s'])
        else:
           #hard value for timeout (1h)
           script_runner.extend(['-t','3600'])

        try:
            import Ganga.Core.InternalServices.Coordinator
            if Ganga.Core.InternalServices.Coordinator.servicesEnabled:
                from Ganga.Core.InternalServices.Coordinator import disableInternalServices
                disableInternalServices()
        
            from Ganga.Core.InternalServices.Coordinator import enableInternalServices, disableInternalServices
            enableInternalServices()
            reactivate()
        
            from Ganga.Core.GangaRepository import getRegistryProxy
            for j in getRegistryProxy('jobs'): j.remove()
            for t in getRegistryProxy('templates'): t.remove()
            if hasattr(getRegistryProxy('jobs'),'clean'):
                getRegistryProxy('jobs').clean(confirm=True, force=True)
            if hasattr(getRegistryProxy('templates'),'clean'):
                getRegistryProxy('templates').clean(confirm=True, force=True)
        
            disableInternalServices()
        
        #    # DANGEROUS only use as a last resort to files being kept open
        #    #from Ganga.Runtime import bootstrap
        #    #bootstrap.safeCloseOpenFiles()
        #
        except:
            pass

        script_runner.append('{ ' + cmd + '; }')
        output = open(output_path,out_mode)
        output.write('########## Test started: ' + time.ctime() + ' ##########')
        output.flush()
        print "Running: \\n" + str(script_runner) + "\\n"
        process = Popen(script_runner, shell=False, bufsize=0, 
                        stdin=PIPE, stdout=output, stderr=STDOUT, close_fds=True)        
        process.wait()
        output.write('########## Test finished: ' + time.ctime() + ' ##########')
        output.write("\\n")
        output.close()
        sts = process.returncode
        if sts is None:
           sts = 0
        #if timeout
        if sts == 7:
           raise unittest.TestCase.failureException("Test Timeout(%(timeout)s sec). " 
                      "Consider increasing the value of '[TestingFramework]timeout' parameter in your test configuration file")

        text = read_file(output_path)        
        tb_index = text.rfind('Traceback')
        out=text[:tb_index]
        err=text[tb_index:]


        return sts,out,err
        
    #delete the output file if it already exists
    try:
        os.unlink("%(output_path)s")
    except OSError as e:
        pass
    
""" % VARS
        
        code=code+"""
    try:
        print "\\n****** %(description)s"
        print "\\tLog file: %(output_path)s"
        import signal
"""%VARS
        
        if setUpCmd is not None:
            code=code+"""
        import sys
        print "\\tSetup test..."
        (status,out,err)=getstatusouterr('%(setUpCmd)s')
        if status: 
            print "\\tSetup failed: ",out,' ',err
        else: 
            print "\\tSetup done."
"""%VARS
        
        for cmd in testCmd:
            code=code+"""
        (status,out,err)=getstatusouterr('%s',setTimeout=True)
        if status: raise unittest.TestCase.failureException, err;
"""%cmd
        code=code+"""                
    finally:
        #cleanup:
        pass
          
"""
        if tearDownCmd is not None:
            code=code+"""
        import sys
        (status,out,err)=getstatusouterr('%(tearDownCmd)s')
        if status: 
            print "Cleanup failed: ",out,' ',err        
"""%VARS
        else:
            code=code+"""
        pass
"""
        
        
        #print code
        exec(code, globals(), d)
        __method = d[method_name].__get__(self, self.__class__)
        setattr(self, method_name, __method)
        return NamedFunctionTestCase(__method,description=description)

    def __generateTestCaseFromGPITest(self,test_path,ganga_config_path,test_relpath=None):
        """
        test_path: relative test path to self.testsTopDir
        """
        test_filepart = os.path.basename(test_path)
        test_dir = os.path.dirname(test_path)   
        test_filename = os.path.splitext(test_filepart)[0]
        
        # test specific configuration file
        test_ini = os.path.join(self.testsTopDir,test_dir,test_filename+".ini")
        # test setup script 
        test_setup = os.path.join(self.testsTopDir,test_dir,test_filename+".setup")
        # test tear down command
        test_cleanup = os.path.join(self.testsTopDir,test_dir,test_filename+".cleanup")

        if test_relpath is None: #relative to release
            test_relpath = test_path[len(self.testsTopDir)+1:-len(os.path.splitext(test_filepart)[1])]
            
        test_name = "%s (GPI)"%test_relpath
        
        if self.releaseTesting:
            #when doing release testing we generate the coverage analysis reports
            #and generatate a well defined output dir structure
            if self.schema_test is not '':
                output_path=os.path.join(self.outputPrefix,test_relpath.replace("/",".")+"_"+self.schema_test+"_"+self.testID+"_"+ self.configAlias)
            else:
                output_path=os.path.join(self.outputPrefix,test_relpath.replace("/",".")+"__"+ self.configAlias)
            coverage_path = "%s.figleaf" % output_path
        else:
            output_path=os.path.join(self.outputPrefix,test_filename)
            if self.configAlias:
                output_path+="__%s" % self.configAlias
            coverage_path = ""
         
        config = ganga_config_path
        if os.path.isfile(test_ini):
            config = "%s:%s"%(test_ini,config)
    
        #read the timeout for this testcase            
        timeout = readTestTimeout(config)
        
        def opt_path(coverage_path):
            if coverage_path:
                return coverage_path
            return 'None'
        runPyFile = os.path.join(self.releaseTopDir,"python","GangaTest","Framework","driver.py")
        from Ganga.Utility.Config import getConfig
        if self.schema_test is not '':
            newgangadir = os.path.join(getConfig('Configuration')['gangadir'],self.schema_test)
            testCmd ="cd %s ; env --unset=GANGA_INTERNAL_PROCREEXEC OUTPUT_PATH=%s PYTHONUNBUFFERED=1 "\
                    "%s/bin/ganga -o[Configuration]gangadir=%s -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s "\
                    "%s --test-type=gpi --coverage-report=%s %s"%(
                os.path.join(self.testsTopDir,test_dir),
                fullpath(output_path),
                self.releaseTopDir,            
                newgangadir,
                config, runPyFile,
                opt_path(coverage_path),
                test_path)        
        else:
            testCmd ="cd %s ; env --unset=GANGA_INTERNAL_PROCREEXEC OUTPUT_PATH=%s PYTHONUNBUFFERED=1 "\
                    "%s/bin/ganga -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s "\
                    "%s --test-type=gpi --coverage-report=%s %s"%(
                os.path.join(self.testsTopDir,test_dir),
                fullpath(output_path),
                self.releaseTopDir,            
                config, runPyFile,
                opt_path(coverage_path),
                test_path)        


        # setup Ganga-Session
        if os.path.isfile(test_setup):
            setUpCmd = "cd %s ; PYTHONUNBUFFERED=1  python %s"%(
                os.path.join(self.testsTopDir,test_dir),                
                test_setup)
        else:
            setUpCmd = None

        # tearDown Ganga Session
        if os.path.isfile(test_cleanup):
            tearDownCmd = "cd %s ; PYTHONUNBUFFERED=1 python %s"%(
                os.path.join(self.testsTopDir,test_dir),                
                test_cleanup)
        else: 
            tearDownCmd = None
            #default cleanup code is located in GangaTest/Framework/cleanup.py and is used by default when running GPI scripts            
            #cleanUpPyFile = os.path.join(self.releaseTopDir,"python","GangaTest","Framework","cleanup.py")
            #tearDownCmd = "cd %s ; env --unset=GANGA_INTERNAL_PROCREEXEC OUTPUT_PATH=%s PYTHONUNBUFFERED=1 "\
            #              "%s/bin/ganga -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s --quiet %s"%(
            #    os.path.join(self.testsTopDir,test_dir),
            #    fullpath(output_path),                
            #    self.releaseTopDir,
            #    config,
            #    cleanUpPyFile)
        try:
            descr = os.path.join(test_dir[test_dir.index(self.testsTopDir)+len(self.testsTopDir)+1:],test_filename)+" [GPI]"
        except ValueError:
            descr = os.path.join(test_dir,test_filename)+" [GPI]"

        return self.__generateTestCaseFromCmd(
            [testCmd], 
            test_filename,
            testAlias = test_name,
            setUpCmd=setUpCmd,
            tearDownCmd=tearDownCmd,
            description=descr,
            output_path=output_path+'.out',
            timeout=timeout
            ) 
        
    def __generateTestCaseFromGMPTest(self, test_path,ganga_config_path,test_relpath=None):
        
        test_filepart = os.path.basename(test_path)
        test_dir = os.path.dirname(test_path)   
        test_filename = os.path.splitext(test_filepart)[0]
        test_ini_prefix = os.path.join(self.testsTopDir,test_dir,test_filename)
        test_ini = test_ini_prefix + ".ini"

        if test_relpath is None: #relative to release
            test_relpath = test_path[len(self.testsTopDir)+1:-len(os.path.splitext(test_filepart)[1])]
            
        test_name = "%s (GPIM)"%test_relpath
        
        if self.releaseTesting:
            #when doing release testing we generate the coverage analysis reports
            #and generatate a well defined output dir structure
            output_path=os.path.join(self.outputPrefix,test_relpath.replace("/",".")+"__"+ self.configAlias)
            coverage_path = "%s.figleaf" % output_path
        else:
            output_path=os.path.join(self.outputPrefix,test_filename)
            if self.configAlias:
                output_path+="__%s" % self.configAlias
            coverage_path = ""
 
        if os.path.isfile(test_ini):
            test_ini = "%s:%s"%(test_ini,ganga_config_path)
        else:
            test_ini = ganga_config_path

        #read the timeout for this testcase
        timeout = readTestTimeout(test_ini)
        
        #passes may have specific configuration files (e.g. MyTest.pass1.ini)
        test_ini = '%s:'+ test_ini

        #testCmdPrefix = "cd %s ; PYTHONUNBUFFERED=1 PYTHONPATH=%s %s/bin/ganga -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s"%(
        testCmdPrefix = "cd %s ; env PYTHONUNBUFFERED=1 OUTPUT_PATH=%s "\
                      "%s/bin/ganga -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s" % \
                        (os.path.join(self.testsTopDir,test_dir), 
                         fullpath(output_path),
                         self.releaseTopDir,
                         test_ini)
        try:
            module = exec_module_code(test_path)
        except Exception as e:
            print "[WARNING] Cannot parse Multipass test (invalid format) %s [skipped]-> %s"%(test_path,e)
            return 
            
        testCmds = []
        runPyFile = os.path.join(self.releaseTopDir,"python","GangaTest","Framework","driver.py")
        for name in dir(module):

            clazz = getattr(module, name)
            clazz_dict = dir(clazz)
            sclazz=str(clazz)
            if isinstance(clazz, (type, types.ClassType)) and issubclass(clazz, MultipassTest) and sclazz.find(test_filename)>=0:
                if "run" not in clazz_dict:
                    print "Cannot find <run> method in the MultipassTest class"
                    break
                #for each multipass tests: build n test-cases
                for attrname in clazz_dict:                   
                    # if is "pass%i" method
                    if attrname.startswith("pass"):
                        pass_ini = '%s.%s.ini' % (test_ini_prefix,attrname)
                        if not os.path.isfile(pass_ini):
                            pass_ini = ''
                        targs = 'run'
                        if attrname=="pass1":
                            targs = 'run --multipass-restart setUp'                     
                        testCmds.append( "%s %s %s --test-type=gpim --coverage-report=%s %s" % \
                                         (testCmdPrefix % pass_ini,
                                          runPyFile,
                                          test_path,
                                          coverage_path,
                                          targs))
                if "tearDown" in clazz_dict and len(testCmds)>0:
                    testCmds[-1] += ' tearDown'
                break
        try:
            descr = os.path.join(test_dir[test_dir.index(self.testsTopDir)+len(self.testsTopDir)+1:],test_filename)+" [GPIM]"
        except ValueError:
            descr = os.path.join(test_dir,test_filename)+" [GPIM]"
        if len(testCmds)>0:
            return self.__generateTestCaseFromCmd(
                testCmds,
                test_filename,
                testAlias = test_name,
                description=descr,
                output_path=output_path+'.out',
                timeout=timeout)
        
    def __generateTestCasesFromPyUnitTest(self,test_path,ganga_config_path,test=None,test_relpath=None):
        
        test_filepart = os.path.basename(test_path)
        test_dir = os.path.dirname(test_path)
        test_filename = os.path.splitext(test_filepart)[0]
        
        if test_relpath is None: #relative to release
            test_relpath = test_path[len(self.testsTopDir)+1:-len(os.path.splitext(test_filepart)[1])]
            
        test_name = "%s"%test_relpath
        
        if self.releaseTesting:
            #when doing release testing we generate the coverage analysis reports
            #and generatate a well defined output dir structure
            output_path=os.path.join(self.outputPrefix,test_relpath.replace("/",".")+"."+"%s"+"__"+self.configAlias+".out")
            coverage_path = "%s.figleaf" % os.path.splitext(output_path)[0]
        else:
            output_path=os.path.join(self.outputPrefix,test_filename+"."+"%s"+".out")            
            if self.configAlias:
                output_path+="__%s" % self.configAlias
            coverage_path = ""

        def getCoveragePath(template,value):
            if template:
                return template%value
            return ''
            
        #print "Checking PyUnit tests from: %s"%test_name
        test_ini = os.path.join(self.testsTopDir,test_dir,test_filename+".ini")

        #set the ganga configuration path
        if os.path.isfile(test_ini):
            test_ini = "%s:%s"%(test_ini,ganga_config_path)
        else:
            test_ini = ganga_config_path

        #read the timeout for this testcase
        timeout = readTestTimeout(test_ini)
        
        try:
            module = exec_module_code(test_path)
            if hasattr(module,'setUp'):
                getattr(module,'setUp')()
        except Exception as e:
            print "[WARNING] Cannot parse PYUnit test  %s [skipped]:"%test_path
            import traceback
            traceback.print_exc()
            return []
            
        tests = []
        runPyFile = os.path.join(self.releaseTopDir,"python","GangaTest","Framework","driver.py")
        for name in dir(module):

            try:
                clazz = getattr(module, name)
                sclazz = str(clazz)
            #print("name: %s" % str(name))
            #print("sclazz: %s" % str(sclazz))
                if isinstance(clazz, (type, types.ClassType)) and  sclazz.split('.')[-1].find(test_filename)>=0:
                    clazz_dict = dir(clazz)
                    for attrname in clazz_dict:
                        try:
                            descr = os.path.join(test_dir[test_dir.index(self.testsTopDir)+len(self.testsTopDir)+1:],test_filename,attrname)+" [PY]"
                        except ValueError:
                            descr = os.path.join(test_dir,test_filename,attrname)+" [PY]"
                        if test is not None and attrname==test:
                            testCmd = "cd %s ; env OUTPUT_PATH=%s "\
                                "%s/bin/ganga -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s %s %s --test-type=py --coverage-report=%s %s" % (
                            os.path.join(self.testsTopDir,test_dir),
                            fullpath(output_path%attrname),                            
                            self.releaseTopDir,
                            test_ini, runPyFile,
                            test_path, 
                            getCoveragePath(coverage_path,attrname),
                            attrname)

                            testcase = self.__generateTestCaseFromCmd(
                            [testCmd],
                            attrname,
                            testAlias="%s.%s (PY)"%(test_name,attrname),
                            description=descr,
                            output_path=output_path%attrname,
                            timeout=timeout
                            )
                            if testcase is not None: return [testcase]
                        elif attrname.startswith("test") and test is None:
                            #syspath = [p for p in sys.path if p.find("/usr/lib")==-1]                        
                            testCmd = "cd %s ;env OUTPUT_PATH=%s "\
                                "%s/bin/ganga -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s %s %s --test-type=py --coverage-report=%s %s setUp tearDown"%(
                            os.path.join(self.testsTopDir,test_dir),
                            fullpath(output_path%attrname),
                            self.releaseTopDir,
                            test_ini, 
                            runPyFile,
                            test_path,
                            getCoveragePath(coverage_path,attrname),
                            attrname)                        
                            testcase = self.__generateTestCaseFromCmd(
                            [testCmd],
                            attrname,
                            testAlias="%s.%s (PY)"%(test_name,attrname),
                            description=descr,
                            output_path=output_path%attrname,
                            timeout=timeout
                            )
                            if testcase is not None: tests.append(testcase)
            except:
                pass

                break
        return tests

    def __generateTestCasesFromGPIPTest(self,test_path,ganga_config_path,test=None,test_relpath=None):
        
        test_filepart = os.path.basename(test_path)
        test_dir = os.path.dirname(test_path)
        test_filename = os.path.splitext(test_filepart)[0]
        
        if test_relpath is None: #relative to release
            test_relpath = test_path[len(self.testsTopDir)+1:-len(os.path.splitext(test_filepart)[1])]
            
        test_name = "%s"%test_relpath
        
        if self.releaseTesting:
            #when doing release testing we generate the coverage analysis reports
            #and generatate a well defined output dir structure
            output_path=os.path.join(self.outputPrefix,test_relpath.replace("/",".")+"."+"%s"+"__"+self.configAlias+".out")
            coverage_path = "%s.figleaf" % os.path.splitext(output_path)[0]
        else:
            output_path=os.path.join(self.outputPrefix,test_filename+"."+"%s"+".out")            
            if self.configAlias:
                output_path+="__%s" % self.configAlias
            coverage_path = ""

        def getCoveragePath(template,value):
            if template:
                return template%value
            return ''
            
        #print "Checking PyUnit tests from: %s"%test_name
        test_ini = os.path.join(self.testsTopDir,test_dir,test_filename+".ini")

        #set the ganga configuration path
        if os.path.isfile(test_ini):
            test_ini = "%s:%s"%(test_ini,ganga_config_path)
        else:
            test_ini = ganga_config_path

        #read the timeout for this testcase
        timeout = readTestTimeout(test_ini)
        
        try:
            module = exec_module_code(test_path)
            if hasattr(module,'setUp'):
                getattr(module,'setUp')()
        except Exception as e:
            print "[WARNING] Cannot parse GPIP test  %s [skipped]:"%test_path
            import traceback
            traceback.print_exc()
            return []
            
        tests = []
        runPyFile = os.path.join(self.releaseTopDir,"python","GangaTest","Framework","driver.py")
        for name in dir(module):
            clazz = getattr(module, name)
            sclazz = str(clazz)
            #print sclazz
            #print '-->%s, -->%s' % (sclazz.split('.')[-1].split("'")[0], test_filename)
            if isinstance(clazz, (type, types.ClassType)) and  sclazz.split('.')[-1].split("'")[0] == test_filename:
                #print '[%s]' % test
                if test is not None and attrname==test:
                    print 'This line should be printed out.'
                else:
                    try:
                        descr = os.path.join(test_dir[test_dir.index(self.testsTopDir)+len(self.testsTopDir)+1:],test_filename,'ALL')+" [GPIP]"
                    except ValueError:
                        descr = os.path.join(test_dir,test_filename,'ALL')+" [GPIP]"
                    testCmd = "cd %s ;env OUTPUT_PATH=%s "\
                              "%s/bin/ganga -o[Configuration]RUNTIME_PATH=GangaTest --config= --config-path=%s %s %s --test-type=gpip --output_base=%s --coverage-report=%s --timeout=%s %s setUp tearDown"%(
                              os.path.join(self.testsTopDir,test_dir),
                              fullpath(output_path),
                              self.releaseTopDir,
                              test_ini,
                              runPyFile,
                              test_path,
                              output_path,
                              getCoveragePath(coverage_path,'ALL'),
                              timeout,
                              'run_pyunit_testcases')

                    #print '%s *********' % descr
                    testcase = GangaGPIPTestCase(testCmd, descr,output_path,self.report_outputpath,self.current_report_name,self.releaseTesting)
                    if testcase is not None: tests.append(testcase)
        #print tests
        return tests

## Utility functions ##

def loadArg(args,ind,default=None):
    try:
        if args[ind] == '':
            return default
        else:
            return args[ind]
    except IndexError:
        return default
    
def readTestTimeout(config):
    """get the timeout value from the list of test configuration files (colon separated list)"""
   
    import ConfigParser
    parser = ConfigParser.ConfigParser()
    timeout=DEFAULT_TEST_TIMEOUT
    for file in config.split(':'):
        try:
            parser.read(file)
            timeout = parser.get("TestingFramework","timeout")
            break
        except:
            #not found or error
            continue
    return timeout          

def get_ext(filename):
    return os.path.splitext(os.path.split(filename)[1])[1]

def get_name(filename):
    return os.path.splitext(os.path.split(filename)[1])[0]

def dirwalk(dir):
    "walk a directory tree, using a generator"
    for f in os.listdir(dir):
        fullpath = os.path.join(dir,f)
        if os.path.isdir(fullpath) and not os.path.islink(fullpath):
            for x in dirwalk(fullpath):  # recurse into subdir
                yield x
        else:
            yield fullpath
            
def walk(top, topdown=True, onerror=None):
    """
    os.path.walk function 
    """    
    from os.path import join, isdir, islink
    from os import listdir
    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.path.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    try:
        names = listdir(top)
    except Exception as err:
        print err
        if onerror is not None:
            onerror(err)
        return

    dirs, nondirs = [], []
    for name in names:
        if isdir(join(top, name)):
            dirs.append(name)
        else:
            nondirs.append(name)

    if topdown:
        yield top, dirs, nondirs
    for name in dirs:
        path = join(top, name)

        if not islink(path):
            for x in walk(path, topdown, onerror):
                yield x
    if not topdown:
        yield top, dirs, nondirs
        
def expandfilename(filename):
    "expand a path or filename in a standard way so that it may contain ~ and ${VAR} strings"
    return os.path.expandvars(os.path.expanduser(filename))

def fullpath(path):
    "expandfilename() and additionally: strip leading and trailing whitespaces and expand symbolic links"
    return os.path.realpath(expandfilename(path.strip()))

def _log(logger,level,msg):
    if logger and hasattr(logger,level):
        getattr(logger,level)(msg)
    else:
        print "[%s] %s" % (level.upper(),msg)

#$Log: not supported by cvs2svn $
#Revision 1.1  2008/07/17 16:41:36  moscicki
#migration of 5.0.2 to HEAD
#
#the doc and release/tools have been taken from HEAD
#
#Revision 1.43.4.6  2008/02/12 14:08:02  amuraru
#*** empty log message ***
#
#Revision 1.43.4.5  2007/12/18 13:12:48  amuraru
#stream-line the test-case execution code (use a single entry-point : Framework/driver.py)
#symplified the interface with Ganga bootstrap
#
#Revision 1.43.4.4  2007/12/17 15:51:29  amuraru
#use subprocess in the Test Runner
#
#Revision 1.43.4.3  2007/12/07 17:30:33  amuraru
#- use the timeout functionality from cmd_run shell wrapper
#
#Revision 1.43.4.2  2007/11/21 12:18:58  amuraru
#fixed the loader imports to work on python 2.5
#
#Revision 1.43.4.1  2007/10/31 13:37:21  amuraru
#updated to the new config subsystem in Ganga 5.0
#
#Revision 1.43  2007/09/13 15:18:50  amuraru
#fixed the local configuration
#
#Revision 1.42  2007/09/11 16:17:06  amuraru
#*** empty log message ***
#
#Revision 1.41  2007/09/11 15:44:59  amuraru
#-delete the test output file if it already exists
#
#Revision 1.40  2007/09/04 09:48:13  amuraru
#restructed code to allow two modes of running: developer mode and release mode
#
#Revision 1.39  2007/08/21 14:38:44  amuraru
#*** empty log message ***
#
#Revision 1.38  2007/08/21 14:30:22  amuraru
#code cleanup
#
#Revision 1.37  2007/08/13 17:20:37  amuraru
#- fix bug #28430
#- do not generate coverage analysis reports for local tests
#- cleanup
#
#Revision 1.36  2007/08/13 12:33:15  amuraru
#added verbosity control, avoid purging the used reporisotry when running local tests
#
#Revision 1.35  2007/06/06 14:27:25  amuraru
#
#minor fixes
#
#Revision 1.34  2007/06/05 23:17:08  amuraru
#*** empty log message ***
#
#Revision 1.33  2007/06/04 13:24:11  amuraru
#*** empty log message ***
#
#Revision 1.32  2007/06/01 09:00:36  amuraru
#small fix in GPIM loading
#
#Revision 1.31  2007/05/31 11:40:03  amuraru
#- updated multi-pass tests loader to read pass specific configuration files (if exist)
# i.e: MyMultipass.pass<no>.ini are searched in the test driectory and added in ganga config-path
#
#Revision 1.30  2007/05/21 16:01:20  amuraru
#use default website css style in test/coverage reports;
#disabled per test-case coverage report generation;
#other fixes
#
#Revision 1.29  2007/05/16 10:15:52  amuraru
#use ganga logger
#
#Revision 1.28  2007/05/15 09:58:36  amuraru
#html reporter updated
#
