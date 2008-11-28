################################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# $Id: driver.py,v 1.2 2008-11-26 08:30:35 moscicki Exp $
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

"""
Test driver used to executed GPI, GPIM and PyUnit tests 
This file is executed as a Ganga script so we can access to all GPI objects
"""

import os,os.path,sys,new,types

from Ganga.Utility.Config import getConfig
myconfig = getConfig('TestingFramework')
from Ganga.Utility.logging import getLogger
#logger
logger=getLogger('GangaTest.Framework')

class GPIRunner:
        """
        load/setup/run/tearDown encapsulation for a GPI test
        Note: These operations are executed in the same ganga process
        """
        def __init__(self, testPath, testName):
                self.testPath = testPath
                self.testName = testName
        
        def load(self):
                """
                        NOP: the testPath is sufficient for this type of tests
                """
                return True
        
        def setup(self):
                """
                Apart from .startup script which is executed in a different Ganga session
                optionally the job registry can be cleaned up.
                """
                #auto cleanup of job registry as a pre-step of the initialization (optionally)
                cleanup()
        
        def run(self): 
                """
                Execute file in a Ganga session
                """
                import os.path
                testFile = os.path.split(testPath)[1]           
                execfile( testFile , Ganga.Runtime._prog.local_ns )
                
        def tearDown(self):
                """
                NOP: .cleanup file is ran in a different session
                """
                pass
        
                
class UnitRunner:
        """
        load/setup/run/tearDown pipe-line for a GPIM/PY tests
        """
        
        def __init__(self, testPath, testName):
                self.testPath = testPath
                self.testName = testName
        
        def load(self):
                self.__instance = None
                try:                    
                        exec_module_code(self.testPath,Ganga.Runtime._prog.local_ns)
                        testClass=os.path.splitext(os.path.basename(self.testPath))[0]
                        clazz = Ganga.Runtime._prog.local_ns[testClass]# getattr(Ganga.Runtime._prog.local_ns.__dict__, testClass)                  
                        self.__instance = clazz()
                except Exception,e:
                        print >>sys.stderr, "Cannot load test"
                        import traceback
                        traceback.print_exc(file=sys.stderr)
                        self.__instance=None
                        return False
                return True
        
        def setup(self):
                if hasattr(self.__instance,"setUp") and "setUp" in sys.argv:
                        #auto cleanup of job registry as a pre-step of the initialization
                        cleanup()
                        getattr(self.__instance,"setUp")()
                        
        def run(self):
                getattr(self.__instance,self.testName)()

        def tearDown(self):
                try:
                        if hasattr(self.__instance,"tearDown") and "tearDown" in sys.argv:
                                getattr(self.__instance,"tearDown")()
                except Exception,e:
                        print >>sys.stderr, "Cannot tear down gracefully the test (%s)" % str(e)
                        import traceback
                        traceback.print_exc()

# Use the modules from testoob for GPIPRunner.
sys.path.append(os.path.join(os.getenv('PYTF_TOP_DIR','').split(':')[0],'pytf'))
from testoob.extracting import full_extractor as _full_extractor
from testoob.running.convenience import _create_reporter_proxy
from testoob.running.simplerunner import SimpleRunner
from testoob.running.convenience import apply_decorators
from testoob.reporting.colored import ColoredTextReporter
from testoob.reporting import XMLFileReporter

import unittest
from GangaTest.Framework.tests import GPIPPreparationTestCase, SimpleRunnerControl

class GPIPRunner:
        def __init__(self, testPath, outputPath,timeout,description,releaseTest,report_outputpath,parent_report_name):
                #print '%s %s %s %s %s %s %s' % (testPath, outputPath,timeout,description,releaseTest,report_outputpath,parent_report_name)
                self.testPath = testPath
                #self.testName = testName
                self.outputPath = outputPath
                self.timeout = timeout
                self.description = description # description template - 'GangaDummy/test/Root/TestRoot/%s [GPIP]'
                self.releaseTest = releaseTest # is release test mode enable 
                self.report_outputpath = report_outputpath # release report directory 
                self.parent_report_name = parent_report_name # indicate the parent report name(suite name)
                self.pytest_name = os.path.basename(self.outputPath.split('%s')[0].strip('.'))
                self._testinstances = []
                self._xml_report_ext = '%s.xml_merged'
                #print 'Path : %s, NAME : %s, REPORT : %s' %(testPath, 'ALL', self.parent_report_name)

        def load(self):
                self.__instance = None
                try:
                        exec_module_code(self.testPath,Ganga.Runtime._prog.local_ns)
                        testClass=os.path.splitext(os.path.basename(self.testPath))[0]
                        clazz = Ganga.Runtime._prog.local_ns[testClass]# getattr(Ganga.Runtime._prog.local_ns.__dict__, testClass) 
                        #print clazz
                        self.clazz = clazz
                        self.__instance = clazz()
                        self.testsuite = self._getTestSuite()
                        #print self.__instance
                except Exception,e:
                        print >>sys.stderr, "Cannot load test"
                        import traceback
                        traceback.print_exc(file=sys.stderr)
                        self.__instance=None
                        return False
                return True

        def setup(self):
                if hasattr(self.__instance,"setUp") and "setUp" in sys.argv:
                        #auto cleanup of job registry as a pre-step of the initialization
                        cleanup()
                        for instance in self._testinstances:
                            getattr(instance,"setUp")()

        def run(self):
                """
                Execute file in a Ganga session
                """
                import time

                from testoob.running.fixture_decorators import BaseFixture
                fixture_decorators = [BaseFixture]

                test_extractor = apply_decorators(_full_extractor, [])

                runner_threads = []
                runners_control = []
                
                start = time.time()

                # self.testsuite must be initialized in load()
                for fixture in test_extractor(self.testsuite):
                        testName = fixture.method_name
                        decorated_fixture = apply_decorators(fixture, fixture_decorators)
                        runner = self._generate_runner(testName)
                        runner_control = SimpleRunnerControl(testName, runner, decorated_fixture, fixture_decorators, self.timeout)
                        if runner_control is not None: runners_control.append(runner_control)

                for run_control in runners_control:
                    run_status = run_control.runPreparationTestCase()
                    if not run_status:
                        run_control.setFinished(True) # Fail occurred while preparing the job, stop the test and write out the report.

                all_done = False

                #start = time.time()

                duration = start - time.time()

                while not all_done and not (duration > self.timeout):
                    all_done = True
                    #print 'duration : %d, timeout : %d' % (duration, self.timeout)
                    for run_control in runners_control:
                        if not run_control.isReadyForCheck() and not run_control.isFinished():
                            all_done = False
                    if not all_done:
                        end = time.time()
                        duration = end - start
                    time.sleep(1)

                if duration > self.timeout:
                    for run_control in runners_control:
                        if not run_control.isReadyForCheck():
                            run_control.setTimeout(True) # Mark the jobs which are still running with timeout 'True' flag.

                for run_control in runners_control:
                    if not run_control.isFinished():
                        run_control.runCheckTest()


        def _generate_runner(self, testName):
            from testoob.running.simplerunner import SimpleRunner
            runner = SimpleRunner()

            verbosity = 1
            immediate = False
            coverage = (None, None)
            
            reporters = []

            if self.releaseTest:
                runner.xml_filename = self._xml_report_ext % os.path.join(self.report_outputpath, '%s_%s_%s' % (self.parent_report_name,self.pytest_name,testName))
                xml_reporter = XMLFileReporter(filename=runner.xml_filename)
                reporters.append(xml_reporter)
            else:
                runner.xml_filename = None

            
            coloredReporter = ColoredTextReporter(
                              verbosity=verbosity,
                              immediate=immediate,
                              descriptions=1,
                              stream=sys.stderr)

            reporters.append(coloredReporter)
            

            runner.reporter = _create_reporter_proxy(reporters, None, threads=True)

            return runner


        def _getTestSuite(self):
                attrs = dir(self.clazz)
                testcase = None
                #print attrs
                testcases = []
                for test in attrs:
                        stest = str(test)
                        if stest.startswith('test'):
                                testcase = GPIPPreparationTestCase(self._get_tester_instance(), self.description % stest, stest, self.timeout, self.outputPath % stest)
                                if testcase is not None: testcases.append(testcase)
                return unittest.TestSuite(testcases)

        def _get_tester_instance(self):
            instance = self.clazz()
            self._testinstances.append(instance)
            return instance

        def tearDown(self):
                try:
                        if hasattr(self.__instance,"tearDown") and "tearDown" in sys.argv:
                            for instance in self._testinstances:
                                getattr(instance,"tearDown")()
                except Exception,e:
                        print >>sys.stderr, "Cannot tear down gracefully the test (%s)" % str(e)
                        import traceback
                        traceback.print_exc()

#utils 
def cleanup():    
    if myconfig['AutoCleanup'] == True:
        logger.info("[TestingFramework]AutoCleanup=True")
        logger.info("cleaning up job repository...")
        # explicit removal of jobs is not required but makes one test more
        try:
            for id in jobs.ids():
                jobs(id).remove()
            jobs.remove()

            for id in templates.ids():
                templates(id).remove()
            templates.remove()
            logger.info("regular cleanup done.")
        except Exception,e:
            logger.exception("Cleanup failed (%s)... will delete repository and workspace dirs manually "%str(e))

        logger.info('performing "hard" cleanup')

        if hasattr(jobs._impl,'resetAll'):
                jobs._impl.resetAll()
        if hasattr(templates._impl,'resetAll'):
                templates._impl.resetAll()

def read_file(filename):
        f = open(filename)
        try: return "%s\n"%f.read()
        finally: f.close()

def exec_module_code(path,ns=None ):
        """
            loads the code from path and execute it in the given namespace
            If ns is None the code is executed in the newly created module namespace
        """
        (testdir,filename) = os.path.split(path)    
        cwd = os.getcwd()
        if testdir:
                try:
                        os.chdir(testdir)
                except:
                        pass

        name     = os.path.splitext(filename)[0]
        code_str = read_file(filename)
        code     = compile(code_str,filename,'exec')
        (name,ext) = os.path.splitext(filename)
        if sys.modules.has_key(name):
                mod = sys.modules[name] # necessary for reload()
        else:
                mod = new.module(name)
                sys.modules[name] = mod
        mod.__name__ = name
        mod.__file__ = filename
        if ns is None:
                exec code in mod.__dict__
        else:
                exec code in ns
        
        #restore cwd
        os.chdir(cwd)       
        return mod

def parse_args(sys_argv):    
        opts = {}
        params = []    
        for arg in sys_argv:
                if arg.startswith("--"):            
                        list = arg.split('=')
                        opt = list[0]
                        try:
                                val = list[1]
                        except IndexError:
                                val = None                          
                        opts[opt]=val
                else:
                        params.append(arg)
        return params,opts


if __name__=="__main__":
        
        if len(sys.argv)<4:
                print >> sys.stderr, "Invalid usage of program. At least three parameters are required"
                sys.exit(1)     
        
        param,opt = parse_args(sys.argv[1:])    
        testPath=param[0]
        try:
                testName=param[1]
        except IndexError:
                testName = None
        coverage_report = opt.get("--coverage-report", None) 
        #test type: GPI, GPIM or PY    
        test_type = opt.get("--test-type", "gpi")
        if test_type == 'gpi':
                testRunner = GPIRunner(testPath,testName)
        elif test_type == 'gpip':
                #testRunner = UnitRunner(testPath,testName)
                testRunner = GPIPRunner(testPath,opt.get('--output-path',None),int(opt.get('--timeout',120)),opt.get('--description'),opt.get('--release-test',False),opt.get('--report-outputpath',None),opt.get('--parent-report-name',None))
        elif test_type in ['py','gpim']:
                testRunner = UnitRunner(testPath,testName) 
        
        #1. LOAD
        if not testRunner.load():
                sys.exit(1)
        success = True
        try:
                #2. SETUP
                testRunner.setup()      
                if coverage_report and coverage_report!='None':
                        import figleaf
                        figleaf.start()
                #3. RUN
                testRunner.run()
                if coverage_report and coverage_report!='None':
                        try:
                                figleaf.stop()
                                figleaf.write_coverage(coverage_report)
                        except NameError:
                                pass
                #4. TEAR-DOWN
                testRunner.tearDown()   
        except Exception,e:
                import traceback
                traceback.print_exc(file=sys.stderr)
                success = False
                if coverage_report and coverage_report!='None':
                        try:
                                figleaf.stop()
                                figleaf.write_coverage(coverage_report)
                        except NameError:
                                pass
        sys.exit(not success)

#$Log: not supported by cvs2svn $
#Revision 1.1  2008/07/17 16:41:35  moscicki
#migration of 5.0.2 to HEAD
#
#the doc and release/tools have been taken from HEAD
#
#Revision 1.1.2.7  2008/04/18 13:50:22  moscicki
#resetAll() method wipes all the repository
#
#Revision 1.1.2.6  2008/04/04 16:03:23  moscicki
#fixed import in cleanup (Will)
#
#Revision 1.1.2.5  2008/03/13 15:53:32  amuraru
#forcibly remove repos and wspace dir in cleanup code
#
#Revision 1.1.2.4  2008/02/29 15:33:40  amuraru
#JobRegistry fix
#
#Revision 1.1.2.3  2007/12/19 17:37:03  amuraru
#removed the cleanup file and moved the cleanup code in test driver
#fixed the namespace problems when executing test-caseswq
#
