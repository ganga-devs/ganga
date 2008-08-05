from GangaTest.Framework.tests import GangaGPITestCase

#only define backends if not already defined.
# done so that Altas and LHCb can run the tests
# using other backends
loc = locals()
if not loc.has_key('backends_to_test'):
    loc['backends_to_test'] = ['Interactive','Local','LCG','LSF']#backends to test

class TestRoot(GangaGPITestCase):
    '''
    This is the Ganga testcase used by the testing framework
    The test methods are dynamically added to TestRoot (emulate an unittest test suite). 
    At the begining it is just an empty class
    '''
    
class _TestRootBase(object):
    def __init__(self,backend):
        self.__backend = backend
        self.cputime = 10 #(minutes) for remote jobs

    def _makeFileName(self,usepython):
        fName = 'test.%s'
        ext = 'C'
        if usepython:
            ext = 'py'
        fName = fName % ext
        return fName
    
    def _makeRootApp(self,scriptString,args=[],usepython=False, file_name = None):
        '''The script will be named test.{C,py} and so you should name the function test'''
        import os
        from tempfile import mktemp
        
        if not file_name:
            file_name = self._makeFileName(usepython)
        
        #write string to tmpfile
        tmpdir = mktemp()
        os.mkdir(tmpdir)
        fileName = os.path.join(tmpdir,file_name)
        
        from GangaTest.Framework.utils import write_file
        write_file(fileName,scriptString)
        
        #make a Root application object
        r = Root()
        r.script = fileName
        r.args = args
        
        return r
    
    def _submitJob(self,app,IJobTest=None):
        """Used to construct, configure and test a job."""
        if IJobTest == None:
            IJobTest = _DefaultJobTest(self)
        print 'Backend:',self.__backend
        j = Job(application=app,backend=self.__backend)
        #configure the job
        IJobTest.configJob(j)
        #submit the jobs
        j.submit()
        #look at the output
        IJobTest.jobTest(j)
    
    def testCintArgs(self):
        '''Runs a basic Cint job and tests that int args are handled.'''
        print 'testCintArgs'
        n = 123
        s = 'foo'
        r = self._makeRootApp("""
                #include <fstream>
                #include <iostream>
                using std::ofstream;
                using std::endl;
                void test(int i, char* str){
                    ofstream file("output.txt");
                    if(file){
                        file << "###" << i << "###" << endl;
                        file << "@@@" << str << "@@@" << endl;
                        file.close();
                    }
                }
        """, [n,s], usepython = False)
        
        assert(not r.usepython)
        
        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                from GangaTest.Framework.utils import file_contains
                from os.path import join,exists
                output = join(j.outputdir,'output.txt')
                assert os.path.exists(output), 'File must have been written'
                assert(file_contains(output,'###%d###' % n))
                assert(file_contains(output,'@@@%s@@@' % s))
                assert(j.status != 'failed')
            def configJob(self,j):
                super(JobTest,self).configJob(j)
                j.outputsandbox = ['output.txt']

        self._submitJob(r,JobTest(self))
        
    def testCintArgsAllStrings(self):
        '''Runs a basic Cint job and tests that string args are handled.'''
        print 'testCintArgs'
        
        args = ['foo'*i for i in xrange(5)]
        assert len(args) == 5
        r = self._makeRootApp("""
                #include <fstream>
                #include <iostream>
                using std::ofstream;
                using std::endl;
                void test(char* s1, char* s2, char* s3, char* s4, char* s5){
                    ofstream file("output.txt");
                    if(file){
                        file << "<s1>" << s1 << "</s1>" << endl;
                        file << "<s2>" << s2 << "</s2>" << endl;
                        file << "<s3>" << s3 << "</s3>" << endl;
                        file << "<s4>" << s4 << "</s4>" << endl;
                        file << "<s5>" << s5 << "</s5>" << endl;
                        file.close();
                    }
                }
        """, args, usepython = False)
        
        assert(not r.usepython)
        
        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                from GangaTest.Framework.utils import file_contains
                from os.path import join,exists
                output = join(j.outputdir,'output.txt')
                assert os.path.exists(output), 'File must have been written'
                for i in xrange(len(args)):
                    assert(file_contains(output,'<s%d>%s</s%d>' % (i+1,args[i],i+1)))
                assert(j.status != 'failed')
            def configJob(self,j):
                super(JobTest,self).configJob(j)
                j.outputsandbox = ['output.txt']

        self._submitJob(r,JobTest(self))
    
    def testCintArgsAllInts(self):
        '''Runs a basic Cint job and tests that int args are handled.'''
        print 'testCintArgs'
        
        args = [i*i for i in xrange(5)]
        assert len(args) == 5
        r = self._makeRootApp("""
                #include <fstream>
                #include <iostream>
                using std::ofstream;
                using std::endl;
                void test(int s1, int s2, int s3, int s4, int s5){
                    ofstream file("output.txt");
                    if(file){
                        file << "<s1>" << s1 << "</s1>" << endl;
                        file << "<s2>" << s2 << "</s2>" << endl;
                        file << "<s3>" << s3 << "</s3>" << endl;
                        file << "<s4>" << s4 << "</s4>" << endl;
                        file << "<s5>" << s5 << "</s5>" << endl;
                        file.close();
                    }
                }
        """, args, usepython = False)
        
        assert(not r.usepython)
        
        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                from GangaTest.Framework.utils import file_contains
                from os.path import join,exists
                output = join(j.outputdir,'output.txt')
                assert os.path.exists(output), 'File must have been written'
                for i in xrange(len(args)):
                    assert(file_contains(output,'<s%d>%d</s%d>' % (i+1,args[i],i+1)))
                assert(j.status != 'failed')
            def configJob(self,j):
                super(JobTest,self).configJob(j)
                j.outputsandbox = ['output.txt']

        self._submitJob(r,JobTest(self))
    
    def testNoArgJob(self):
        """Runs the default cint script. Should produce no errors"""
        print 'testNoArgJob'
        r = Root()
        assert(not r.usepython)

        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                assert(j.status != 'failed')
                
        self._submitJob(r,JobTest(self))
        
    def testNoArgPyRootJob(self):
        """Runs the default PyRoot script. Should produce no errors"""
        print 'testNoArgPyRootJob'
        r = Root()
        r.usepython = True

        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                assert(j.status != 'failed')
                
        self._submitJob(r,JobTest(self))        
     
    def testReturnCode(self):
        '''Runs a basic Cint job and tests that the return code is propagated back to the user.'''
        print 'testReturnCode'
        exitCode = 45      
        r = self._makeRootApp("""
                #include <cstdlib>
                void test(){
                    exit(%d);
                }
        """ % exitCode)
        
        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                #wait for test to finish but don't assert
                from GangaTest.Framework.utils import sleep_until_completed
                sleep_until_completed(j)
                assert(j.status == 'failed')
 
                print 'exitCode',j.backend.exitcode, exitCode
                assert j.backend.exitcode == exitCode, 'Exit codes do not match'
        
        self._submitJob(r,JobTest(self))

    def testCintLoadSharedObject(self):
        '''Runs a basic Cint job and tests so file loading.'''
        print 'testCintLoadSharedObject'
        r = self._makeRootApp("""
                #include <cstdlib>
                void test(){
                    //set up main, eg command line opts
                    char* argv[] = {"runMain.C","--muons","100"};
                    int argc = 3;
  
                    //compile the shared object
                    gSystem->Exec("make");  
                    
                    //load the shared library
                    gSystem->Load("libMain");

                    //run the code
                    Main m(argv,argc);
                    int returnCode = m.run();
                }
        """)
        #seems to be some problem with default version of ROOT
        r.version = '5.14.00h'

        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                from GangaTest.Framework.utils import file_contains
                from os.path import exists,join

                output = join(j.outputdir,'output.txt')
                assert exists(output), 'Output file must exist'
                assert(file_contains(output,'12345'))
                assert(j.status != 'failed')
            def configJob(self,j):
                super(JobTest,self).configJob(j)
                from os.path import dirname,join
                #testing framework seems to define this variable for us
                testDir = dirname(globals()['testPath'])
                
                mainCpp = join(testDir,'Main.cpp')
                mainH = join(testDir,'Main.h')
                makeFile = join(testDir,'Makefile')
                
                j.inputsandbox = [mainCpp,mainH,makeFile]
                j.outputsandbox = ['output.txt']

        self._submitJob(r,JobTest(self))
    

    def testPythonArgs(self):
        '''Runs a basic python job and tests that int args are handled.'''
        print 'testPythonArgs'
        n = 123
        s = 'foo'
        r = self._makeRootApp("""#!/usr/bin/env python
if __name__ == '__main__':
    from sys import argv
    assert(len(argv) == 4)
                
    n = int(argv[1])
    str = argv[2]
                
    f = file('output.txt','w')
    try:
        s = '''###%d###
@@@%s@@@
'''
        f.write(s % (n,str))        
    finally:
        f.close()
        """, [n,s], usepython = True)
        
        assert(r.usepython)
        
        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                from GangaTest.Framework.utils import file_contains
                from os.path import join
               
                output = join(j.outputdir,'output.txt')
                assert(file_contains(output,'###%d###' % n))
                assert(file_contains(output,'@@@%s@@@' % s))
                assert(j.status != 'failed')
            def configJob(self,j):
                super(JobTest,self).configJob(j)
                j.outputsandbox = ['output.txt']

        self._submitJob(r,JobTest(self))
        
    def testPythonRootBasicImport(self):
        '''Runs a basic python job and used same basic root classes'''
        print 'testPythonBasicRootImport'
        n = 1001
        r = self._makeRootApp("""#!/usr/bin/env python
from ROOT import gRandom
from sys import argv
n = int(argv[1])
ran = []
for i in range(n):
    ran.append(gRandom.Gaus())
                
f = file('output.txt','w')
try:
    f.write('###%d###' % len(ran))        
finally:
    f.close()
""", [n], usepython = True)
        
        assert(r.usepython)
        
        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                from GangaTest.Framework.utils import file_contains
                from os.path import join
                output = join(j.outputdir,'output.txt')
                assert(file_contains(output,'###%d###' % n))
                assert(j.status != 'failed')
            def configJob(self,j):
                super(JobTest,self).configJob(j)
                j.outputsandbox = ['output.txt']

        self._submitJob(r,JobTest(self))
    
    def testUsePythonFlag(self):
        '''Runs a .C file as python using the usepython flag'''
        print 'testUsePythonFlag'
        r = self._makeRootApp("""#!/usr/bin/env python
f = file('output.txt','w')
try:
    f.write('###Running###')        
finally:
    f.close()
""", [], usepython = True, file_name = 'test.C')
        
        assert(not r.usepython)
        r.usepython = True
        
        class JobTest(_DefaultJobTest):
            def jobTest(self,j):
                super(JobTest,self).jobTest(j)
                from GangaTest.Framework.utils import file_contains
                from os.path import join
                output = join(j.outputdir,'output.txt')
                assert(file_contains(output,'###Running###'))
                assert(j.status != 'failed')
            def configJob(self,j):
                super(JobTest,self).configJob(j)
                j.outputsandbox = ['output.txt']
        self._submitJob(r,JobTest(self))

class _IJobTest(object):
    """Interface class for configuring and testing a job object"""
    def configJob(self,j):
        raise NotImplementedError
    def jobTest(self,j):
        raise NotImplementedError
 
class _DefaultJobTest(_IJobTest):
    """Does some simple job config and testing. Intended to be a common base class"""
    def __init__(self,test):
        self.test = test
    def configJob(self,j):
        """Sets up a generic root job for running"""
        if hasattr(j.backend, 'requirements'):
            #used for LCG
            j.backend.requirements.cputime = self.test.cputime
        elif  hasattr(j.backend, 'queue'):
            #used for batch
            j.backend.queue = '8nm'
        elif  hasattr(j.backend, 'CPUTime'):
            j.backend.CPUTime = self.test.cputime*60 #in sec
    def jobTest(self,j):
        """Tests that a job completed"""
        from GangaTest.Framework.utils import sleep_until_completed
        assert sleep_until_completed(j), 'Job did not complete (Status = %s)' % j.status

# Add the tests to the test container (TestRoot)
def addTests(test_container, testinstance, testname):
    """Nasty introspective function to produce a test suite"""
    from inspect import getmembers,ismethod
    import new
    #introspect the testclass to find methods to pass on
    for test in getmembers(testinstance): #the test methods used
        if ismethod(test[1]):
            instanceMethodName = test[1].__name__
            #look at the method name before appending
            if instanceMethodName[0:4].lower() == 'test':
                #these 2 lines are fragile - relies on _ nameing convention - don't use in method names!  
                method = new.instancemethod(lambda self: getattr(testinstance,globals()['testName'].split('_')[0])(),None,test_container)
                method_name='%s_%s' % (instanceMethodName,testname)
                #set the method onto the Test container
                setattr(test_container, method_name, method)

#am forced to hard code as plugins are not loaded yet.
#this is the list of backends to run
for b in backends_to_test:
    testcase = _TestRootBase(b)
    addTests(TestRoot, testcase, b)
