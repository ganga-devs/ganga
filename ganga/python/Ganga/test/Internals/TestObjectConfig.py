################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestObjectConfig.py,v 1.1 2008-07-17 16:41:15 moscicki Exp $
################################################################################

# File: testObjectConfig.py
# Author: K. Harrison
# Created: 051231
# Last modified: 060103
                                                                                
"""
   Test suite for use of config settings to override GangaObject defaults
"""

from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Utility.Config import ConfigError
      
class TestObjectConfig( GangaGPITestCase ):
    """
    Class defining tests for use of config settings
    to override GangaObject defaults. This test does not preserve options set at the User level.
    It reverts to Session level values.
    """

    def setUp(self):
       GangaGPITestCase.setUp(self)
       print "WARNING: this test is NOT GPI test and should be moved to Internal test collection..."
       
    def tearDown(self):
       GangaGPITestCase.tearDown(self)
       used_configs = ['defaults_Executable',"defaults_LSF","defaults_Job"]	
       for c in used_configs:
          getConfig(c).revertToSessionOptions()

    def test001_SimpleItems( self ):
        
       # Set config values for Executable attributes
       exeConfig = getConfig( "defaults_Executable" )
       testArgs = [ "arg1", "arg2", "arg3" ]
       testExe = "/myDir/myExe"
       exeConfig.setUserValue("args",testArgs)
       exeConfig.setUserValue("exe",testExe)

       # Perform checks
       myExecutable = Executable()
       assert( myExecutable.args == testArgs )
       assert( myExecutable.exe == testExe )
       return None
   
   
    def test002_ComponentItems( self ):

       # Set the LSF queue attribute
       testQueue1 = "myQueue1"
       lsfConfig = getConfig( "defaults_LSF" )
       lsfConfig.setUserValue("queue",testQueue1)

       # Set the Job backend attribute 
       jobConfig = getConfig( "defaults_Job" )
       jobConfig.setUserValue("backend","LSF")

       try:
           jobConfig.setUserValue("backend.queue","myQueue2")
           assert(0)
       except ConfigError,x:
           print x
       
       # Perform checks
       lsf = LSF()
       job = Job()
       assert( lsf.queue == testQueue1 )
       assert( job.backend._impl._name == "LSF" )
       assert( job.backend.queue == testQueue1 )

       return None

    def test003_failures(self):
       
       exe1 = Executable()
       exeConfig = getConfig( "defaults_Executable" )

       try:
           exeConfig.setUserValue("not_existing_args", ['something_wrong'])
           assert(0)
       except ConfigError,x:
           print x
           
       exe2 = Executable()
       assert(exe2.args == exe1.args)

       job1 = Job()
       jobConfig = getConfig( "defaults_Job" )

       try:
           jobConfig.setUserValue("backend", "NOT_EXISTING_BACKEND")           
           jobConfig.setUserValue("backend.queue", "x")
           assert(0)           
       except ConfigError,x:
           print x
           
       job2 = Job()

       print job1.backend
       print job2.backend
       
       assert(job1.backend == job2.backend)

##     def test004_failureBackup(self):
##        jobConfig = getConfig( "Job_Properties" )
       
##        # first set something legal
##        jobConfig.setSessionOption("backend","LCG")
##        job1 = Job()

##        # now something illegal
##        jobConfig.setUserValue("backend", "NOT_EXISTING_BACKEND")
##        jobConfig.setUserValue("backend.queue", "x")
##        job2 = Job()

##        # job2 is expected to have backend

##        print job1.backend
##        print job2.backend
       
##        assert(job1.backend == job2.backend)       

