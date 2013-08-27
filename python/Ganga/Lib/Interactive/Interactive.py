###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Interactive.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
###############################################################################
# File: Interactive.py
# Author: K. Harrison
# Created: 060720
# Version 1.0: 060728
#
# KH 060803 - Corrected _getJobObject to getJobObject
#
# KH 060829 - Updated to use Sandbox module
#
# KH 060901 - Updates in submit and preparejob methods, for core changes
#
# KH 061103 - Corrected to take into account master input sandbox
#
# KH 080306 - Corrections from VR

"""Module containing class for running jobs interactively"""
                                                                                
__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "6 February 2008"
__version__ = "1.4"

from Ganga.Core import Sandbox
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility import logging, tempfile, util
from Ganga.Utility.Config import getConfig

import inspect
import os
import re
import shutil
import signal
import time
import copy


logger = logging.getLogger()

class Interactive( IBackend ):
   """Run jobs interactively on local host.
  
      Interactive job prints output directly on screen and takes the input from the keyboard.
      So it may be interupted with Ctrl-C
   """
    
   _schema = Schema( Version( 1, 0 ), {\
      "id" : SimpleItem( defvalue = 0, protected=1, copyable = 0,
         doc = "Process id" ),
      "status" : SimpleItem( defvalue = "new", protected = 1, copyable = 0,
         doc = "Backend status" ),
      "exitcode" : SimpleItem( defvalue = 0, protected = 1, copyable = 0,
         doc = "Process exit code" ),
      "workdir" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Work directory" ),
      "actualCE" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Name of machine where job is run" ) })

   _category = "backends"
   _name = 'Interactive'

   def __init__( self ):
      super( Interactive, self ).__init__()

   def _getIntFromOutfile\
      ( self, keyword = "", outfileName = "" ):
      value = -999
      job = self.getJobObject()
      if keyword and outfileName and hasattr( job, "outputdir" ):
         outfilePath = os.path.join( job.outputdir, outfileName )
         try:
            statfile = open( outfilePath )
            statString = statfile.read()
            testString = "".join( [ "^", keyword, " (?P<value>\\d*)" ] )
            regexp = re.compile( testString, re.M )
            match = regexp.search( statString )
            if match:
               value = int( match.group( "value" ) )
            statfile.close()
         except IOError:
            pass
      return value

   def submit( self, jobconfig, master_input_sandbox ):
      """Submit job to backend (i.e. run job interactively).
                                                                                
          Arguments other than self:
             subjobconfig         - Dictionary of subjob properties
             master_input_sandbox - Dictionary of properties of master job
                                                                                
          Return value: True always"""

      job = self.getJobObject()

      scriptpath = self.preparejob( jobconfig, master_input_sandbox )
      return self._submit(scriptpath, jobconfig.env)

   def resubmit( self ):
      return self._submit(self.getJobObject().getInputWorkspace().getPath("__jobscript__"))

   def _submit( self, scriptpath, env=copy.deepcopy(os.environ)):
      job = self.getJobObject()
      self.actualCE = util.hostname()
      logger.info('Starting job %d', job.id)

      try:
         job.updateStatus( "submitted" )
         self.status = "submitted"
         os.spawnve( os.P_WAIT, scriptpath, ( scriptpath, ), env )
         self.status = "completed"
      except KeyboardInterrupt:
         self.status = "killed"
         pass

      return True
      

   def kill( self ):

      """Method for killing job running on backend
                                                                                
         No arguments other than self:
                                                                                
         Return value: True always"""

      job = self.getJobObject()

      if not self.id:
         time.sleep( 0.2 )
         self.id = self._getIntFromOutfile( "PID:", "__id__" )

      try:
         os.kill( self.id, signal.SIGKILL )
      except OSError,x:
         logger.warning( "Problem killing process %d for job %d: %s",\
            self.id, job.id, str( x ) )

      self.status = "killed"
      self.remove_workdir()

      return True

   def remove_workdir( self ):

      """Method for removing job's work directory
                                                                                
         No arguments other than self:
                                                                                
         Return value: None"""

      try:
         shutil.rmtree( self.workdir )
      except OSError,x:
         logger.warning( "Problem removing workdir %s: %s", self.workdir,\
            str( x ) )        

      return None

   def preparejob( self, jobconfig, master_input_sandbox ):

      """Method for preparing job script"""

      job = self.getJobObject()

      inputfiles = jobconfig.getSandboxFiles()  
      inbox = job.createPackedInputSandbox( inputfiles )

      inbox.extend( master_input_sandbox )
      inpDir = job.getInputWorkspace().getPath()
      outDir = job.getOutputWorkspace().getPath()
      workdir = tempfile.mkdtemp()
      self.workdir = workdir
      exeString = repr( jobconfig.getExeString() )
      argList = jobconfig.getArgStrings()
      argString = " ".join( map( lambda x : "' \\'%s\\' '" % x, argList ) )

      outputSandboxPatterns = jobconfig.outputbox
      patternsToZip = []  
      wnCodeForPostprocessing = '' 
      wnCodeToDownloadInputFiles = ''            

      if (len(job.outputfiles) > 0):
        
         from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatternsForInteractive, getWNCodeForOutputPostprocessing   
         (outputSandboxPatterns, patternsToZip) = getOutputSandboxPatternsForInteractive(job)
         wnCodeForPostprocessing = 'def printError(message):pass\ndef printInfo(message):pass' + getWNCodeForOutputPostprocessing(job, '')      

      if (len(job.inputfiles) > 0):

         from Ganga.GPIDev.Lib.File.OutputFileManager import getWNCodeForDownloadingInputFiles

         wnCodeToDownloadInputFiles = getWNCodeForDownloadingInputFiles(job, '')

      commandList = [
         "#!/usr/bin/env python",
         "# Interactive job wrapper created by Ganga",
         "# %s" % ( time.strftime( "%c" ) ),
         "",
         inspect.getsource( Sandbox.WNSandbox ),
         "import os",
         "import sys",
         "import time",
         "import glob",
         "",
         "sys.path.insert( 0, '%s' )" % \
            getConfig( "System" )[ "GANGA_PYTHONPATH" ],
         "",
         "statfileName = os.path.join( '%s', '__jobstatus__' )" % outDir,
         "try:",
         "   statfile = open( statfileName, 'w' )",
         "except IOError, x:",
         "   print 'ERROR: Unable to write status file: %s' % statfileName",
         "   print 'ERROR: ',x",
         "   raise",
         "",
         "idfileName = os.path.join( '%s', '__id__' )" % outDir,
         "try:",
         "   idfile = open( idfileName, 'w' )",
         "except IOError, x:",
         "   print 'ERROR: Unable to write id file: %s' % idfileName",
         "   print 'ERROR: ',x",
         "   raise",
         "idfile.close()",
         "",
         "timeString = time.strftime"\
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
         "statfile.write( 'START: ' + timeString + os.linesep )",
         "",
         "os.chdir( '%s' )" % workdir,
         "for inFile in %s:" % inbox,
         "   getPackedInputSandbox( inFile )",
         "", 
         wnCodeToDownloadInputFiles,
         "for key, value in %s.iteritems():" % jobconfig.env,
         "   os.environ[ key ] = value",
         "",
         "pyCommandList = [",
         "   'import os',",
         "   'idfileName = \"%s\"' % idfileName,",
         "   'idfile = open( idfileName, \"a\" )',",
         "   'idfile.write( \"PID: \" + str( os.getppid() ) )',",
         "   'idfile.flush()',",
         "   'idfile.close()' ]",
         "pyCommandString = ';'.join( pyCommandList )",
         "",
         "commandList = [",
         "   'python -c \\\'%s\\\'' % pyCommandString,",
         "   'exec ' " + exeString + " " + argString + "]",
         "commandString = ';'.join( commandList )",
         "",
         "result = os.system( '%s' % commandString )",
         "",
         wnCodeForPostprocessing,
         "for patternToZip in " + str(patternsToZip) +":",
         "   for currentFile in glob.glob(patternToZip):",
         "      os.system('gzip %s' % currentFile)",
         "",
         "createOutputSandbox( %s, None, '%s' )" % \
            ( outputSandboxPatterns, outDir ),
         "",
         "statfile.write( 'EXITCODE: ' + str( result >> 8 ) + os.linesep )",
         "timeString = time.strftime"\
            + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
         "statfile.write( 'STOP: ' + timeString + os.linesep )",
         "statfile.flush()",
         "statfile.close()" ]

      commandString = "\n".join( commandList )
      return job.getInputWorkspace().writefile\
         ( FileBuffer( "__jobscript__", commandString), executable = 1 )

   def updateMonitoringInformation( jobs ):

      for j in jobs:

         if not j.backend.id:
            id = j.backend._getIntFromOutfile( "PID:", "__id__" )
            if id > 0:
               j.backend.id = id
               if ( "submitted" == j.backend.status ):
                  j.backend.status = "running"

        # Check that the process is still alive
         if j.backend.id:
            try:
               os.kill( j.backend.id, 0 )
            except:
               j.backend.status = "completed"

         if j.backend.status in [ "completed", "failed", "killed" ]:
            j.backend.exitcode = j.backend._getIntFromOutfile\
               ( "EXITCODE:", "__jobstatus__" )
           # Set job status to failed for non-zero exit code
            if j.backend.exitcode:
                  if j.backend.exitcode in [ 2, 9, 256 ]:
                     j.backend.status = "killed"
                  else:
                     j.backend.status = "failed"
            if ( j.backend.status != j.status ):
               j.updateStatus( j.backend.status )
         
      return None

   updateMonitoringInformation = staticmethod( updateMonitoringInformation )
