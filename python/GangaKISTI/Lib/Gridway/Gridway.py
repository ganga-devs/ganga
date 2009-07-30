###############################################################################
# SNU DCS Lab. & KISTI Project.
#
# Gridway.py
###############################################################################

"""Module containing class for handling job submission to Gridway backend"""

__author__  = "Yoonki Lee <yklee@dcslab.snu.ac.kr>"
__date__    = "2008. 10. 15"
__version__ = "1.7"


from Ganga.Core import Sandbox
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility.ColourText import Foreground, Effects

import Ganga.Utility.Config 
import Ganga.Utility.logging

import commands
import inspect
import os
import shutil
import time

logger = Ganga.Utility.logging.getLogger()


class Gridway(IBackend):
   """
   Gridway backend - submit jobs to Gridway Metascheduler.
   """

   _schema = Schema( Version( 1, 0 ), {\
      "id" : SimpleItem( defvalue = "", protected = 1, copyable = 0, doc = "Gridway jobid" ),
      "status" : SimpleItem( defvalue = "", protected = 1, copyable = 0, doc = "Gridway status"),
      "actualCE" : SimpleItem( defvalue = "", protected = 1, copyable = 0, doc = "Machine where job has been submitted" ) } )

   _category = "backends"
   _name =  "Gridway"   

   statusDict = \
    {
        "pend" : "Pending", 
        "hold" : "Hold", 
        "prol" : "Prolog",
        "prew" : "Pre-wrapper", 
        "wrap" : "Wrapper", 
        "epil" : "Epilog", 
        "migr" : "Migration", 
        "stop" : "Stopped", 
        "fail" : "Failed", 
        "done" : "Done"
    }

   def __init__(self):
      super(Gridway,self).__init__()


   def submit( self, jobconfig, master_input_sandbox, job=None ):
      """
      Submit job to backend.
      Return value: True if job is submitted successfully,
      or False otherwise
      """
      
      # make job template file and return its path
      jtpath = self.preparejob( jobconfig, master_input_sandbox, job )
      
      # submit to gridway and return status information
      status = self.submit_jt( jtpath )
      return status

   def preparejob( self, jobconfig, master_input_sandbox, job=None ):
      """
      Prepare Gridway job template file
      """

      if job is None : 
         job = self.getJobObject()
      inbox = job.createPackedInputSandbox( jobconfig.getSandboxFiles() )
      inpDir = job.getInputWorkspace().getPath()
      outDir = job.getOutputWorkspace().getPath()
      
      infileList = []
      
      exeString = jobconfig.getExeString().strip()
      quotedArgList=[]
      for arg in jobconfig.getArgStrings():
         quotedArgList.append( "\\'%s\\'" % arg ) 
      exeCmdString = " ".join( [ exeString ] + quotedArgList )

      for filePath in inbox:
         if not filePath in infileList:
            infileList.append( filePath )

      for filePath in master_input_sandbox:
         if not filePath in infileList:
            infileList.append( filePath )

      fileList = []
      for filePath in infileList:
         fileList.append( os.path.basename( filePath ) )

      
      # ganga wrapper script for gridway backend
      commandList = [
          "#!/usr/bin/env python",
          "# Gridway job wrapper created by Ganga",
          "# %s" % ( time.strftime( "%c" ) ),
          "",
          inspect.getsource( Sandbox.WNSandbox ),
          "",
          "import os",
          "import time",
          "",
          "startTime = time.strftime"\
          + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
          "",
          "for inFile in %s:" % str( fileList ),
          "   getPackedInputSandbox( inFile )",
          "",
          "if os.path.isfile( '%s' ):" % os.path.basename( exeString ),
          "   os.chmod( '%s', 0755 )" % exeString,
          "result = os.system( '%s' )" % exeCmdString,
          "",
          "endTime = time.strftime"\
          + "( '%a %d %b %H:%M:%S %Y', time.gmtime( time.time() ) )",
          "print '\\nJob start: ' + startTime",
          "print 'Job end: ' + endTime",
          "print 'Exit code: %s\\n' % str( result )",
          "" ]

      commandString = "\n".join( commandList )
      if job.name:
          name = job.name
      else:
          name = job.application._name 
      
      wrapperName = "_".join( [ "Ganga", str( job.id ), name ] )
      
      # write job template file to workspace
      wrapper = job.getInputWorkspace().writefile\
            ( FileBuffer( wrapperName, commandString), executable = 1 )

      # add wrapper script to input sandbox
      infileList.append(wrapper)
        
      # protocol should be specified in job template
      infileList2 = []
      for file in infileList:
         infileList2.append("file://"+file)

      # add output sandbox
      outfileList=[]
      for file in jobconfig.outputbox:
         outfileList.append(file+" "+outDir+file)         

      infileString = ",".join( infileList2 )
      outfileString = ",".join( outfileList )

      jtDict = \
            {
               'EXECUTABLE' : os.path.basename(wrapper),
               'STDOUT_FILE' : 'stdout',
               'STDERR_FILE' : 'stderr',
            }
               
      
      if infileString:
          jtDict[ 'INPUT_FILES' ] = infileString

      if outfileString:
         jtDict[ 'OUTPUT_FILES' ] = outfileString

      """
      # 2009/05/13 hgkim
      # HOSTNAME, QUEUE_NAME setting
      if config['Gridway']['HOSTNAME'] != '':
         jtDict[ 'REQUIREMENTS' ] = 'HOSTNAME = "%s"' % ['Gridway']['HOSTNAME']

      if config['Gridway']['QUEUE_NAME'] != '':
         jtDict[ 'REQUIREMENTS' ] = 'QUEUE_NAME= "%s"' % ['Gridway']['QUEUE_NAME']
      """

      jtList = [
         "# Gridway Description File created by Ganga",
         "# %s" % (time.strftime( "%c" ) ),
         ""]

      for key, value in jtDict.iteritems():
         jtList.append( "%s = %s" % (key, value))
      jtString = "\n".join(jtList)
      
      return job.getInputWorkspace().writefile\
              ( FileBuffer( "__jt__", jtString) )



   def submit_jt( self, jtpath = "" ):
      """
      Submit Gridway Description File.
          
          Argument other than self:
             jtpath - path to Gridway Description File to be submitted
                       
          Return value: True if job is submitted successfully,
                        or False otherwise
      """

      commandList = [ "gwsubmit -t" ]
      commandList.append( jtpath )
      commandString = " ".join( commandList )


      status, output = commands.getstatusoutput( commandString )

      if status == 0 :
         output = commands.getoutput("gwps")
         # parsing gwps result and get id (last job in gwps result)
         self.id = output.split('\n')[-1].split()[1]

      return not self.id is ""

   def resubmit( self, job=None ):
      """
      Resubmit job that has already been configured.
          
          Return value: True if job is resubmitted successfully,
                        or False otherwise
      """

      if job is None:
         job = self.getJobObject()
      inpDir = job.getInputWorkspace().getPath()
      outDir = job.getOutputWorkspace().getPath()

      # Delete any existing output files, and recreate output directory
      if os.path.isdir( outDir ):
         shutil.rmtree( outDir )
      if os.path.exists( outDir ):
         os.remove( outDir )
      os.mkdir( outDir )

      # Determine path to job template file
      jtpath = os.path.join( inpDir, "__jt__" )

      # Resubmit job
      if os.path.exists( jtpath ):
         status = self.submit_jt( jtpath )
      else:
         logger.warning\
            ( "No Gridway Description File for job '%s' found in '%s'" % \
            ( str( job.id ), inpDir ) )
         logger.warning( "Resubmission failed" )
         status = False

      return status

   def kill( self, job=None  ):
      """
      Kill running job.

         No arguments other than self

         Return value: True if job killed successfully,
                       or False otherwise
      """

      if job is None:
         job = self.getJobObject()

      if not self.id:
         logger.warning( "Job %s not running" % job.id )
         return False

      killCommand = "gwkill "+job.backend.id

      status, output = commands.getstatusoutput( killCommand )

      if output == "":
         job.backend.status = "Removed"
         killStatus = True
      else:
         logger.error( "Error killing job '%s' - Gridway id '%s'" % \
            ( job.id, job.backend.id ) )
         logger.error( "Tried command: '%s'" % killCommand )
         logger.error( "Command output: '%s'" % output )
         killStatus = False

      return killStatus

   def updateMonitoringInformation( jobs ):

      jobDict = {}
      for job in jobs:
         if job.backend.id:
            jobDict[ job.backend.id ] = job

      idList = jobDict.keys()

      if not idList:
         return


      status, output = commands.getstatusoutput( "gwps -n" )
      if 0 != status:
         logger.error( "Problem retrieving status for Gridway jobs" )
         return

      infoList = output.split( "\n" )

      allDict = {}
      for infoString in infoList:
         tmpList = infoString.split()

         id = tmpList[1]
         host = tmpList[10]
         status = tmpList[2]
         
         if id:
            allDict[ id ] = {}
            allDict[ id ][ "status" ] = Gridway.statusDict[ status ]
            allDict[ id ][ "host" ] = host

      fg = Foreground()
      fx = Effects()
      status_colours = { 'submitted' : fg.orange,
                         'running'   : fg.green,
                         'completed' : fg.blue }
      
      for id in idList:
         #subjob or not - move stdout, stderr
         if not jobDict[id].subjobs:
            inpDir = jobDict[id].getInputWorkspace().getPath()
            outDir = jobDict[id].getOutputWorkspace().getPath()
            isExist = os.path.isfile(inpDir+"stdout") and os.path.isfile(inpDir+"stderr")
            if isExist:
               os.system("mv "+inpDir+"stdout "+inpDir+"stderr "+outDir)
         else:
            for subjob in jobDict[id].subjobs:
               inpDir = subjob.getInputWorkspace().getPath()
               outDir = subjob.getOutputWorkspace().getPath()
               isExist = os.path.isfile(inpDir+"stdout") and os.path.isfile(inpDir+"stderr")
               if isExist:
                  os.system("mv "+inpDir+"stdout "+inpDir+"stderr "+outDir)
            
         printStatus = False
         if jobDict[ id ].status == "killed":
            continue

         globalId = id

         if globalId in allDict.keys():
            status = allDict[ globalId ][ "status" ]
            host = allDict[ globalId ][ "host" ]
            if status != jobDict[ id ].backend.status:
               printStatus = True
               jobDict[ id ].backend.status = status
               if jobDict[ id ].backend.status == "Pending":
                  jobDict[ id ].updateStatus( "submitted" )
               elif jobDict[ id ].backend.status == "Prolog":
                  jobDict[ id ].updateStatus( "running" )
               elif jobDict[ id ].backend.status == "Pre-wrapper":
                  jobDict[ id ].updateStatus( "running" )
               elif jobDict[ id ].backend.status == "Wrapper":
                  jobDict[ id ].updateStatus( "running" )
               elif jobDict[ id ].backend.status == "Epilog":
                  jobDict[ id ].updateStatus( "running" )
               elif jobDict[ id ].backend.status == "Failed":
                  jobDict[ id ].updateStatus( "failed" )
               elif jobDict[ id ].backend.status == "Done":
                  jobDict[ id ].updateStatus( "completed" )
               
            if host:
               if jobDict[ id ].backend.actualCE != host:
                  jobDict[ id ].backend.actualCE = host
         else:
            printStatus = True
            jobDict[ id ].backend.status = ""
            jobDict[ id ].updateStatus( "completed" )

         if printStatus:
            if jobDict[ id ].backend.actualCE:
               hostInfo = jobDict[ id ].backend.actualCE
            else:
               hostInfo = "Localhost"
            status = jobDict[ id ].status
            if status_colours.has_key( status ):
               colour = status_colours[ status ]
            else:
               colour = fg.magenta
            if "submitted" == status:
               preposition = "to"
            else:
               preposition = "on"

            if jobDict[ id ].backend.status:
               backendStatus = "".join\
                  ( [ " (", jobDict[ id ].backend.status, ") " ] )
            else:
               backendStatus = ""

            logger.info( colour + 'Job %d %s%s %s %s - %s' + fx.normal,\
               jobDict[ id ].id, status, backendStatus, preposition, hostInfo,\
               time.strftime( '%c' ) )

      return None

   updateMonitoringInformation = staticmethod( updateMonitoringInformation )
