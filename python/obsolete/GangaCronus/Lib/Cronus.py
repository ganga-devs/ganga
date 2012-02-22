###############################################################################
# Ganga Project. http://cern.ch/ganga
#
###############################################################################
# File: Condor.py
# Author: R. Walker
# Adapted from Condor.py
#

"""Module containing class for handling job submission to Cronus backend"""

__author__  = "R. Walker"
__date__    = "5 March 2007"
__version__ = "1.1"

from CronusRequirements import CronusRequirements

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
import time
import types

logger = Ganga.Utility.logging.getLogger()

class Cronus( IBackend ):
   """Class for handling job submission to Cronus backend

      Implements following methods required by IBackend parent class:
         submit                      - Perform job submission
         updateMonitoringInformation - Update information for all jobs
                                       submitted to the backend"""
    
   _schema = Schema( Version( 1, 0 ), {\
      "requirements" : ComponentItem( category = "cronus_requirements",
         defvalue = "CronusRequirements",
         doc = "Requirements for selecting execution host" ),
      "env" : SimpleItem( defvalue = {},
         doc = 'Environment settings for execution host' ),
      "rank" : SimpleItem( defvalue = "Memory",
         doc = "Ranking scheme to be used when selecting execution host" ),
      "submit_options" : SimpleItem( defvalue = [], sequence = 1,
         doc = "Options passed to Condor at submission time" ),
      "id" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Condor jobid" ),
      "status" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Condor status"),
      "cputime" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "CPU time used by job"),
      "priority" : SimpleItem( defvalue = 0,
                         doc = "User level job priority" ),
      "actualCE" : SimpleItem( defvalue = "", protected = 1, copyable = 0,
         doc = "Machine where job has been submitted" ),
      "scheduler" : SimpleItem( defvalue = "",
         doc = "Intermediate(regional) scheduler for Condor-C submission" ),
      "collector" : SimpleItem( defvalue = "rodvm00.triumf.ca",
         doc = "Intermediate scheduler is advertised in this Collector" ),
      "shared_filesystem" : SimpleItem( defvalue = True,
         doc = "Flag indicating if Condor nodes have shared filesystem" ),
      "universe" : SimpleItem( defvalue = "vanilla",
         doc = "Type of execution environment to be used by Condor" ),
      "globusscheduler" : SimpleItem( defvalue = "", doc = \
         "Globus scheduler to be used (required for Condor-G submission)" ),
      "globus_rsl" : SimpleItem( defvalue = "",
         doc = "Globus RSL settings (for Condor-G submission)" ),

      } )

   _category = "backends"
   _name =  "Cronus"

   def __init__( self ):
      super( Cronus, self ).__init__()
    
   def submit( self, jobconfig, master_input_sandbox ):
      """Submit job to backend.
                       
          Return value: True if job is submitted successfully,
                        or False otherwise"""
       
      cdfpath = self.preparejob( jobconfig, master_input_sandbox )

      commandList = [ "condor_submit -v" ]
      commandList.extend( self.submit_options )
      commandList.append( cdfpath )
      commandString = " ".join( commandList )

      bef=time.time()
      status, output = commands.getstatusoutput( commandString )
      now=time.time()

      self.id = ""
      if 0 == status:
         logger.info("condor_submit took %.3f secs"%(now-bef))
         tmpList = output.split( "\n" )
         for item in tmpList:
            if 1 + item.find( "** Proc" ):
               self.id = item.strip( ":" ).split()[ 2 ]
               break

      return not self.id is ""

   def kill( self  ):
      """Kill running job.

         No arguments other than self

         Return value: True if job killed successfully,
                       or False otherwise"""

      job = self.getJobObject()

      if not self.id:
         logger.warning( "Job %s not running" % job.id )
	 return False
	  
      commands.getstatusoutput( "condor_rm %s" % job.backend.id )
      job.backend.status = "X"
      job.updateStatus( "killed" )
      return True

   def preparejob( self, jobconfig, master_input_sandbox ):
      """Prepare Condor description file"""

      job = self.getJobObject()
      inbox = job.createPackedInputSandbox( jobconfig.getSandboxFiles() )
      inpDir = job.getInputWorkspace().getPath()
      outDir = job.getOutputWorkspace().getPath()

      infileList = []

      exeCmdString = jobconfig.getExeCmdString()
      exeString = jobconfig.getExeString().strip()

      for filePath in inbox:
         if not filePath in infileList:
            infileList.append( filePath )

      for filePath in master_input_sandbox:
         if not filePath in infileList:
            infileList.append( filePath )

      fileList = []
      for filePath in infileList:
         fileList.append( os.path.basename( filePath ) )

      commandList = [
         "#!/usr/bin/env python",
         "# Interactive job wrapper created by Ganga",
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
         "# Make certain output files exist for transfer_output",
         "for outFile in %s:"% str( jobconfig.outputbox ),
         "   f=open(outFile,'a')",
         "   f.close()",
         "if os.path.isfile( '%s' ):" % os.path.basename( exeCmdString ),
         "   os.chmod( '%s', 0755 )" % exeCmdString,
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
      wrapper = job.getInputWorkspace().writefile\
         ( FileBuffer( wrapperName, commandString), executable = 1 )

      infileString = ",".join( infileList )
      outfileString = ""
      if len(jobconfig.outputbox) > 0:
         outfileString += ",".join( jobconfig.outputbox )

 # Check CEs is a list
      if isinstance(self.requirements.CEs,types.StringType):
         self.requirements.CEs=self.requirements.CEs.split(' ')

 # input data CE requirements. Three sources: user, user exclude, location
 # Build combined CEs list and make sure there's at least 1
      if job.inputdata and job.inputdata.match_ce and \
                             job.inputdata._name == 'DQ2Dataset':
         locCEs = job.inputdata.list_locations_ce()
         if len(locCEs) == 0:
            logger.error("Input dataset has no location CEs")
            return False
         if len(self.requirements.CEs)==0:
            self.requirements.CEs=locCEs
         else:
            commonCEs=[]
            for ce in self.requirements.CEs:
              if ce in locCEs:
                 commonCEs+=[ce]
            if len(commonCEs)==0:
               logger.error("Input dataset has no location CEs in your list of pssible CEs: %s"%' '.join(self.requirements.CEs))
               return False
            else:
               self.requirements.CEs=commonCEs
#         logger.warn('Common CEs: %s'%' '.join(commonCEs))

         
         self.requirements.CEs
         self.requirements.excludedCEs

 # common JDL
      cdfDict =\
         {
         'on_exit_remove' : 'True',
         'should_transfer_files' : 'YES',
         'transfer_input_files' : infileString,
         'when_to_transfer_output' : 'ON_EXIT_OR_EVICT',
         'transfer_output_files' : outfileString,
         'executable' : wrapper,
         'transfer_executable' : 'True',
         'notification' : 'Never',
         'initialdir' : outDir,
         'error' : 'stderr',
         'output' : 'stdout',
         'log' : 'condorLog',
         'priority' : self.priority,
         }      

      if not self.scheduler: 
       # direct local Schedd to glide-in
        cdfDict1 = \
         {
         'universe' : 'vanilla',
         'rank' : self.rank,
         'stream_output' : 'true',
         'stream_error' : 'true',
         'environment' : '"%s"'%self.buildEnvString(jobconfig),
         'Requirements' : '%s'%self.requirements.convert()
         }

      else:
       # local schedd to regional schedd to glide-in
        cdfDict1={
         'universe' : 'grid',
         'grid_type' : 'condor',
         'stream_output' : 'false',
         'stream_error' : 'false',
         'remote_schedd' : 'rodvm01.triumf.ca',
         'grid_resource' : 'condor %s %s'%(self.scheduler,self.collector),
         '+remote_jobuniverse' : '5',
         '+remote_ShouldTransferFiles' : '"YES"',
         '+remote_WhenToTransferOutput' : '"ON_EXIT"',
         '+remote_Environment' : '"%s"'%self.buildEnvString(jobconfig),
         '+remote_Requirements' : '%s'%self.requirements.convert()
         }
      for k in cdfDict1.keys():
        cdfDict[k]=cdfDict1[k]
      
#      if infileString:
#         cdfDict[ 'transfer_input_files' ] = infileString

#      if outfileString:
#         cdfDict[ 'transfer_output_files' ] = 'stdout,stderr,'+outfileString
#         cdfDict[ '+remote_TransferOutput' ] = '"%s"'%outfileString

      cdfList = [
         "# Condor Description File created by Ganga",
         "# %s" % ( time.strftime( "%c" ) ),
         "" ]
      for key, value in cdfDict.iteritems():
         cdfList.append( "%s = %s" % ( key, value ) )

#      cdfList.append( self.requirements.convert() )


      cdfList.append( "queue" )
      cdfString = "\n".join( cdfList )

      return job.getInputWorkspace().writefile\
         ( FileBuffer( "__cdf__", cdfString) )

   def buildEnvString(self,jobconfig):
    # New condor format - space seperated and risky - beware envs with spaces
    # TODO: protect quotes and spaces
    #
      envList=[]
      if self.env:
         for key in self.env.keys():
            envList.append( "=".join( [ key, str( self.env[ key ] ) ] ) )
      if jobconfig.env:
         for key in jobconfig.env.keys():
            envList.append( "=".join( [ key, str( jobconfig.env[ key ] ) ] ) )
      envString = " ".join( envList )
      return envString

   def updateMonitoringInformation( jobs ):

      jobDict = {}
      for job in jobs:
         if job.backend.id:
            jobDict[ job.backend.id ] = job

      idList = jobDict.keys()

      if not idList:
         return

# 1234785.0 2 1 0 46656
      status, output = commands.getstatusoutput( "condor_q -format '%d' clusterid -format '.%d' procid -format ' %d' JobStatus -format ' %d' RemoteUserCpu -format ' %d' RemoteWallClockTime -format ' %d\n' ImageSize_RAW" )
      if 0 != status:
         logger.error( "Problem retrieving status for Condor jobs" )
         return

      allList = output.split( "\n" )

      allDict = {}
      for statusline in allList:
         infoList = statusline.split()
         if len(infoList) != 5:
            logger.error( "Condor status line format problem: %s"
                          %str(infoList) )
         else:
            id = infoList[0]
            JobStatus = int(infoList[1])
            RemoteUserCpu = infoList[2]
            RemoteWallClockTime = infoList[3]
            ImageSize_RAW = infoList[4]
            
            allDict[ id ] = {'JobStatus':JobStatus,
                             'RemoteUserCpu':RemoteUserCpu,
                             'RemoteWallClockTime':RemoteWallClockTime,
                             'ImageSize_RAW':ImageSize_RAW}

      fg = Foreground()
      fx = Effects()
      status_colours = { 
                         'submitted' : fg.black,
                         'removed'   : fg.red,
                         'running'   : fg.green,
                         'completed' : fg.blue,
                         'held'      : fg.orange}
      
     # backend status to ganga status mapping
      back2ganga = {1:'submitted',
              2:'running',
              3:'removed',
              4:'completed',
              5:'held',
              }

      for id in idList:
         printStatus = False
         
         if jobDict[ id ].status == "killed":
            continue
         if id in allDict.keys():
            bstatus = allDict[ id ]['JobStatus']
            if bstatus != jobDict[ id ].backend.status:
               printStatus = True
               jobDict[ id ].backend.status = bstatus
               if bstatus in back2ganga.keys():
                  jobDict[ id ].updateStatus( back2ganga[bstatus] )
               jobDict[ id ].backend.cputime = allDict[ id ]['RemoteUserCpu']
         else:
           # job not found, so is completed 
            printStatus = True
            jobDict[ id ].backend.status = 4
            jobDict[ id ].updateStatus( "completed" )

         if printStatus:
            if jobDict[ id ].backend.actualCE:
               hostInfo = jobDict[ id ].backend.actualCE
            else:
               hostInfo = "Cronus"
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
               backendStatus = jobDict[ id ].backend.status
            else:
               backendStatus = ""

            logger.info( colour + 'Job %d %s%d %s %s - %s' + fx.normal,\
               jobDict[ id ].id, status, backendStatus, preposition, hostInfo,\
               time.strftime( '%c' ) )

      return None

   updateMonitoringInformation = \
      staticmethod( updateMonitoringInformation )
