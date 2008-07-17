"""Module containing class for handling job submission to Remote backend"""

__author__  = "Mark Slater <mws@hep.ph.bham.ac.uk>"
__date__    = "10 June 2008"
__version__ = "1.0"

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
from Ganga.Lib.Root import randomString

logger = Ganga.Utility.logging.getLogger()

class Remote( IBackend ):
   """Remote backend - submit jobs to a Remote pool.

   The remote backend works as an SSH tunnel to a remote site
   where a ganga session is opened and the job submitted there
   using the specified remote_backend. It is (in theory!)
   transparent to the user and should allow submission of any jobs
   to any backends that are already possible in Ganga.

   NOTE: Due to the file transfers required, there can be some slow
   down during submission and monitoring

   
   E.g. 1 - Hello World example submitted to local backend:

   j = Job(application=Executable(exe='/bin/echo',args=['Hello World']), backend="Remote")
   j.backend.host = "bluebear.bham.ac.uk"                  # Host name
   j.backend.username = "slatermw"                         # User name
   j.backend.ganga_cmd = "/bb/projects/Ganga/runGanga"     # Ganga Command line on remote site
   j.backend.ganga_dir = "/bb/phy/slatermw/gangadir/remote_jobs"  # Where to store the jobs
   j.backend.remote_backend = Local()
   j.submit()


   E.g. 2 - Root example submitted to PBS backend:
   
   r = Root()
   r.version = '5.14.00'
   r.script = 'gengaus.C'
   
   j = Job(application=r,backend="Remote")
   j.backend.host = "bluebear.bham.ac.uk"
   j.backend.username = "slatermw"
   j.backend.ganga_cmd = "/bb/projects/Ganga/runGanga"
   j.backend.ganga_dir = "/bb/phy/slatermw/gangadir/remote_jobs"
   j.outputsandbox = ['gaus.txt']
   j.backend.remote_backend = PBS()
   j.submit()


   E.g. 3 - Athena example submitted to LCG backend
   NOTE: you don't need a grid certificate (or UI) available on the local machine,
   just the host machine

   j = Job()
   j.name='Ex3_2_1'
   j.application=Athena()
   j.application.prepare(athena_compile=False)
   j.application.option_file='/disk/f8b/home/mws/athena/testarea/13.0.40/PhysicsAnalysis/AnalysisCommon/UserAnalysis/run/AthExHelloWorld_jobOptions.py'

   j.backend = Remote()
   j.backend.host = "bluebear.bham.ac.uk"
   j.backend.username = "slatermw"
   j.backend.ganga_cmd = "/bb/projects/Ganga/runGanga"
   j.backend.ganga_dir = "/bb/phy/slatermw/gangadir/remote_jobs"   
   j.backend.environment = {'ATLAS_VERSION' : '13.0.40'}     # Additional environment variables
   j.backend.remote_backend = LCG()
   j.backend.remote_backend.CE = 'epgce2.ph.bham.ac.uk:2119/jobmanager-lcgpbs-short'

   j.submit()

   """
    
   _schema = Schema( Version( 1, 0 ), {\
      "remote_backend": ComponentItem('backends',doc='specification of the resources to be used (e.g. batch system)'),
      "host" : SimpleItem( defvalue=None, doc="The remote host to use" ),
      "username" : SimpleItem( defvalue=None, doc="The username at the remote host" ),
      "ganga_dir" : SimpleItem( defvalue="", doc="The directory to use for the remote workspace, repository, etc." ),
      "ganga_cmd" : SimpleItem( defvalue=None, doc="Command line to start ganga on the remote host" ),
      "environment" : SimpleItem( defvalue="", doc="Overides any environment variables set in the job" ),
      "pre_script" : SimpleItem( defvalue="", doc="Script to run on the remote site before running the submission script in Ganga" ),
      'remote_job_id' : SimpleItem(defvalue=0,protected=1,copyable=0,doc='Remote job id.'),
      'exitcode' : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Process exit code.'),
      'workdir' : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Working directory.'),
      'actualCE' : SimpleItem(defvalue=None,protected=1,copyable=0,doc='Hostname where the job was submitted.')
      } )

   _category = "backends"
   _name =  "Remote"
   _hidden = True # KUBA: temporarily disabled from the public
   _port = 22
   _transport = None
   _sftp = None
   _code = randomString()
   _transportarray = None
   
   def __init__( self ):
      super( Remote, self ).__init__()

   def __del__( self ):
      if (self._transport != None):
         self._transport.close()
         self._transport = None

   def setup( self ): # KUBA: generic setup hook
      job = self.getJobObject()
      if job.status in [ 'submitted', 'running', 'completing' ]:
         self.opentransport()
         
   def opentransport( self ):

      import paramiko
      import getpass
      
      if (self._transport != None):
         # transport is open
         return

      # check for a useable transport for this username and host
      if Remote._transportarray != None:
         for t in Remote._transportarray:
            if (t != None) and (t[0] == self.username) and (t[1] == self.host):
               self._transport = t[2]
               self._sftp = t[3]
               return
         
      # Ask user for password
      password = getpass.getpass('Password for %s@%s: ' % (self.username, self.host))
      
      try:
         self._transport = paramiko.Transport((self.host, self._port))
         self._transport.connect(username=self.username, password=password)
         channel = self._transport.open_session()
         channel.exec_command( 'mkdir -p ' + self.ganga_dir )
         self._sftp = paramiko.SFTPClient.from_transport(self._transport)

         # Add to the transport array
         Remote._transportarray = [Remote._transportarray,
                                   [self.username, self.host, self._transport, self._sftp]]
         
      except:
         print "Error when comunicating with remote host."
         self._transport = None
         self._sftp = None
      
      # blank the password just in case
      password = ""

      return
   
   def submit( self, jobconfig, master_input_sandbox ):
      """Submit the job to the remote backend.
                       
          Return value: True if job is submitted successfully,
                        or False otherwise"""

      import os
      import getpass
      
      # initiate the connection
      self.opentransport()
      if self._transport == None:
         return 0

      # Tar up the input sandbox and copy to the remote cluster
      job = self.getJobObject()
      subjob_input_sandbox = job.createPackedInputSandbox(jobconfig.getSandboxFiles())
      input_sandbox = subjob_input_sandbox + master_input_sandbox
      
      # send the sandbox
      sbx_name = '/__subjob_input_sbx__%s' % self._code
      self._sftp.put(subjob_input_sandbox[0], self.ganga_dir + sbx_name)
      sbx_name = '/__master_input_sbx__%s' % self._code
      self._sftp.put(master_input_sandbox[0], self.ganga_dir + sbx_name )
   
      # run the submit script on the remote cluster
      scriptpath = self.preparejob(jobconfig,master_input_sandbox)
      
      # send the script
      data = open(scriptpath, 'r').read()
      script_name = '/__jobscript_run__%s.py' % self._code
      self._sftp.open(self.ganga_dir + script_name, 'w').write(data)
      
      # run ganga command
      cmd = self.pre_script + " ; " + self.ganga_cmd
      cmd = self.ganga_cmd
      cmd += " -o\"[DefaultJobRepository]local_root=" + self.ganga_dir + "/repository\" "
      cmd += "-o\"[FileWorkspace]topdir=" + self.ganga_dir + "/workspace\" "
      cmd += self.ganga_dir + script_name
      channel = self._transport.open_session()
      channel.exec_command(cmd)
      
      # Read the output after command
      out = x = ""
      out2 = x2 = ""
      grid_ok = False

      while not channel.exit_status_ready():

         if channel.recv_ready():
            x = channel.recv(1024)
            out += x

         if channel.recv_stderr_ready():
            x2 = channel.recv_stderr(1024)
            out2 += x2

         if out.find("*** FINISHED ***") != -1:
            break

         if (x.find("GRID pass") != -1 or x2.find("GRID pass") != -1) :
            grid_ok = True
            password = getpass.getpass('Enter GRID pass phrase: ')
            channel.send( password + "\n" )
            password = ""

         x = x2 = ""
         
      if out.find("*** FINISHED ***") != -1:
         status, exitcode, outputdir, id = self.grabremoteinfo(out)
         
      self.remote_job_id = id

      # finally, delete the jobscript
      self._sftp.remove(self.ganga_dir + script_name)
            
      return 1
   
   def kill( self  ):
      """Kill running job.

         No arguments other than self

         Return value: True if job killed successfully,
                       or False otherwise"""

      # NOT IMPLEMENTED YET
      return False

   def grabremoteinfo( self, out ):
      
      from string import strip
      tok_list = out.split()
      
      idx = 0
      outputdir = ""
      status = "submitted"
      exitcode = 0
      id = -1
      injob = False
      
      for tok in tok_list:
         if tok == "Job":
            injob = True

         if injob:
            if tok == "status":
               status = strip(tok_list[idx+2], "'")
            
            if tok == "outputdir":
               outputdir = strip(tok_list[idx+2], "'")

            if tok == "id" and id == -1:
               id = eval(tok_list[idx+2])

         if tok == "backend":
            injob = False
            
         if tok == "exitcode":
            exitcode = eval(tok_list[idx+2])
         
         idx += 1

      return status, exitcode, outputdir, id

   def printinfo(self, job, obj_name):
      """Returns a string containing the user alterable properties of the object"""

      import os
      
      str = ''

      try:
         schema = job._schema
      except:
         return " = " + repr(job)

      str += " = " + schema.name + " (\n"

      # check for file object
      in_file = False
      if schema.name == "File":
         in_file = True
         
      for (name,item) in job._schema.simpleItems():
         if not item['protected']:
            if in_file and name == "name":
               filename = getattr(job, name)
               str += obj_name + name + " = " + repr(os.path.basename( filename )) + ", \n"
            else:
               str += obj_name + name + " = " + repr( getattr(job, name) ) + ", \n"

      for (name,item) in job._schema.componentItems():
         if not item['protected']:
            str += obj_name + name
            str += self.printinfo(getattr(job,name), obj_name + "   ")
               
            str += obj_name + ",\n"
         
      str += obj_name + ")"
         
      return str

   def preparejob( self, jobconfig, master_input_sandbox ):
      """Prepare the script to create the job on the remote host"""

      from Ganga.Utility import tempfile

      job = self.getJobObject()
      str = self.printinfo(job, " ")

      workdir = tempfile.mkdtemp()

      script = """#!/usr/bin/env python
#-----------------------------------------------------
# This job wrapper script is automatically created by
# GANGA Remote backend handler.
#
# It controls:
# 1. unpack input sandbox
# 2. create the new job
# 3. submit it
#-----------------------------------------------------
import os,os.path,shutil,tempfile
import sys,popen2,time,traceback
import tarfile

############################################################################################

###INLINEMODULES###

############################################################################################

j = Job()

output_sandbox = ###OUTPUTSANDBOX###
input_sandbox = ###INPUTSANDBOX###
appexec = ###APPLICATIONEXEC###
appargs = ###APPLICATIONARGS###
back_end ###BACKEND###
ganga_dir = ###GANGADIR###
code = ###CODE###
environment = ###ENVIRONMENT###
user_env = ###USERENV###

if user_env != None:
   for env_var in user_env:
      environment[env_var] = user_env[env_var]

j.outputsandbox = output_sandbox
j.application = Executable ( exe = appexec, args = appargs, env = environment )
j.backend = back_end

# Unpack the input sandboxes
shutil.move(os.path.expanduser(ganga_dir + "/__subjob_input_sbx__" + code), j.inputdir+"/__subjob_input_sbx__")
shutil.move(os.path.expanduser(ganga_dir + "/__master_input_sbx__" + code), j.inputdir+"/__master_input_sbx__")

# Add the files in the sandbox to the job
inputsbx = []
try:
   tar = tarfile.open(j.inputdir+"/__master_input_sbx__")
   filelist = tar.getnames()

   for f in filelist:
      inputsbx.append( j.inputdir + "/" + f )

except:
   print "Unable to open master input sandbox"
   
j.inputsandbox = inputsbx

getPackedInputSandbox(j.inputdir+"/__subjob_input_sbx__", j.inputdir + "/.")
getPackedInputSandbox(j.inputdir+"/__master_input_sbx__", j.inputdir + "/.")

# submit the job
j.submit()

print j

print "*** FINISHED ***"
"""
      import inspect
      import Ganga.Core.Sandbox as Sandbox
      script = script.replace('###ENVIRONMENT###', repr(jobconfig.env) )
      script = script.replace('###USERENV###', repr(self.environment) )
      script = script.replace('###INLINEMODULES###', inspect.getsource(Sandbox.WNSandbox))
      script = script.replace('###OUTPUTSANDBOX###', repr(jobconfig.outputbox))
      script = script.replace('###APPLICATIONEXEC###',repr(os.path.basename(jobconfig.getExeString())))
      script = script.replace('###APPLICATIONARGS###',repr(jobconfig.getArgStrings()))
      script = script.replace('###BACKEND###', self.printinfo( getattr( getattr(job, "backend" ), "remote_backend"), " "))
      script = script.replace('###GANGADIR###', repr(self.ganga_dir))
      script = script.replace('###CODE###', repr(self._code))
      
      sandbox_list = jobconfig.getSandboxFiles()

      str_list = "[ "
      for fname in sandbox_list:
         str_list += "j.inputdir + '/' + " + repr(os.path.basename( fname.name ))
         str_list += ", "

      str_list += "j.inputdir + '/__master_input_sbx__' ]"

      script = script.replace('###INPUTSANDBOX###', str_list)
      return job.getInputWorkspace().writefile(FileBuffer('__jobscript__.py',script),executable=0)

   def updateMonitoringInformation( jobs ):

      # Send a script over to the remote site that updates this jobs
      # info with the info of the remote job
      import os
      import getpass
      from string import strip

      for j in jobs:

         # Create a ganga script that updates the job info from the remote site
         script = """#!/usr/bin/env python
#-----------------------------------------------------
# This is a monitoring script for a remote job. It
# outputs some useful job info and exits
#-----------------------------------------------------
import os,os.path,shutil,tempfile
import sys,popen2,time,traceback

############################################################################################

###INLINEMODULES###

############################################################################################

code = ###CODE###
jid = ###JOBID###

j = jobs( jid )

runMonitoring()
print j

print "*** FINISHED ***"
"""

         script = script.replace('###CODE###', repr(j.backend._code))
         script = script.replace('###JOBID###', str(j.backend.remote_job_id))
         
         # check for the connection
         j.backend.opentransport()
         
         # send the script
         #data = open(scriptpath, 'r').read()
         script_name = '/__jobscript__%s.py' % j.backend._code
         j.backend._sftp.open(j.backend.ganga_dir + script_name, 'w').write(script)
         
         # run it remotely
         cmd = j.backend.ganga_cmd
         cmd += " -o\"[DefaultJobRepository]local_root=" + j.backend.ganga_dir + "/repository\" "
         cmd += "-o\"[FileWorkspace]topdir=" + j.backend.ganga_dir + "/workspace\" "
         cmd += j.backend.ganga_dir + script_name
         channel = j.backend._transport.open_session()
         channel.exec_command(cmd)

         # Read the output after command
         out = x = ""
         out2 = x2 = ""

         while not channel.exit_status_ready():

            if channel.recv_ready():
               x = channel.recv(1024)
               out += x

            if channel.recv_stderr_ready():
               x2 = channel.recv_stderr(1024)
               out2 += x2

            if out.find("*** FINISHED ***") != -1:
               break

            if (x.find("GRID pass") != -1 or x2.find("GRID pass") != -1) :
               grid_ok = True
               password = getpass.getpass('Enter GRID pass phrase: ')
               channel.send( password )
               password = ""

            x = x2 = ""

         if out.find("*** FINISHED ***") != -1:
            status, exitcode, outputdir, id = j.backend.grabremoteinfo(out)
            
         if status != j.status:
            j.updateStatus(status)
            
         j.backend.exitcode = exitcode
         
         # check for completed or failed and pull the output if required
         if j.status == 'completed' or j.status == 'failed':

            # we should have output, so get the file list first
            filelist = j.backend._sftp.listdir(outputdir)

            # go through and sftp them back
            for fname in filelist:
               data = j.backend._sftp.open(outputdir + '/' + fname, 'r').read()
               open(j.outputdir + '/' + os.path.basename(fname), 'w').write(data)


         # remove the script
         j.backend._sftp.remove(j.backend.ganga_dir + script_name)
         
      return None

   updateMonitoringInformation = \
      staticmethod( updateMonitoringInformation )
