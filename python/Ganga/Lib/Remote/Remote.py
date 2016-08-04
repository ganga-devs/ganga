"""Module containing class for handling job submission to Remote backend"""

__author__ = "Mark Slater <mws@hep.ph.bham.ac.uk>"
__date__ = "10 June 2008"
__version__ = "1.0"

from Ganga.Core import Sandbox
from Ganga.GPIDev.Adapters.IBackend import IBackend
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import inspect
import os
from Ganga.Lib.Root import randomString


def shutdown_transport(tr):
    """Shutdown the transport cleanly - otherwise python 2.5 throws a wobble"""
    if (tr != None):
        tr.close()
        tr = None


class Remote(IBackend):

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
    just the remote machine

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

    E.g. 4 - Hello World submitted at CERN on LSF using atlas startup

    j = Job()
    j.backend = Remote()
    j.backend.host = "lxplus.cern.ch"
    j.backend.username = "mslater"
    j.backend.ganga_cmd = "ganga"
    j.backend.ganga_dir = "/afs/cern.ch/user/m/mslater/gangadir/remote_jobs"
    j.backend.pre_script = ['source /afs/cern.ch/sw/ganga/install/etc/setup-atlas.csh'] # source the atlas setup script before running ganga
    j.backend.remote_backend = LSF()
    j.submit()

    """

    _schema = Schema(Version(1, 0), {
        "remote_backend": ComponentItem('backends', doc='specification of the resources to be used (e.g. batch system)'),
        "host": SimpleItem(defvalue="", doc="The remote host and port number ('host:port') to use. Default port is 22."),
        "ssh_key": SimpleItem(defvalue="", doc="Set to true to the location of the the ssh key to use for authentication, e.g. /home/mws/.ssh/id_rsa. Note, you should make sure 'key_type' is also set correctly."),
        "key_type": SimpleItem(defvalue="RSA", doc="Set to the type of ssh key to use (if required). Possible values are 'RSA' and 'DSS'."),
        "username": SimpleItem(defvalue="", doc="The username at the remote host"),
        "ganga_dir": SimpleItem(defvalue="", doc="The directory to use for the remote workspace, repository, etc."),
        "ganga_cmd": SimpleItem(defvalue="", doc="Command line to start ganga on the remote host"),
        "environment": SimpleItem(defvalue={}, doc="Overides any environment variables set in the job"),
        "pre_script": SimpleItem(defvalue=[''], doc="Sequence of commands to execute before running Ganga on the remote site"),
        'remote_job_id': SimpleItem(defvalue=0, protected=1, copyable=0, doc='Remote job id.'),
        'exitcode': SimpleItem(defvalue=0, protected=1, copyable=0, doc='Application exit code'),
        'actualCE': SimpleItem(defvalue=0, protected=1, copyable=0, doc='Computing Element where the job actually runs.')
    })

    _category = "backends"
    _name = "Remote"
    #_hidden = False # KUBA: temporarily disabled from the public
    _port = 22
    _transport = None
    _sftp = None
    _code = randomString()
    _transportarray = None
    _key = {}

    _exportmethods = ['setup']

    def __init__(self):
        super(Remote, self).__init__()

    def __del__(self):
        if (self._transport != None):
            self._transport.close()
            self._transport = None

    def setup(self):  # KUBA: generic setup hook
        job = self.getJobObject()
        if job.status in ['submitted', 'running', 'completing']:

            # Send a script over to the remote site that updates this jobs
            # info with the info of the remote job
            import os

            # Create a ganga script that updates the job info from the remote
            # site
            script = """#!/usr/bin/env python
from __future__ import print_function
#-----------------------------------------------------
# This is a setup script for a remote job. It
# does very litte
#-----------------------------------------------------

# print a finished token
print("***_FINISHED_***")
"""

            # check for the connection
            if (self.opentransport() == False):
                return False

            # send the script
            #script_name = '/__setupscript__%s.py' % self._code
            #self._sftp.open(self.ganga_dir + script_name, 'w').write(script)

            # run the script
            #stdout, stderr = self.run_remote_script( script_name, self.pre_script )

            # remove the script
            #self._sftp.remove(self.ganga_dir + script_name)

        return True

    def opentransport(self):

        import paramiko
        import getpass
        import atexit

        if (self._transport != None):
            # transport is open
            return

        # check for a useable transport for this username and host
        if Remote._transportarray != None:
            for t in Remote._transportarray:
                if (t != None) and (t[0] == self.username) and (t[1] == self.host):

                    # check for too many retries on the same host
                    if t[2] == None or t[3] == None:
                        logger.warning("Too many retries for remote host " + self.username +
                                       "@" + self.host + ". Restart Ganga to have another go.")
                        return False

                    self._transport = t[2]
                    self._sftp = t[3]

                    # ensure that the remote dir is still there - it will crash if the dir structure
                    # changes with the sftp sill open
                    channel = self._transport.open_session()
                    channel.exec_command('mkdir -p ' + self.ganga_dir)
                    bufout = ""
                    while not channel.exit_status_ready():
                        if channel.recv_ready():
                            bufout = channel.recv(1024)

                    return

        # Ask user for password - give three tries
        num_try = 0
        password = ""
        while num_try < 3:

            try:
                temp_host = self.host
                temp_port = self._port
                if self.host.find(":") != -1:
                    # user specified port
                    temp_port = eval(self.host[self.host.find(":") + 1:])
                    temp_host = self.host[: self.host.find(":")]

                self._transport = paramiko.Transport((temp_host, temp_port))

                # avoid hang on exit my daemonising the thread
                self._transport.setDaemon(True)

                # register for proper shutdown
                atexit.register(shutdown_transport, self._transport)

                if self.ssh_key != "" and os.path.exists(os.path.expanduser(os.path.expandvars(self.ssh_key))):
                    privatekeyfile = os.path.expanduser(
                        os.path.expandvars(self.ssh_key))

                    if self.ssh_key not in Remote._key:

                        if self.key_type == "RSA":
                            password = getpass.getpass(
                                'Enter passphrase for key \'%s\': ' % (self.ssh_key))
                            Remote._key[self.ssh_key] = paramiko.RSAKey.from_private_key_file(
                                privatekeyfile, password=password)
                        elif self.key_type == "DSS":
                            password = getpass.getpass(
                                'Enter passphrase for key \'%s\': ' % (self.ssh_key))
                            Remote._key[self.ssh_key] = paramiko.DSSKey.from_private_key_file(
                                privatekeyfile, password=password)
                        else:
                            logger.error(
                                "Unknown ssh key_type '%s'. Unable to connect." % self.key_type)
                            return False

                    self._transport.connect(
                        username=self.username, pkey=Remote._key[self.ssh_key])
                else:
                    logger.debug("SSH key: %s" % self.ssh_key)
                    if os.path.exists(os.path.expanduser(os.path.expandvars(self.ssh_key))):
                        logger.debug(
                            "PATH: %s Exists" % os.path.expanduser(os.path.expandvars(self.ssh_key)))
                    else:
                        logger.debug("PATH: %s Does NOT Exist" % os.path.expanduser(
                            os.path.expandvars(self.ssh_key)))

                    if self.username != "" and self.host != "":
                        password = getpass.getpass(
                            'Password for %s@%s: ' % (self.username, self.host))
                        self._transport.connect(
                            username=self.username, password=password)
                    elif self.username == "":
                        logger.error("ERROR: USERNAME NOT DEFINED!!!")
                        return False
                    elif self.host == "":
                        logger.error("ERROR: HOSTNAME NOT DEFINED!!!")
                        return False
                    else:
                        pass

                # blank the password just in case
                password = "                                                "

                channel = self._transport.open_session()
                channel.exec_command('mkdir -p ' + self.ganga_dir)
                self._sftp = paramiko.SFTPClient.from_transport(
                    self._transport)

                # Add to the transport array
                Remote._transportarray = [Remote._transportarray,
                                          [self.username, self.host, self._transport, self._sftp]]
                num_try = 1000

            except Exception as err:
                logger.debug("Err: %s" %str(err))
                logger.warning("Error when comunicating with remote host. Retrying...")
                self._transport = None
                self._sftp = None
                if self.ssh_key in Remote._key:
                    del Remote._key[self.ssh_key]

            num_try = num_try + 1

        if num_try == 3:
            logger.error("Could not logon to remote host " + self.username + "@" +
                         self.host + " after three attempts. Restart Ganga to have another go.")
            Remote._transportarray = [Remote._transportarray,
                                      [self.username, self.host, None, None]]
            return False

        return True

    def run_remote_script(self, script_name, pre_script):
        """Run a ganga script on the remote site"""

        import getpass

        # Set up a command file to source. This gets around a silly alias
        # problem
        cmd_str = ""
        for c in pre_script:
            cmd_str += c + '\n'

        cmd_str += self.ganga_cmd + \
            " -o\'[Configuration]gangadir=" + self.ganga_dir + "\' "
        cmd_str += self.ganga_dir + script_name + '\n'
        cmd_file = os.path.join(
            self.ganga_dir, "__gangacmd__" + randomString())
        self._sftp.open(cmd_file, 'w').write(cmd_str)

        # run ganga command
        channel = self._transport.open_session()
        channel.exec_command("source " + cmd_file)

        # Read the output after command
        stdout = bufout = ""
        stderr = buferr = ""
        grid_ok = False

        while not channel.exit_status_ready():

            if channel.recv_ready():
                bufout = channel.recv(1024)
                stdout += bufout

            if channel.recv_stderr_ready():
                buferr = channel.recv_stderr(1024)
                stderr += buferr

            if stdout.find("***_FINISHED_***") != -1:
                break

            if (bufout.find("GRID pass") != -1 or buferr.find("GRID pass") != -1):
                grid_ok = True
                password = getpass.getpass('Enter GRID pass phrase: ')
                channel.send(password + "\n")
                password = ""

            bufout = buferr = ""

        self._sftp.remove(cmd_file)

        return stdout, stderr

    def submit(self, jobconfig, master_input_sandbox):
        """Submit the job to the remote backend.

            Return value: True if job is submitted successfully,
                          or False otherwise"""

        import os
        import getpass

        # First some sanity checks...
        fail = 0
        if self.remote_backend == None:
            logger.error("No backend specified for remote host.")
            fail = 1
        if self.host == "":
            logger.error("No remote host specified.")
            fail = 1
        if self.username == "":
            logger.error("No username specified.")
            fail = 1
        if self.ganga_dir == "":
            logger.error("No remote ganga directory specified.")
            fail = 1
        if self.ganga_cmd == "":
            logger.error("No ganga command specified.")
            fail = 1

        if fail:
            return 0

        # initiate the connection
        if self.opentransport() == False:
            return 0

        # Tar up the input sandbox and copy to the remote cluster
        job = self.getJobObject()
        subjob_input_sandbox = job.createPackedInputSandbox(
            jobconfig.getSandboxFiles())
        input_sandbox = subjob_input_sandbox + master_input_sandbox

        # send the sandbox
        sbx_name = '/__subjob_input_sbx__%s' % self._code
        self._sftp.put(subjob_input_sandbox[0], self.ganga_dir + sbx_name)
        sbx_name = '/__master_input_sbx__%s' % self._code
        self._sftp.put(master_input_sandbox[0], self.ganga_dir + sbx_name)

        # run the submit script on the remote cluster
        scriptpath = self.preparejob(jobconfig, master_input_sandbox)

        # send the script
        data = open(scriptpath, 'r').read()
        script_name = '/__jobscript_run__%s.py' % self._code
        self._sftp.open(self.ganga_dir + script_name, 'w').write(data)

        # run the script
        stdout, stderr = self.run_remote_script(script_name, self.pre_script)

        # delete the jobscript
        self._sftp.remove(self.ganga_dir + script_name)

        # Copy the job object
        if stdout.find("***_FINISHED_***") != -1:
            status, outputdir, id, be = self.grabremoteinfo(stdout)

            self.remote_job_id = id
            if hasattr(self.remote_backend, 'exitcode'):
                self.exitcode = be.exitcode
            if hasattr(self.remote_backend, 'actualCE'):
                self.actualCE = be.actualCE

            # copy each variable in the schema
            # Please can someone tell me why I can't just do
            # self.remote_backend = be?
            for o in be._schema.allItems():
                exec("self.remote_backend." + o[0] + " = be." + o[0])

            return 1
        else:
            logger.error("Problem submitting the job on the remote site.")
            logger.error("<last 1536 bytes of stderr>")
            cut = stderr[len(stderr) - 1536:]

            for ln in cut.splitlines():
                logger.error(ln)

            logger.error("<end of last 1536 bytes of stderr>")

        return 0

    def kill(self):
        """Kill running job.

           No arguments other than self

           Return value: True if job killed successfully,
                         or False otherwise"""

        script = """#!/usr/bin/env python
from __future__ import print_function
#-----------------------------------------------------
# This is a kill script for a remote job. It
# attempts to kill the given job and returns
#-----------------------------------------------------
import os,os.path,shutil,tempfile
import sys,popen2,time,traceback

############################################################################################

###INLINEMODULES###

############################################################################################

code = ###CODE###
jid = ###JOBID###

j = jobs( jid )
j.kill()

# Start pickle token
print("***_START_PICKLE_***")

# pickle the job
import pickle
print(j.outputdir)
print(pickle.dumps(j._impl))
print(j)

# print a finished token
print("***_END_PICKLE_***")
print("***_FINISHED_***")
"""

        script = script.replace('###CODE###', repr(self._code))
        script = script.replace('###JOBID###', str(self.remote_job_id))

        # check for the connection
        if (self.opentransport() == False):
            return 0

        # send the script
        script_name = '/__jobscript_kill__%s.py' % self._code
        self._sftp.open(self.ganga_dir + script_name, 'w').write(script)

        # run the script
        stdout, stderr = self.run_remote_script(script_name, self.pre_script)

        # Copy the job object
        if stdout.find("***_FINISHED_***") != -1:
            status, outputdir, id, be = self.grabremoteinfo(stdout)

            if status == 'killed':
                return True

        return False

    def remove(self):
        """Remove the selected job from the remote site

           No arguments other than self

           Return value: True if job removed successfully,
                         or False otherwise"""

        script = """#!/usr/bin/env python
from __future__ import print_function
#-----------------------------------------------------
# This is a remove script for a remote job. It
# attempts to kill the given job and returns
#-----------------------------------------------------
import os,os.path,shutil,tempfile
import sys,popen2,time,traceback

############################################################################################

###INLINEMODULES###

############################################################################################

code = ###CODE###
jid = ###JOBID###

j = jobs( jid )
j.remove()

jobs( jid )

# print a finished token
print("***_FINISHED_***")
"""

        script = script.replace('###CODE###', repr(self._code))
        script = script.replace('###JOBID###', str(self.remote_job_id))

        # check for the connection
        if (self.opentransport() == False):
            return 0

        # send the script
        script_name = '/__jobscript_remove__%s.py' % self._code
        self._sftp.open(self.ganga_dir + script_name, 'w').write(script)

        # run the script
        stdout, stderr = self.run_remote_script(script_name, self.pre_script)

        # Copy the job object
        if stdout.find("***_FINISHED_***") != -1:
            return True

        return False

    def resubmit(self):
        """Resubmit the job.

           No arguments other than self

           Return value: 1 if job was resubmitted,
                         or 0 otherwise"""

        script = """#!/usr/bin/env python
from __future__ import print_function
#-----------------------------------------------------
# This is a resubmit script for a remote job. It
# attempts to kill the given job and returns
#-----------------------------------------------------
import os,os.path,shutil,tempfile
import sys,popen2,time,traceback

############################################################################################

###INLINEMODULES###

############################################################################################

code = ###CODE###
jid = ###JOBID###

j = jobs( jid )
j.resubmit()

# Start pickle token
print("***_START_PICKLE_***")

# pickle the job
import pickle
print(j.outputdir)
print(pickle.dumps(j._impl))
print(j)

# print a finished token
print("***_END_PICKLE_***")
print("***_FINISHED_***")
"""

        script = script.replace('###CODE###', repr(self._code))
        script = script.replace('###JOBID###', str(self.remote_job_id))

        # check for the connection
        if (self.opentransport() == False):
            return 0

        # send the script
        script_name = '/__jobscript_resubmit__%s.py' % self._code
        self._sftp.open(self.ganga_dir + script_name, 'w').write(script)

        # run the script
        stdout, stderr = self.run_remote_script(script_name, self.pre_script)

        # Copy the job object
        if stdout.find("***_FINISHED_***") != -1:
            status, outputdir, id, be = self.grabremoteinfo(stdout)

            if status == 'submitted' or status == 'running':
                return 1

        return 0

    def grabremoteinfo(self, out):

        import pickle

        # Find the start and end of the pickle
        start = out.find("***_START_PICKLE_***") + len("***_START_PICKLE_***")
        stop = out.find("***_END_PICKLE_***")
        outputdir = out[start + 1:out.find("\n", start + 1) - 1]
        pickle_str = out[out.find("\n", start + 1) + 1:stop]

        # Now unpickle and recreate the job
        j = pickle.loads(pickle_str)

        return j.status, outputdir, j.id, j.backend

    def preparejob(self, jobconfig, master_input_sandbox):
        """Prepare the script to create the job on the remote host"""

        import tempfile

        workdir = tempfile.mkdtemp()
        job = self.getJobObject()

        script = """#!/usr/bin/env python
from __future__ import print_function
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
back_end = ###BACKEND###
ganga_dir = ###GANGADIR###
code = ###CODE###
environment = ###ENVIRONMENT###
user_env = ###USERENV###

if user_env != None:
   for env_var in user_env:
      environment[env_var] = user_env[env_var]

j.outputsandbox = output_sandbox
j.backend = back_end

# Unpack the input sandboxes
shutil.move(os.path.expanduser(ganga_dir + "/__subjob_input_sbx__" + code), j.inputdir+"/__subjob_input_sbx__")
shutil.move(os.path.expanduser(ganga_dir + "/__master_input_sbx__" + code), j.inputdir+"/__master_input_sbx__")

# Add the files in the sandbox to the job
inputsbx = []
fullsbxlist = []
try:
   tar = tarfile.open(j.inputdir+"/__master_input_sbx__")
   filelist = tar.getnames()
   print(filelist)
   
   for f in filelist:
      fullsbxlist.append( f )
      inputsbx.append( j.inputdir + "/" + f )

except:
   print("Unable to open master input sandbox")

try:
   tar = tarfile.open(j.inputdir+"/__subjob_input_sbx__")
   filelist = tar.getnames()

   for f in filelist:
      fullsbxlist.append( f )
      inputsbx.append( j.inputdir + "/" + f )

except:
   print("Unable to open subjob input sandbox")

# sort out the path of the exe
if appexec in fullsbxlist:
   j.application = Executable ( exe = File(os.path.join(j.inputdir, appexec)), args = appargs, env = environment )
   print("Script found: %s" % appexec)
else:
   j.application = Executable ( exe = appexec, args = appargs, env = environment )

   
j.inputsandbox = inputsbx

getPackedInputSandbox(j.inputdir+"/__subjob_input_sbx__", j.inputdir + "/.")
getPackedInputSandbox(j.inputdir+"/__master_input_sbx__", j.inputdir + "/.")

# submit the job
j.submit()

# Start pickle token
print("***_START_PICKLE_***")

# pickle the job
import pickle
print(j.outputdir)
print(pickle.dumps(j._impl))

# print a finished token
print("***_END_PICKLE_***")
print("***_FINISHED_***")
"""
        import inspect
        import Ganga.Core.Sandbox as Sandbox
        script = script.replace('###ENVIRONMENT###', repr(jobconfig.env))
        script = script.replace('###USERENV###', repr(self.environment))
        script = script.replace(
            '###INLINEMODULES###', inspect.getsource(Sandbox.WNSandbox))
        script = script.replace(
            '###OUTPUTSANDBOX###', repr(jobconfig.outputbox))
        script = script.replace(
            '###APPLICATIONEXEC###', repr(os.path.basename(jobconfig.getExeString())))
        script = script.replace(
            '###APPLICATIONARGS###', repr(jobconfig.getArgStrings()))

        # get a string describing the required backend
        import cStringIO
        be_out = cStringIO.StringIO()
        job.backend.remote_backend.printTree(be_out, "copyable")
        be_str = be_out.getvalue()
        script = script.replace('###BACKEND###', be_str)

        script = script.replace('###GANGADIR###', repr(self.ganga_dir))
        script = script.replace('###CODE###', repr(self._code))

        sandbox_list = jobconfig.getSandboxFiles()

        str_list = "[ "
        for fname in sandbox_list:
            str_list += "j.inputdir + '/' + " + \
                repr(os.path.basename(fname.name))
            str_list += ", "

        str_list += "j.inputdir + '/__master_input_sbx__' ]"

        script = script.replace('###INPUTSANDBOX###', str_list)
        return job.getInputWorkspace().writefile(FileBuffer('__jobscript__.py', script), executable=0)

    @staticmethod
    def updateMonitoringInformation(jobs):

        # Send a script over to the remote site that updates this jobs
        # info with the info of the remote job
        import os
        import getpass

        # first, loop over the jobs and sort by host, username, gangadir and
        # pre_script
        jobs_sort = {}
        for j in jobs:
            host_str = j.backend.username + "@" + j.backend.host + ":" + \
                j.backend.ganga_dir + "+" + ';'.join(j.backend.pre_script)
            if host_str not in jobs_sort:
                jobs_sort[host_str] = []

            jobs_sort[host_str].append(j)

        for host_str in jobs_sort:
            # Create a ganga script that updates the job info for all jobs at
            # this remote site
            script = """#!/usr/bin/env python
from __future__ import print_function
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
jids = ###JOBID###

runMonitoring()

import pickle

for jid in jids:

    j = jobs( jid )

    # Start pickle token
    print("***_START_PICKLE_***")

    # pickle the job
    print(j.outputdir)
    print(pickle.dumps(j._impl))
    print(j)

    # print a finished token
    print("***_END_PICKLE_***")

print("***_FINISHED_***")
"""

            mj = jobs_sort[host_str][0]
            script = script.replace('###CODE###', repr(mj.backend._code))
            rem_ids = []
            for j in jobs_sort[host_str]:
                rem_ids.append(j.backend.remote_job_id)
            script = script.replace('###JOBID###', str(rem_ids))

            # check for the connection
            if (mj.backend.opentransport() == False):
                return 0

            # send the script
            script_name = '/__jobscript__%s.py' % mj.backend._code
            mj.backend._sftp.open(
                mj.backend.ganga_dir + script_name, 'w').write(script)

            # run the script
            stdout, stderr = mj.backend.run_remote_script(
                script_name, mj.backend.pre_script)

            # Copy the job object
            if stdout.find("***_FINISHED_***") != -1:

                start_pos = stdout.find("***_START_PICKLE_***")
                end_pos = stdout.find(
                    "***_END_PICKLE_***") + len("***_END_PICKLE_***")

                while start_pos != -1 and end_pos != -1:
                    pickle_str = stdout[start_pos:end_pos + 1]

                    status, outputdir, id, be = mj.backend.grabremoteinfo(
                        pickle_str)

                    # find the job and update it
                    found = False
                    for j in jobs_sort[host_str]:

                        if (id == j.backend.remote_job_id):
                            found = True
                            if status != j.status:
                                j.updateStatus(status)

                            if hasattr(j.backend.remote_backend, 'exitcode'):
                                j.backend.exitcode = be.exitcode
                            if hasattr(j.backend.remote_backend, 'actualCE'):
                                j.backend.actualCE = be.actualCE

                            for o in be._schema.allItems():
                                exec(
                                    "j.backend.remote_backend." + o[0] + " = be." + o[0])

                            # check for completed or failed and pull the output
                            # if required
                            if j.status == 'completed' or j.status == 'failed':

                                # we should have output, so get the file list
                                # first
                                filelist = j.backend._sftp.listdir(outputdir)

                                # go through and sftp them back
                                for fname in filelist:
                                    data = j.backend._sftp.open(
                                        outputdir + '/' + fname, 'r').read()
                                    open(
                                        j.outputdir + '/' + os.path.basename(fname), 'w').write(data)

                    if not found:
                        logger.warning(
                            "Couldn't match remote id %d with monitored job. Serious problems in Remote monitoring." % id)

                    start_pos = stdout.find("***_START_PICKLE_***", end_pos)
                    end_pos = stdout.find(
                        "***_END_PICKLE_***", end_pos) + len("***_END_PICKLE_***")

            # remove the script
            j.backend._sftp.remove(j.backend.ganga_dir + script_name)

        return None

