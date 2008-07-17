################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Shell.py,v 1.1 2008-07-17 16:41:00 moscicki Exp $
################################################################################
#
# Shell wrapper with environment caching 
#
# Usage:
#
# 
# Initialisation: The shell script is sourced and the environment is captured
#
#     shell = Shell('/afs/cern.ch/project/gd/LCG-share/sl3/etc/profile.d/grid_env.sh')
#
# Output is returned in a file
# 
#     rc,outfile,m=shell.cmd('edg-get-job-status -all')
#
# Output is returned as a string
# 
#     rc,output,m=shell.cmd1('edg-get-job-status -all')
#
# Output is not captured. Useful for commands that require interactions
#
#     rc=shell.system('grid-proxy-init')
#
# Wrapper script is written for command including the setting of the
# environment first. Useful for situations where it is an external Python
# module that is calling command. It is callers responsibility to enter
# new location into PATH as this might have external effects.
#
#     fullpath=shell.wrapper('lcg-cp')

import os, re, tempfile

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()
from Ganga.Utility.Config import getConfig


class Shell:

   exceptions=getConfig('Shell')['IgnoredVars']

   def __init__(self,setup=None, setup_args=[]):
      "The setup script is sourced (with possible arguments) and the environment is captured"

      if setup:
         pipe=os.popen('source %s %s; printenv' % (setup," ".join(setup_args)))
         output=pipe.read()
         rc=pipe.close()
         if rc: logger.warning('Unexpected rc %d from setup command %s',rc,setup)

	 self.env=dict([[key,val] for key,val in re.findall('(\S+?)=(.*)\n',output) if key not in Shell.exceptions])
      else:
         self.env=os.environ

      self.dirname=None

   def cmd(self,cmd,soutfile=None,allowed_exit=[0], capture_stderr=False):
      "Execute an OS command and captures the stderr and stdout which are returned in a file"
 
      if not soutfile: soutfile=tempfile.mktemp('.out')
         
      logger.debug('Running shell command: %s' % cmd)
      try:
         rc=os.spawnve(os.P_WAIT,'/bin/sh',['/bin/sh','-c','%s &>%s' % (cmd,soutfile)],self.env)
      except OSError, (num,text):
         logger.warning( 'Problem with shell command: %s, %s', num,text)
         rc = 255
       
      if rc not in allowed_exit:
         logger.warning('exit status [%d] of command %s',rc,cmd)
         logger.warning('full output is in file: %s',soutfile)
         logger.warning('<first 255 bytes of output>\n%s',file(soutfile).read(255))
         logger.warning('<end of first 255 bytes of output>')
           
#FIXME /bin/sh might have also other error messages                                                                                            
      m = None
      if rc != 0:
         m = re.search('command not found\n',file(soutfile).read())
         if m: logger.warning('command %s not found',cmd)
                                                                                                       
      return rc,soutfile,m is None

   def cmd1(self,cmd,allowed_exit=[0],capture_stderr=False):
       "Executes an OS command and captures the stderr and stdout which are returned as a string"
       
       rc,outfile,m = self.cmd(cmd,None,allowed_exit)
       output=file(outfile).read()
       os.unlink(outfile)
       
       return rc,output, m
       
   def system(self,cmd,allowed_exit=[0], stderr_file=None):
      """Execute on OS command. Useful for interactive commands. Stdout and Stderr are not
      caputured and are passed on the caller.

      stderr_capture may specify a name of a file to which stderr is redirected.
      """

      logger.debug('Running shell command: %s' % cmd)

      if stderr_file:
         cmd += " 2> %s"%stderr_file
         
      try:
         rc = os.spawnve(os.P_WAIT,'/bin/sh',['/bin/sh','-c',cmd],self.env)
      except OSError, (num,text):
         logger.warning( 'Problem with shell command: %s, %s', num,text)
         rc = 255
      return rc

   def wrapper(self,cmd,preexecute=None):
      """Write wrapper script for command

      A wrapper around cmd is written including the setting of the environment.
      Useful for situations where it is an external Python module that is
      calling the command. It is callers responsibility to enter
      new location into PATH as this might have external effects. Full path of
      wrapper script is returned. Preexecute can contain extra commands to be
      executed before cmd

      fullpath = s.wrapper('lcg-cp', 'echo lcg-cp called with arguments $*'"""

      from Ganga.Utility.tempfile_compatibility import mkdtemp
      from os.path import join

      if not self.dirname:
         self.dirname=mkdtemp()
         
      fullpath = join(self.dirname,cmd)
      f = open(fullpath,'w')
      f.write("#!/bin/bash\n")
      for k,v in self.env.iteritems():
         f.write("export %s='%s'\n" % (k,v))
      if preexecute:
         f.write("%s\n" % preexecute)
      f.write("%s $*\n" % cmd)
      f.close()
      import stat,os
      os.chmod(fullpath,stat.S_IRWXU)

      return fullpath

#
#
# $Log: not supported by cvs2svn $
# Revision 1.5.12.4  2008/07/02 13:18:54  moscicki
# syntax error fix
#
# Revision 1.5.12.3  2008/07/01 14:45:03  moscicki
# fixes to support ARC (Nordu Grid)
#
# Revision 1.5.12.2  2008/02/21 12:09:41  amuraru
# bugfix #33685
#
# Revision 1.5.12.1  2007/12/10 19:19:59  amuraru
# merged changes from Ganga 4.4.4
#
# Revision 1.7  2007/10/23 14:45:58  amuraru
# fixed a bug in wrapper function to pass the command line arguments
#
# Revision 1.6  2007/10/15 14:16:57  amuraru
# [ulrik[ Added wrapper method to allow constructing a  wrapper around the command to
# include the setting of the environment. Useful for situations where it is an external
# Python module that is calling the command.
#
# Revision 1.5  2007/06/08 07:57:12  moscicki
# stderr capture option for Shell.system()
#
# Revision 1.4  2005/12/15 12:20:09  moscicki
# fix from uegede: OSError handling to avoid the waitpid problem
#
# Revision 1.3  2005/09/21 09:05:01  andrew
# removed unecessary redirection lines from the "system" method. Since the
# purpose of the system method is to be interactive, it does not make sense
# to capture std[in|out|err]
#
#
#

