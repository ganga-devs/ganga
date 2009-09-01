################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Shell.py,v 1.7 2009-06-26 11:35:09 moscicki Exp $
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
      
      """The setup script is sourced (with possible arguments) and the
      environment is captured. The environment variables are expanded
      automatically (this is a fix for bug #44259: GangaLHCb tests fail due to
      gridProxy check).

      Example of variable expansion:
      
      os.environ['BAR'] = 'rabarbar'
      os.environ['FOO'] = '$BAR'
      s = Shell() # with or without the setup script
      assert s.env['FOO'] == 'rabarbar' # NOT literal string '$BAR'

      NOTE: the behaviour is not 100% bash compatible: undefined variables in
      bash are empty strings, Shell() leaves the literals unchanged,so:

      os.environ['FOO'] = '$NO_BAR'
      s = Shell()
      if os.environ.not has_key('NO_BAR'):
         assert s.env['FOO'] == '$NO_BAR'
         
      """

      def expand_vars(env):
         tmp_dict = {}
         for k,v in env.iteritems():
            tmp_dict[k] = os.path.expandvars(v)
         return tmp_dict

      if setup:
         pipe=os.popen('source %s %s > /dev/null 2>&1; python -c "import os; print os.environ"' % (setup," ".join(setup_args)))
         output=pipe.read()
         rc=pipe.close()
         if rc: logger.warning('Unexpected rc %d from setup command %s',rc,setup)

         env = expand_vars(eval(output))

         for key in Shell.exceptions:
            try:
               del env[key]
            except KeyError:
               pass
         self.env = env
      else:
         env=dict(os.environ) #bug #44334: Ganga/Utility/Shell.py does not save environ
         self.env = expand_vars(env)

      self.dirname=None

   def cmd(self,cmd,soutfile=None,allowed_exit=[0], capture_stderr=False):
      "Execute an OS command and captures the stderr and stdout which are returned in a file"
 
      if not soutfile: soutfile=tempfile.mktemp('.out')
         
      logger.debug('Running shell command: %s' % cmd)
      try:
         rc=os.spawnve(os.P_WAIT,'/bin/sh',['/bin/sh','-c','%s > %s 2>&1' % (cmd,soutfile)],self.env)
      except OSError, (num,text):
         logger.warning( 'Problem with shell command: %s, %s', num,text)
         rc = 255
      
      BYTES = 4096
      if rc not in allowed_exit:
         logger.warning('exit status [%d] of command %s',rc,cmd)
         logger.warning('full output is in file: %s',soutfile)
         logger.warning('<first %d bytes of output>\n%s',BYTES,file(soutfile).read(BYTES))
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
# Revision 1.6  2008/11/27 15:06:08  moscicki
# bug #44393: wrong redirection of stdout/stderr in Shell
#
# Revision 1.5  2008/11/21 15:42:03  moscicki
# bug #44259: GangaLHCb tests fail due to gridProxy check
#
# Revision 1.4  2008/11/21 14:03:36  moscicki
# bug #44334: Ganga/Utility/Shell.py does not save environ
#
# Revision 1.3  2008/11/07 12:26:12  moscicki
# fixed Shell in case the setup script produces garbage on stdout (now discarded to /dev/null)
#
# Revision 1.2  2008/10/24 06:42:14  moscicki
# bugfix #40932: Ganga incompatible with shell functions (using os.environ directly instead of printenv)
#
# Revision 1.1  2008/07/17 16:41:00  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
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

