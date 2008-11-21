#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
"""Write a script containing CMT command which can subsequence be executed."""

__author__ = 'U. Egede'
__date__ = "$Date: 2008-11-21 11:28:41 $"
__revision__ = "$Revision: 1.5 $"

from Ganga.GPI import *
import Ganga.Utility.logging
import os
import sys
import time
import types
import warnings
import tempfile
import shutil
from Ganga.Utility.Shell import Shell
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def parse_master_package(mstrpckg):
   # first check if we have slashes
   if mstrpckg.find('/')>=0:
      try:
         list=mstrpckg.split('/')
         if len(list)==3:
            return list
         elif len(list)==2:
            list.insert(0,'')
            return list
         else:
            raise ValueError,"wrongly formatted masterpackage"
      except:
         pass
   elif mstrpckg.find(' ')>=0:
      try:
         list=mstrpckg.split()
         if len(list)==3:
            list = (list[2],list[0],list[1])
            return list
         elif len(list)==2:
            list=('',list[0],list[1])
            return list
         else:
            raise ValueError,"wrongly formatted masterpackage"
      except:
         pass
   else:
      raise ValueError,"wrongly formatted masterpackage"

def CMTscript(app,command=''):
   """Function to execute a cmt command for a specific job

      Arguments:
         app       - The Gaudi application object to take information from
         command   - String [default ''] The cmt command to execute.
   """
   cmtroot = os.getenv('CMTROOT')
   cmtbin  = os.getenv('CMTBIN')
   cmtcmd  = 'cmt'
   if cmtbin and cmtroot :
      cmtcmd = cmtroot + os.sep + cmtbin + os.sep + 'cmt'

   warnings.filterwarnings('ignore', 'tempnam', RuntimeWarning)
   tmppath   = tempfile.mktemp()
   tmpcmtdir = os.path.join(tmppath,'cmttemp','v1','cmt')
   reqfname  = os.path.join(tmpcmtdir,'requirements')

   appname = app.appname
   if not os.path.exists(tmpcmtdir):
      os.makedirs(tmpcmtdir) 
   reqfile = open(reqfname,'w')
   reqfile.write('use '+appname+' '+app.version+' '+app.package+'\n')
   if app.masterpackage:
      (pack, alg, ver) = parse_master_package(app.masterpackage)
      reqfile.write('use '+alg+' '+ver+' '+pack+'\n')
   reqfile.close()

   cmtoption = '-pack=cmttemp -version=v1 -path='+ tmppath

   # generate shell script
   script='#!/bin/sh\n'
   script+='unalias -a\n'
   script+='unset CMTPROJECTPATH\n'
   script+='export CMTCONFIG='+str(app.platform)+'\n' 
   script+='export User_release_area='+str(app.user_release_area)+'\n'
   script+='. $LHCBHOME/scripts/setenvProject.sh '
   script+= '%s %s %s\n' % (app.setupProjectOptions,app.appname,app.version)
   script+='[ x$CMTPATH == x ] || cd ' + str(app.user_release_area) + '\n'
   script+='pwd\n'
   command=command.replace('###CMT###',cmtcmd + ' ' + cmtoption)
   logger.debug('Will execute the command: '+command)

   script += command + '\n'

   logger.debug('The full script for execution:\n'+script)

   # write file
   try:
      fn = os.path.join(tmppath, 'cmtcommand_script')
      file1 = open(fn, 'w')
   except Exception, e:
      logger.error("Can not create temporary file %s", fn)
      return
   else:
      try:
         file1.write(script)
      finally:
         file1.close()

   # make file executable
   os.chmod(fn, 0777)

   shell = Shell()  
   rc=shell.system(fn)

   if os.path.exists(tmppath) :
      shutil.rmtree(tmppath)

   return True

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
