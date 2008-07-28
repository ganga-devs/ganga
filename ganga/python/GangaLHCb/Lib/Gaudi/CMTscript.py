#######################################################################
# File: CMTscript.py
# Ganga Project. http://cern.ch/ganga
# Author: U. Egede
# Date: December 2005
# Purpose: Write a script containing CMT command which can subsequently
#          be executed.
# $Id: 
#######################################################################
"""
File: CMTscript.py
Purpose: Write a script containing CMT command which can subsequent
         be executed.
"""
__revision__ = 0.1

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

def CMTscript(app, command=''):
   """Function to execute a cmt command for a specific job

      Arguments:
         app       - The Gauid application object to take information from
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
   
   if not os.path.exists(tmpcmtdir):
      os.makedirs(tmpcmtdir) 
   reqfile = open(reqfname,'w')
   reqfile.write('use '+app.appname+' '+app.version+' '+app.package+'\n')
   if app.masterpackage:
      (pack, alg, ver) = app._parseMasterPackage()
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
   script+= str(app.appname) + " " + str(app.version) +"\n"
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

#
#
# $Log: not supported by cvs2svn $
# Revision 1.4.18.2  2008/06/13 08:50:29  uegede
# Updated Gaudi handler
# - To allow platform to be modified
# - To work with python style options
#
# Revision 1.4.18.1  2008/04/04 15:11:38  andrew
# Schema changes:
#   * make optsfile a list
#   * rename cmt_user_path to user_release_area
#   * rename cmt_release_area to lhcb_release_area
#
# Add type info to Gaudi schema
#
# Adapt code for schema changes
#
# Revision 1.4  2007/04/18 11:00:00  uegede
# The getpack, make and cmt commands for the Gaudi application updated to
# work with new install areas in LHCb. The commands still work in the old
# system as well.
# Bug fixed to make sure cmt_user_area is set to a sensible default value.
#
# Revision 1.3  2007/03/12 08:48:15  wreece
# Merge of the GangaLHCb-2-40 tag to head.
#
# Revision 1.2.2.2  2007/02/27 09:10:51  andrew
# Fixes for new style apps
#
# Revision 1.2.2.1  2006/12/22 14:33:00  uegede
# Fix to bug 22622 (missing output in cmt command) by taking quiet option away.
#
# Revision 1.2  2006/03/29 07:33:28  andrew
# Added fix for bug #15627
#
# Revision 1.1  2005/12/15 14:14:47  andrew
# Added the cmt and make functionality provided by Ulrik
#
#
