#!/usr/bin/env python

import sys
import time
import os


print sys.version

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Perform setup needed for using Ganga Public Interface (GPI)
# This is a Copy/Paste logic which must stay in THIS file

def standardSetup():
   """Function to perform standard setup for Ganga.
   """   
   import sys, os.path

   # insert the path to Ganga itself
   exeDir = os.path.abspath(os.path.normpath(os.path.dirname(sys.argv[0])))
   for i in range(2):
	   exeDir = os.path.dirname(exeDir)
   gangaDir = os.path.join(os.path.dirname(exeDir), 'python' )
   print gangaDir
   sys.path.insert(0, gangaDir)

   import Ganga.PACKAGE
   Ganga.PACKAGE.standardSetup()
   import GangaLHCb.PACKAGE
   GangaLHCb.PACKAGE.standardSetup()
   import GangaGUI.PACKAGE
   GangaGUI.PACKAGE.standardSetup()

standardSetup()
del standardSetup

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

if not os.environ.has_key('LHCB_BROWSER_GANGA_INTERNAL_PROCREEXEC'):
	os.environ['LHCB_BROWSER_GANGA_INTERNAL_PROCREEXEC'] = '1'
	prog = os.path.normpath(sys.argv[0])
	os.execv(prog,sys.argv)

		
print sys.path

import browser_mod
browser_mod.browse("standalone")

##########################################################################################################

# $Log: not supported by cvs2svn $
# Revision 1.12  2006/09/25 08:55:16  andrew
# Changed the default to the xmlrpc system
#
# Added a warning in case of an XMLRPC server error
#
# Changed the default number of records per query  from 10 to 300
#
# Changed the radio button into a checkbox for selecting the technology (AMGA vs XMLRPC)
#
