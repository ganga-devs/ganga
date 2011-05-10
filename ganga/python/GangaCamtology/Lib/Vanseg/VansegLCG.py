###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VansegLCG.py,v 1.1 2008-10-04 17:42:39 karl Exp $
###############################################################################
# File: VansegLCG.py
# Author: K. Harrison
# Created: 070126

"""Module containing class dealing with preparation of jobs to run
   Vanseg application on LCG backend"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "30 November 2007"
__version__ = "1.1"

from VansegLocal import VansegLocal
from Ganga.GPIDev.Lib.File import  File
from Ganga.Lib.LCG import LCGJobConfig
from Ganga.Lib.LCG import LCGRequirements
from Ganga.Utility import logging
from Ganga.Utility.files import fullpath

import os

logger = logging.getLogger()

class VansegLCG( VansegLocal ):

   def prepare( self, app, appsubconfig, appmasterconfig, jobmasterconfig ):

      local = VansegLocal.prepare\
         ( self, app, appsubconfig, appmasterconfig, jobmasterconfig )
      inbox = []
      inbox.extend( local.inputbox )
      exeFile = File( fullpath( app.exe ) )
      if not exeFile in inbox:
         inbox.append( exeFile )
      requirements = LCGRequirements()
      lcg = LCGJobConfig( exe = local.exe, inputbox = inbox,\
         outputbox = local.outputbox, requirements = requirements )
      return lcg

   def tail( self, job ):
      outbox = []
      gridUrl = os.path.join( job.outputdata.getGridStorage(), "images.tar.gz" )
      lineList = \
         [
         "",
         "cd ${WORKDIR}/images",
         "tar -zcf ${WORKDIR}/images.tar.gz *",
         "",
         "edg-gridftp-mkdir --parents %s" % os.path.dirname( gridUrl ),
         "globus-url-copy file:${WORKDIR}/images.tar.gz %s" % gridUrl,
         "",
         "echo 'End time: '`date`",
         ]
      return ( lineList, outbox )
