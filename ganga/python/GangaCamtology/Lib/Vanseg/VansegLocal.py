###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: VansegLocal.py,v 1.1 2008-10-04 17:42:39 karl Exp $
###############################################################################
# File: VansegLocal.py
# Author: K. Harrison
# Created: 070123

"""Module containing class dealing with preparation of jobs to run
   Vanseg application on Local backend"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "30 November 2007"
__version__ = "1.2"

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.GPIDev.Lib.File import  File
from Ganga.GPIDev.Lib.File import  FileBuffer
from Ganga.Utility import logging
from Ganga.Utility.files import fullpath

import os
import time

logger = logging.getLogger()

class VansegLocal( IRuntimeHandler ):

   def prepare( self, app, appsubconfig, appmasterconfig, jobmasterconfig ):

      job = app.getJobObject()
      exePath = appsubconfig[ "exePath" ]
      outDir = appsubconfig[ "outDir" ]
      imageDir = appsubconfig[ "imageDir" ]
      elementList = imageDir.split( os.sep )
      imageSubdir = \
         os.sep.join( elementList[ elementList.index( "images" ) : ] )
      urlRoot = \
         os.path.join( "http://hovercraft.hep.phy.cam.ac.uk", imageSubdir )
      imageList = appsubconfig[ "imageList" ]

      lineList = []
      outbox = []

      headList, headBox = self.head( job = job, exePath = exePath )
      lineList.extend( headList )
      outbox.extend( headBox )

      bodyList, bodyBox = self.body( job = job, imageDir = imageDir, \
         urlRoot = urlRoot, imageList = imageList )
      lineList.extend( bodyList )
      outbox.extend( bodyBox )

      tailList, tailBox = self.tail( job = job )
      lineList.extend( tailList )
      outbox.extend( tailBox )

      jobScript = "\n".join( lineList )
      jobWrapper = FileBuffer( "VansegLocal.sh", jobScript, executable = 1 )

      outbox.extend( job.outputsandbox )

      return StandardJobConfig\
         ( exe = jobWrapper, outputbox = outbox )

   def head( self, job = None, exePath = "" ):
      outbox = []
      lineList = \
         [
            "#!/bin/bash",
            "",
            "# Run script for Vanseg",
            "# Created by Ganga - %s" % ( time.strftime( "%c" ) ),
            "",
            "echo 'Job running on '`hostname -f`",
            "echo 'Start time: '`date`",
            "",
            "WORKDIR=`pwd`",
            "VANSEG=''",
            "if [ -f %s ]; then" % exePath,
            "   VANSEG=%s" % exePath,
            "elif [ -f ${WORKDIR}/%s ]; then" % os.path.basename( exePath ),
            "   VANSEG=${WORKDIR}/%s" % os.path.basename( exePath ),
            "else",
            "   echo 'Unable to locate executable'",
            "   exit",
            "fi",
            "if [ -z ${LD_LIBRARY_PATH} ]; then",
            "   export LD_LIBRARY_PATH=''",
            "fi",
            "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${WORKDIR}",
            "",
         ]
      return ( lineList, outbox )

   def body( self, job = None, imageDir = "", urlRoot = "", imageList = [] ):

      lineList = []
      outbox = []

      for relativePath in imageList:
         imageLocal = os.path.join( imageDir, relativePath )

         if os.path.exists( imageLocal ):

            imageUrl = os.path.join( urlRoot, relativePath )
            imageSubdir = os.path.join\
               ( "images", os.path.dirname( relativePath ) )
            imageRemoteDir = os.path.join( "${WORKDIR}", imageSubdir )
            vsegDir = os.path.join( imageRemoteDir, "vseg" )

            imageFile = os.path.basename( relativePath )
            imageName = os.path.splitext( imageFile )[ 0 ]
            imagePath = os.path.join( imageRemoteDir, imageFile )

            rlmFile = ".".join( [ imageName, "rlm" ] )
            rlmPath = os.path.join( vsegDir, rlmFile )
            regsFile = ".".join( [ imageName, "regs" ] )
            regsPath = os.path.join( vsegDir, regsFile )

            lineList.extend\
               ( [
               "mkdir -p %s" % vsegDir,
               "wget -q -P %s %s" % ( imageRemoteDir, imageUrl ),
               "${VANSEG} %s %s %s" % ( imagePath, rlmPath, regsPath ),
               "rm %s" % imagePath,
               ] )
      return ( lineList, outbox )

   def tail( self, job = None ):
      lineList = \
         [
         "",
         "cd ${WORKDIR}/images",
         "tar -zcf ${WORKDIR}/images.tar.gz *",
         "",
         "echo 'End time: '`date`",
         ]
      outbox = [ "images.tar.gz" ]
      return ( lineList, outbox )
