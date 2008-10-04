###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: CamontDataset.py,v 1.1 2008-10-04 17:42:38 karl Exp $
###############################################################################
# File: CamontData.py
# Author: K. Harrison
# Created: 070215

"""Module containing class for Camont output data"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "30 November 2007"
__version__ = "1.1"

import commands
import os
import shutil
import time

from Ganga.GPIDev.Lib.Dataset import Dataset
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility import logging
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import fullpath
from Ganga.Utility.GridShell import getShell

logger = logging.getLogger()
shell = getShell()

class CamontDataset( Dataset ):

   _schema = Schema( Version( 1, 0 ), {} )
   _category = 'datasets'
   _name = 'CamontDataset'

   _exportmethods = [ "cleanup", "download", "fill", "unpack" ]

   def __init__( self ):
      super( CamontDataset, self ).__init__()

   def cleanup( self, job = None ):

      if not job:
         job = self._getParent()

      outfileList = os.listdir( job.outputdir )
      for filename in outfileList:
         if filename not in [ "execute.dat", "stderr", "stdout", "submit.dat", "time.dat" ]:
            filepath = os.path.join( job.outputdir, filename )
            if os.path.isdir( filepath ):
               shutil.rmtree( filepath )
            else:
               os.remove( filepath )

      shutil.rmtree( job.inputdir )
      os.mkdir( job.inputdir )
      job.updateStatus( "completed" )

      return True
        
   def download( self, job = None, tarFile = None ):

      if not job:
         job = self._getParent()

      if not tarFile:
         tarFile = fullpath( os.path.join( job.outputdir, "images.tar.gz" ) )

      if ( "LCG" == job.backend._name ): 
         gridUrl = \
            os.path.join( job.outputdata.getGridStorage(), "images.tar.gz" )
         cp = "globus-url-copy %s file:%s" % ( gridUrl, tarFile )
         status = shell.cmd1( cmd = cp, allowed_exit = range( 1000 ) )[ 0 ]
         if ( 0 != status ):
            logger.warning( "Problems retrieving output for job %s" % job.id )
            logger.warning( "Setting job status to failed" )
            job.updateStatus( "failed" )
            return False
         rm = "edg-gridftp-rm %s" % gridUrl
         shell.cmd1( cmd = rm, allowed_exit = range( 1000 ) )
         rmdir = "edg-gridftp-rmdir %s" % os.path.dirname( gridUrl )
         shell.cmd1( cmd = rmdir, allowed_exit = range( 1000 ) )

      return True

   def fill( self ):

      hostname = commands.getoutput( "hostname -f" )
      job = self._getParent()

      tarFile = fullpath( os.path.join( job.outputdir, "images.tar.gz" ) )

      downloadStartTime = "%.6f" % time.time()
      statusOK = self.download( job = job, tarFile = tarFile )
      downloadEndTime = "%.6f" % time.time()

      unpackStartTime = "%.6f" % time.time()
      if statusOK:
         statusOK = self.unpack( job = job, tarFile = tarFile )
      unpackEndTime = "%.6f" % time.time()

      cleanupStartTime = "%.6f" % time.time()
      if statusOK:
         statusOK = self.cleanup( job = job )
      cleanupEndTime = "%.6f" % time.time()

      lineList = \
         [
         "Hostname: %s" % hostname,
         "Download_start: %s" % downloadStartTime,
         "Download_end: %s" % downloadEndTime,
         "Unpack_start: %s" % unpackStartTime,
         "Unpack_end: %s" % unpackEndTime,
         "Cleanup_start: %s" % cleanupStartTime,
         "Cleanup_end: %s" % cleanupEndTime,
         ]

      outString = "\n".join( lineList )
      outfile = open( os.path.join( job.outputdir, "retrieve.dat" ), "w" )
      outfile.write( outString )
      outfile.close()

      return None

   def getGridStorage( self, gridhome = "" ):
      vo = getConfig( "LCG" )[ "VirtualOrganisation" ]
      if not gridhome:
#        gridhome = os.path.join( \
#           "gsiftp://serv02.hep.phy.cam.ac.uk/dpm/hep.phy.cam.ac.uk/home", vo )
         gridhome = os.path.join( \
            "gsiftp://t2se01.physics.ox.ac.uk/dpm/physics.ox.ac.uk/home", vo )
      job = self._getParent()
      id = job.id
      username = getConfig( "DefaultJobRepository" )[ "user" ]
      userletter = username[ 0 ]
      gridStorage = os.path.join\
         ( gridhome, "user", userletter, username, "ganga", str( id ) )
      return gridStorage

   def unpack( self, job = None, tarFile = None ):

      if not job:
         job = self._getParent()

      if not tarFile:
         tarFile = fullpath( os.path.join( job.outputdir, "images.tar.gz" ) )

      if not os.path.exists( tarFile ):
         logger.warning( "Output not found for job %s" % job.id )
         logger.warning( "Setting job status to failed" )
         job.updateStatus( "failed" )
         return False

      outDir = job.application.outDir
      if not outDir:
         listFile = fullpath( job.application.imageList )
         if not os.path.isfile( listFile ):
            logger.warning( "imageList file = '%s' not found for job %s" % \
                  ( job.application.imageList, job.id ) )
            logger.warning( "Setting job status to failed" )
            job.updateStatus( "failed" )
            return False

         imageFile = open( listFile )
         job.application.outDir = imageFile.readline().strip().rstrip( os.sep )
         imageFile.close()

      outDir = fullpath( job.application.outDir )
      if not os.path.isdir( outDir ):
         os.makedirs( outDir )

      tmpStore = os.path.join( job.outputdir, "tmp" ) 
      if not os.path.isdir( tmpStore ):
         os.makedirs( tmpStore )

      untarCommand = "tar -C %s -zxf %s" % ( tmpStore, tarFile )
      status = os.system( untarCommand )
      if ( 0 != status ):
         logger.warning( "Problems untarring output for job %s" % job.id )
         logger.warning( "Setting job status to failed" )
         job.updateStatus( "failed" )
         return False

      copyCommand = "cp -r %s/* %s" % ( tmpStore, outDir )
      status = os.system( copyCommand )
      if ( 0 != status ):
         logger.warning( "Problems copying output for job %s" % job.id )
         logger.warning( "Setting job status to failed" )
         job.updateStatus( "failed" )
         return False

      return True
