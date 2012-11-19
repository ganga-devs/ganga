###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Vanseg.py,v 1.1 2008-10-04 17:42:39 karl Exp $
###############################################################################
# File: Vanseg.py
# Author: K. Harrison
# Created: 070122

"""Module containing class for Vanseg application"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "30 November 2007"
__version__ = "1.2"

import os
import shutil

from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility import logging
from Ganga.Utility.files import fullpath

logger = logging.getLogger()

class Vanseg( IApplication ):

   _schema = Schema( Version( 1, 0 ), {
      "exe" : SimpleItem( defvalue = "", doc = \
         "Path to vanseg executable" ),
      "outDir" : SimpleItem( defvalue = "", doc = \
         "\n".join( [ "Directory for output files", \
         "If null, use top-level directory of input images" ] ) ),
      "imageList" : SimpleItem( defvalue = "", doc = \
         "Path to file containing list of input images" ),
      "maxImage" : SimpleItem( defvalue = 0, doc = \
         "Maximum number of images to process, no maximum for negative value" ),
      } )

   _category = 'applications'
   _name = 'Vanseg'

   def __init__( self ):
      super( Vanseg, self ).__init__()
        
   def master_configure( self ):

      exePath = fullpath( self.exe )

      if not os.path.isfile( exePath ):
         raise ApplicationConfigurationError\
            ( None, "exe = '%s' not found" % self.exe )

      listFile = fullpath( self.imageList )
      if not os.path.isfile( listFile ):
         raise ApplicationConfigurationError\
            ( None, "imageList file = '%s' not found" % self.imageList )

      imageFile = open( listFile )
      imageDir = fullpath( imageFile.readline().strip().rstrip( os.sep ) )
      inputList = imageFile.readlines()
      imageFile.close()

      if not imageDir:
         raise ApplicationConfigurationError\
            ( None, "Top-level image directory '%s' not found" % imageDir )
      
      outDir = fullpath( self.outDir )
      if not outDir:
         outDir = imageDir

      if not os.path.isdir( outDir ):
         os.makedirs( outDir )

      imageList = []
      for filename in inputList:
         relativePath = filename.strip().strip( os.sep )
         imagePath = fullpath( os.path.join( imageDir, relativePath ) )
         if os.path.exists( imagePath ):
            imageList.append( relativePath )
            resultDir = os.path.join( os.path.dirname( imagePath ), "vseg" )
            if os.path.isdir( resultDir ):
               shutil.rmtree( resultDir )
         else:
            logger.warning( "Specified image file '%s' not found" % imagePath )
            logger.warning( "File will be skipped" )

         if len( imageList ) >= self.maxImage:
            if abs( self.maxImage ) == self.maxImage:
               break

      appDict = \
         {
         "exePath" : exePath,
         "imageDir" : imageDir,
         "outDir" : outDir,
         "imageList" : imageList
         }

      return( False, appDict )

   def configure( self, master_appconfig ):    

      return ( False, master_appconfig )
