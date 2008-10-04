###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Classify.py,v 1.1 2008-10-04 17:42:38 karl Exp $
###############################################################################
# File: Classify.py
# Author: K. Harrison
# Created: 070122

"""Module containing class for Classify application"""

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

class Classify( IApplication ):

   _schema = Schema( Version( 1, 0 ), {
      "version" : SimpleItem( defvalue = "2.0.0", doc = \
         "String giving version number for Grid software installations" ),
      "libList" : SimpleItem( defvalue = [], doc = \
         "List of paths to java libraries used in classification" ),
      "classifierDir" : SimpleItem( defvalue = "", doc = \
         "Directory containing classifier files" ),
      "outDir" : SimpleItem( defvalue = "", doc = \
         "\n".join( [ "Directory for output files", \
         "If null, use top-level directory of input images" ] ) ),
      "imageList" : SimpleItem( defvalue = "", doc = \
         "Path to file containing list of input images" ),
      "maxImage" : SimpleItem( defvalue = 0, doc = \
         "Maximum number of images to process, no maximum for negative value" ),
      } )

   _category = 'applications'
   _name = 'Classify'

   def __init__( self ):
      super( Classify, self ).__init__()
        
   def master_configure( self ):

      listFile = fullpath( self.imageList )
      if not os.path.isfile( listFile ):
         raise ApplicationConfigurationError\
            ( None, "imageList file = '%s' not found" % self.imageList )

      imageFile = open( listFile )
      imageDir = imageFile.readline().strip().rstrip( os.sep )
      inputList = []

      if imageDir: 
         imageDir = fullpath( imageDir )
         inputList = imageFile.readlines()
         imageFile.close()
         outDir = fullpath( self.outDir )
         if not outDir:
            outDir = imageDir
         tagFile = ""
      else:
         outDir = fullpath( self.outDir )
         tagFile = listFile

      if not outDir:
         raise ApplicationConfigurationError\
            ( None, "Output directory not defined" )

      if not os.path.isdir( outDir ):
         os.makedirs( outDir )

      imageList = []
      for input in inputList:
         url = input.split()[ 0 ]
         relativePath = input[ len( url ) : ].strip().strip( os.sep )
         imageList.append( ( url, relativePath ) )

         if len( imageList ) >= self.maxImage:
            if abs( self.maxImage ) == self.maxImage:
               break

      appDict = \
         {
         "version" : self.version,
         "classifierDir" : fullpath( self.classifierDir ),
         "libList" : [],
         "imageDir" : imageDir,
         "outDir" : outDir,
         "imageList" : imageList,
         "tagFile" : tagFile
         }
      for lib in self.libList:
         appDict[ "libList" ].append( fullpath( lib ) )

      return( False, appDict )

   def configure( self, master_appconfig ):    

      return ( False, master_appconfig )

   def postprocess( self ):
      job = self._getParent()
      try:
         outputdata = job.outputdata
         datatype = outputdata._name
      except AttributeError:
         outputdata = None
         datatype = None

      if datatype:
         outputdata.fill()

      return None
