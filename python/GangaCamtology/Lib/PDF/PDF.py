###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PDF.py,v 1.1 2009-05-10 16:42:03 karl Exp $
###############################################################################
# File: PDF.py
# Author: K. Harrison
# Created: 070122

"""Module containing class for PDF application"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "9 May 2009"
__version__ = "1.0"

import os
import shutil

from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility import logging
from Ganga.Utility.files import fullpath

logger = logging.getLogger()

class PDF( IApplication ):

   _schema = Schema( Version( 1, 0 ), {
      "version" : SimpleItem( defvalue = "1.0", doc = \
         "String giving version number" ),
      "softwareDir" : SimpleItem( defvalue = "", doc = \
         "\n".join( [ "Directory containing application software", \
         "Used only if VO_CAMONT_SW_DIR is undefined" ] ) ),
      "outDir" : SimpleItem( defvalue = "", doc = \
         "\n".join( [ "Directory for output files", \
         "If null, use top-level directory of input documents" ] ) ),
      "docList" : SimpleItem( defvalue = "", doc = \
         "Path to file containing list of input documents" ),
      "maxDoc" : SimpleItem( defvalue = 0, doc = \
         "Maximum number of documents to process, "\
         + "no maximum for negative value" ),
      } )

   _category = 'applications'
   _name = 'PDF'

   def __init__( self ):
      super( PDF, self ).__init__()
        
   def master_configure( self ):

      listFile = fullpath( self.docList )
      if not os.path.isfile( listFile ):
         raise ApplicationConfigurationError\
            ( None, "docList file = '%s' not found" % self.docList )

      docFile = open( listFile )
      docDir = docFile.readline().strip().rstrip( os.sep )
      inputList = []

      if docDir: 
         docDir = fullpath( docDir )
         inputList = docFile.readlines()
         docFile.close()
         outDir = fullpath( self.outDir )
         if not outDir:
            outDir = docDir
         tagFile = ""
      else:
         outDir = fullpath( self.outDir )
         tagFile = listFile

      if not outDir:
         raise ApplicationConfigurationError\
            ( None, "Output directory not defined" )

      if not os.path.isdir( outDir ):
         os.makedirs( outDir )

      docList = []
      for input in inputList:
         url = input.split()[ 0 ]
         relativePath = input[ len( url ) : ].strip().strip( os.sep )
         docList.append( ( url, relativePath ) )

         if len( docList ) >= self.maxDoc:
            if abs( self.maxDoc ) == self.maxDoc:
               break

      if self.softwareDir:
        softwareDir = fullpath( self.softwareDir )
      else:
        softwareDir = ""

      appDict = \
         {
         "version" : self.version,
         "docDir" : docDir,
         "outDir" : outDir,
         "docList" : docList,
         "tagFile" : tagFile,
         "softwareDir" : softwareDir
         }

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
