###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: CondorRequirements.py,v 1.1 2008-07-17 16:40:56 moscicki Exp $
###############################################################################
# File: CondorRequirements.py
# Author: K. Harrison
# Created: 051229
# 
# KH - 060728 : Correction to way multiple requirements are combined
#               in convert method
#
# KH - 060829 : Typo corrected
#
# KH - 061026 : Correction for missing import (types module)
#               Correction to allow configuration values for "machine"
#               and "excluded_machine" to be either string or list
#               Correction to handling of requirement for allowed machines

"""Module containing class for handling Condor requirements"""

__author__  = "K.Harrison <Harrison@hep.phy.cam.ac.uk>"
__date__    = "26 October 2006"
__version__ = "1.3"

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger

import types

logger = getLogger()

class CondorRequirements( GangaObject ):
   '''Helper class to group Condor requirements.

   See also: http://www.cs.wisc.edu/condor/manual
   '''

   _schema = Schema(Version(1,0), { 
      "machine" : SimpleItem( defvalue = "",
         doc = "Requested execution host(s)" ),
      "excluded_machine" : SimpleItem( defvalue = "",
         doc = "Excluded execution host(s)" ),
      "opsys" : SimpleItem( defvalue = "LINUX", doc = "Operating system" ),
      "arch" : SimpleItem( defvalue = "INTEL", doc = "System architecture" ),
      "memory" : SimpleItem( defvalue = 400, doc = "Mininum physical memory" ),
      "virtual_memory" : SimpleItem( defvalue = 400,
         doc = "Minimum virtual memory" ),
      "other" : SimpleItem( defvalue=[], sequence=1, doc= "Other requirements" )
      } )

   _category = 'condor_requirements'
   _name = 'CondorRequirements'

   def __init__( self ):
      super( CondorRequirements, self ).__init__()

   def convert( self):
      '''Convert the condition(s) to a JDL specification'''
      
      requirementList = []
      
      if self.machine:
         if type( self.machine ) == types.StringType:
            machineList = self.machine.split()
         else:
            machineList = self.machine
         machineConditionList = []
         for machine in machineList:
            machineConditionList.append( "Machine == \"%s\"" % str( machine ) )
         machineConditionString = " || ".join( machineConditionList )
         requirement = ( " ".join( [ "(", machineConditionString, ")" ] ) )
         requirementList.append( requirement )

      if self.excluded_machine:
         if type( self.excluded_machine ) == types.StringType:
            machineList = self.excluded_machine.split()
         else:
            machineList = self.excluded_machine
         for machine in machineList:
            requirementList.append( "Machine != \"%s\"" % str( machine ) )

      if self.opsys:
         requirementList.append( "OpSys == \"%s\"" % str( self.opsys ) )

      if self.arch:
         requirementList.append( "Arch == \"%s\"" % str( self.arch ) )

      if self.memory:
         requirementList.append( "Memory >= %s" % str( self.memory ) )

      if self.virtual_memory:
         requirementList.append\
            ( "VirtualMemory >= %s" % str( self.virtual_memory ) )

      if self.other:
         requirementList.extend( self.other )

      requirementString = "requirements = " + " && ".join( requirementList )

      return requirementString

# Allow property values to be either string or list
config = getConfig( "defaults_CondorRequirements" )
for property in [ "machine", "excluded_machine" ]:
   config.options[property].type =  [ types.StringType, types.ListType ]

