###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# Author: R. Walker
# 

"""Module containing class for handling Cronus requirements"""

from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import ComponentItem, Schema, SimpleItem, Version
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger

import types

logger = getLogger()

class CronusRequirements( GangaObject ):
   '''Helper class to group requirements'''

   _schema = Schema(Version(1,0), { 
      "machine" : SimpleItem( defvalue = "",
         doc = "Requested execution host(s)" ),
      "CEs" : SimpleItem( defvalue = [], 
                                       doc = "List of Compute Elements" ),
      "excludedCEs" : SimpleItem( defvalue = [],
         doc = "Excluded CEs host(s)" ),
      "opsys" : SimpleItem( defvalue = "LINUX", doc = "Operating system" ),
      "arch" : SimpleItem( defvalue = "INTEL", doc = "System architecture" ),
      "memory" : SimpleItem( defvalue = 400, doc = "Mininum physical memory" ),
      "virtual_memory" : SimpleItem( defvalue = 400,
         doc = "Minimum virtual memory" ),
      "other" : SimpleItem( defvalue=[], sequence=1, doc= "Other requirements" )
      } )

   _category = 'cronus_requirements'
   _name = 'CronusRequirements'

   def __init__( self ):
      super( CronusRequirements, self ).__init__()

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

      if len(self.CEs)>0:
         requirementList.append('StringlistMember(FileSystemDomain,"%s")'%
                                          ' '.join(self.CEs) )
      if len(self.excludedCEs)>0:
         requirementList.append(
            'StringlistMember(FileSystemDomain,"%s")=?= FALSE'%
                                          ' '.join(self.excludedCEs) )
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

      requirementString = " && ".join( requirementList )

      return requirementString

# Allow property values to be either string or list
config = getConfig( "CronusRequirements_Properties" )
for property in [ "machine", "excludedCEs" ]:
   config.setDefaultOption( property, \
      CronusRequirements._schema.getItem( property )[ "defvalue" ], \
      check_type = [ types.StringType, types.ListType ] , override = True )
