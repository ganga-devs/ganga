from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class NGRequirements(GangaObject):
     '''Helper class to group requirements'''

     _schema = Schema(Version(1,0), {
          "runtimeenvironment": SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc='Runtimeenvironment'),
          "cputime" : SimpleItem( defvalue = "30", doc = "Requested cpu time" ),
          "walltime" : SimpleItem( defvalue = "30", doc = "Requested wall time" ),
          "memory" : SimpleItem( defvalue = 500, doc = "Mininum virtual  memory" ),
          "disk" : SimpleItem( defvalue = 500, doc = "Minimum memory" ),
          "other" : SimpleItem( defvalue=[], sequence=1, doc= "Other requirements" )
          } )

     _category = 'ng_requirements'
     _name = 'NGRequirements'
     
     def __init__(self):
          
          super(NGRequirements,self).__init__()

     def convert( self):
          '''Convert the condition(s) to a xrsl specification'''
          requirementList = []

          if self.cputime:
               requirementList.append( "(cputime = %smin" % str( self.cputime ) + ")")
         
          if self.walltime:
               requirementList.append( "(walltime = %smin" % str( self.walltime ) + ")")   

          if self.memory:
               requirementList.append( "(memory = %s" % str( self.memory ) + ")")

          if self.disk:
               requirementList.append\
               ( "(disk = %s" % str( self.disk ) + ")" )

          if self.other:
               requirementList.extend( self.other )

         
          if self.runtimeenvironment:
               for re in self.runtimeenvironment:
                    requirementList.append( '(runtimeenvironment = %s' % str( re ) + ')')

          requirementString = "\n".join( requirementList )

          logger.debug('NG requirement string: %s' % requirementString)

          return requirementString
