from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class NGRequirements(GangaObject):
     '''Helper class to group requirements'''

     _schema = Schema(Version(1,3), {
          "runtimeenvironment": SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc='Runtimeenvironment'),
          "cputime" : SimpleItem( defvalue = 30, typelist=['int'], doc = "Requested cpu time" ),
          "walltime" : SimpleItem( defvalue = 30, typelist=['int'],  doc = "Requested wall time" ),
          "memory" : SimpleItem( defvalue = 1000, typelist=['int'], doc = "Mininum virtual  memory" ),
          "disk" : SimpleItem( defvalue = 1000, typelist=['int'], doc = "Minimum memory" ),
          "timeout" : SimpleItem( defvalue = 5, typelist=['int'], doc = "Submission timeout" ),
          "gsidcap" : SimpleItem( defvalue='srm.swegrid.se', typelist=['str'], sequence=0, doc= "Required GSIDCAP storage element" ),
          "other" : SimpleItem( defvalue=[], typelist=['str'], sequence=1, doc= "Other requirements" ),
          'move_links_locally' : SimpleItem( defvalue=0,typelist=['int'],sequence=0, doc= "Set to 1 to move links to local disk before running (NOT RECOMMENDED!)")
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
