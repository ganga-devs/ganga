from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig, ConfigError
from Ganga.Utility.logging import getLogger

class PandaRequirements(GangaObject):
   '''Helper class to group Panda requirements.
   '''

   _schema = Schema(Version(1,1), { 
      'long'          : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Send job to a long queue'),
      'cloud'         : SimpleItem(defvalue='US',protected=0,copyable=1,doc='cloud where jobs are submitted (default:US)'),
      'memory'        : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required memory size'),
      'cputime'       : SimpleItem(defvalue=-1,protected=0,copyable=1,doc='Required CPU count in seconds. Mainly to extend time limit for looping detection'),
      'corCheck'      : SimpleItem(defvalue=False,protected=0,copyable=1,doc='Enable a checker to skip corrupted files'),        
      'notSkipMissing': SimpleItem(defvalue=False,protected=0,copyable=1,doc='If input files are not read from SE, they will be skipped by default. This option disables the functionality'),
      'excluded_sites': SimpleItem(defvalue = [],typelist=['str'],sequence=1,protected=0,copyable=1,doc='Panda sites to be excluded while brokering.'),
   })

   _category = 'PandaRequirements'
   _name = 'PandaRequirements'

   def __init__(self):
      super(PandaRequirements,self).__init__()
