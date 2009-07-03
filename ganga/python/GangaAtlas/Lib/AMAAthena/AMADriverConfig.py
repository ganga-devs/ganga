###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMADriverConfig.py,v 1.1 2008-09-02 12:50:45 hclee Exp $
###############################################################################
# AMADriverConfig
#
# NIKHEF/ATLAS
# 

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
#from Ganga.Utility.Config import getConfig, ConfigError
#from Ganga.Utility.logging import getLogger

class AMADriverConfig(GangaObject):
   '''Attribute class for AMADriver configuration 
   '''

   _schema = Schema(Version(1,0), { 
      'config_file'     : FileItem(doc='The main configuration file of AMADriver'),
      'include_file'    : FileItem(defvalue = [], typelist=['str'], sequence=1, strict_sequence=0, doc="list of files included by the main configuration file" )
   })

   _category = 'AMADriverConfig'
   _name = 'AMADriverConfig'

   _GUIPrefs = [ { 'attribute' : 'config_file',    'widget' : 'FileOrString' },
                 { 'attribute' : 'include_file',   'widget' : 'FileOrString_List' } ]

   def __init__(self):
      super(AMADriverConfig, self).__init__()
