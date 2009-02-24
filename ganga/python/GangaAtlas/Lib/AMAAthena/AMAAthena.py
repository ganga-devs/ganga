###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthena.py,v 1.3 2009-02-19 15:38:26 hclee Exp $
###############################################################################
# AMAAthena Job Handler
#
# NIKHEF/ATLAS
# 

import os, re, commands, string

from Ganga.GPIDev.Schema import *
from Ganga.Utility.logging import getLogger
from GangaAtlas.Lib.Athena import * 

amaathena_schema_datadict = Athena._schema.inherit_copy().datadict

class AMAAthena(Athena):
    """The main AMAAthena Application Handler"""

    amaathena_schema_datadict.update( {
        'driver_config': ComponentItem('AMADriverConfig', doc='Configuration of AMADriver'),
        'driver_flags' : SimpleItem(defvalue='', typelist=['str'], doc='AMADriver flags'),
        'max_events'   : SimpleItem(defvalue=-1, typelist=['int'], doc='Maximum number of events')
        } )

    _schema   = Schema(Version(1,0), amaathena_schema_datadict)
    _category = 'applications'
    _name = 'AMAAthena'
    _exportmethods = ['prepare', 'setup', 'postprocess']
    
    _GUIPrefs = [ { 'attribute' : 'atlas_release', 'widget' : 'String' },
                  { 'attribute' : 'atlas_production', 'widget' : 'String' },
                  { 'attribute' : 'atlas_project', 'widget' : 'String' },
                  { 'attribute' : 'atlas_cmtconfig', 'widget' : 'String' },
                  { 'attribute' : 'atlas_environment', 'widget' : 'String_List' },
                  { 'attribute' : 'user_area',     'widget' : 'FileOrString' },
                  { 'attribute' : 'group_area',     'widget' : 'FileOrString' },
                  { 'attribute' : 'max_events',    'widget' : 'String' },
                  { 'attribute' : 'option_file',   'widget' : 'FileOrString_List' },

                  { 'attribute' : 'options',       'widget' : 'String_List' },
                  { 'attribute' : 'user_setupfile', 'widget' : 'FileOrString' },
                  { 'attribute' : 'exclude_from_user_area', 'widget' : 'FileOrString_List' },
                  { 'attribute' : 'exclude_package', 'widget' : 'String_List' },

                  { 'attribute' : 'cscdriver_config', 'widget' : 'FileOrString' },
                  ]

    def __init__(self):
        super(AMAAthena,self).__init__()

    #def read_cmt(self):
    #    """Get some relevant CMT settings"""
    #    return Athena.read_cmt(self)
 
    #def setup(self):
    #    """Run CMT setup script"""
    #    Athena.setup(self)

    #def postprocess(self):
    #    """Determine outputdata and outputsandbox locations of finished jobs
    #    and fill output variable"""
    #    Athena.postprocess(self)

    def prepare(self, athena_compile=True, NG=False, **options):
        """Prepare the job from the user area"""
        Athena.prepare(self, athena_compile=athena_compile, NG=NG, **options)

    #def configure(self,masterappconfig):
    #    return Athena.configure(self,masterappconfig)

    def master_configure(self):

        if not self.driver_config.config_file:
            raise ApplicationConfigurationError(None,'AMADriver config file not set')

        if not self.driver_config.config_file.exists():
            raise ApplicationConfigurationError(None,'File not found: %s' % self.driver_config.config_file.name)

        for f in self.driver_config.include_file:
            if not f.exists():
                raise ApplicationConfigurationError(None,'File not found: %s' % f.name)

        return Athena.master_configure(self)

#config = makeConfig('Athena','Athena configuration parameters')
config = getConfig('Athena')
logger = getLogger()
