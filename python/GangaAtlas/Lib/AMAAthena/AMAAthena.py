###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: AMAAthena.py,v 1.5 2009-05-11 16:33:56 hclee Exp $
###############################################################################
# AMAAthena Job Handler
#
# NIKHEF/ATLAS
#
import os
import re
import os.path
import tempfile

from Ganga.GPIDev.Schema import *
from Ganga.Utility.logging import getLogger
from GangaAtlas.Lib.Athena import *
from GangaAtlas.Lib.AMAAthena.AMAAthenaCommon import *

amaathena_schema_datadict = Athena._schema.inherit_copy().datadict

class AMAAthena(Athena):
    """The main AMAAthena Application Handler"""

    amaathena_schema_datadict.update( {
        'sample_name'       : SimpleItem(defvalue='MySample', typelist=['str'], doc='The input sample name'),
        'job_option_flags'  : SimpleItem(defvalue=['MC08','TRIG'], typelist=['str'], sequence=1, doc='AMAAthena job option flags, e.g. MC08, TRIG, DATA, etc.'),
        'driver_config'     : ComponentItem('AMADriverConfig', doc='Configuration of AMADriver'),
        'log_level'         : SimpleItem(defvalue='INFO', typelist=['str'], doc='Athena logging level'),
        'driver_flags'      : SimpleItem(defvalue='', typelist=['str'], doc='AMADriver flags'),
        'max_events'        : SimpleItem(defvalue=-1, typelist=['int'], doc='Maximum number of events'),

        ## hide unused attributes from Athena module
        'atlas_exetype'     : SimpleItem(defvalue='ATHENA',doc='Athena Executable type, e.g. ATHENA, PYARA, ROOT, TRF ', hidden=1),
        'atlas_use_AIDA'    : SimpleItem(defvalue=False, doc='use AIDA', hidden=1),
        'trf_parameter'     : SimpleItem(defvalue={},typelist=["dict","str"], doc='Parameters for transformations', hidden=1)
        } )

    _schema   = Schema(Version(2,0), amaathena_schema_datadict)
    _category = 'applications'
    _name = 'AMAAthena'
    _exportmethods = ['prepare', 'prepare_old', 'setup', 'postprocess']
    
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

    def prepare(self, athena_compile=True, NG=False, **options):
        """Prepare the job from the user area"""

        job = self._getParent()

        tmpdir = tempfile.mkdtemp()

        ## 1. convernts configuration files into job option
        ama_config_joption = ama_make_config_joption( job, joption_fpath=os.path.join(tmpdir, 'AMAConfig_jobOptions.py') )

        logger.debug('fetching main AMA job option file from release/UserArea ...')

        ## 2. retrieves AMAAthena_joboptions_new.py using get_joboptions command
        opt_fnames = ['AMAAthena_jobOptions_new.py']

        ## 3. makes the full list of job option files
        ama_optfiles = [ ama_config_joption ] + get_option_files(opt_fnames, wdir=tmpdir)

        ## 4. puts the job options as user job option files in Ganga job
        ##    NOTE: the option_file should be overwritten, the user-specific job option can be
        ##          added to the list after the prepare() method
        self.option_file = []
        for f in ama_optfiles:
            self.option_file += [ File(f) ]

        ## 5. call Athena.prepare() method
        try:
            Athena.prepare(self, athena_compile=athena_compile, NG=NG, **options)

            ## set the outHist to False to workaround the Panda jobs
            ## Not sure if we need this because in AMAAthenaPandaHandler,
            ## the atlas_run_config is not really taken into account for creating Panda job
            self.atlas_run_config['output']['outHist'] = False

        except Exception, e:
            # if prepare failed, re-set the job option file field
            self.option_file = []
            raise ApplicationConfigurationError('Failed to prepare Athena: %s' % e)

        return

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
config  = getConfig('Athena')
logger = getLogger()
