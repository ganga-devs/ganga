from GangaCore.GPIDev.Adapters.IApplication import IApplication
from GangaCore.GPIDev.Schema import *

import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()

class ProdTrans(IApplication):
    """Ganga application for production queues."""

    _schema = Schema(Version(1, 0), {
            'atlas_release': SimpleItem(defvalue='',
                                        doc='ATLAS Software Release'),
            'atlas_cmtconfig': SimpleItem(defvalue='',
                                          doc='CMTCONFIG environment variable'),
            'output_files': SimpleItem(defvalue=[],
                                       typelist=['str'],
                                       sequence=1,
                                       doc='Logical File Names for output'),
            'home_package': SimpleItem(defvalue='',
                                       doc='Home package'),
            'transformation': SimpleItem(defvalue='',
                                         doc='Transformation'),
            'job_parameters': SimpleItem(defvalue='',
                                         doc='Addtional parameters for the job'),
            'prod_source_label': SimpleItem(defvalue='',
                                            doc='Production Source Label'),
            'dbrelease': SimpleItem(defvalue='LATEST',
                                    doc='ATLAS DB Release. Use LATEST for most recent'),
            'dbrelease_dataset': SimpleItem(defvalue='',
                                            doc='Dataset name for the DB'),
            'priority': SimpleItem(defvalue=1000,
                                   doc='Initialial priority for the Job'),
            'randomize_lfns': SimpleItem(defvalue=False,
                                         doc='Generate output file names with random suffix'),
            'max_events': SimpleItem(defvalue=0,
                                     docs='Max events'),
            'output_type': SimpleItem(defvalue=['NTUP_TOP'], typelist=['str'],sequence=1, strict_sequence=0,
                                      doc='Type of the output file'),
            'input_type': SimpleItem(defvalue='',
                                     doc='Type of the input file'),
            'is_prepared': SharedItem(defvalue=None, strict_sequence=0, visitable=1, 
                copyable=1, typelist=['type(None)', 'bool', 'str'], protected=0,
                doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
            'core_count': SimpleItem(defvalue=0,
                                     docs='Number of cores to run.'),
            })
    _category = 'applications'
    _name = 'ProdTrans'
    _exportmethods = ['postprocess', 'unprepare']

    def __init__(self):
        super(ProdTrans, self).__init__()
        logger.debug('Initializing ProdTrans object')

    def master_configure(self):
        """Configure the general aspect of the application."""
        logger.debug('ProdTrans master_configure called')
        return (0, None)

    def configure(self, master_appconfig):
        """Configure the specific aspect of the application."""
        logger.debug('ProdTrans configure called')
        return (None, None)

    def postprocess(self):
        """Determine the outputdata and outputsandbox locations."""
        pass
